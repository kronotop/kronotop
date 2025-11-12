/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.index.maintenance;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.IndexSubspaceMagic;

import java.util.List;

/**
 * Utility for determining versionstamp scan boundaries for index building operations.
 *
 * <p>Locates lower and upper versionstamp boundaries from the primary index and secondary
 * index back pointers to define the range of documents that need to be indexed during
 * background index building.
 *
 * <p><strong>Boundary Semantics:</strong>
 * <ul>
 *   <li>Lower boundary: First versionstamp in primary index ENTRIES (inclusive)</li>
 *   <li>Upper boundary: One beyond last versionstamp (exclusive)</li>
 *   <li>Both null: Empty bucket, no documents to index</li>
 * </ul>
 *
 * <p><strong>Upper Boundary Sources:</strong>
 * <ol>
 *   <li>Secondary index BACK_POINTER: If index created during document insertion</li>
 *   <li>Primary index last entry + 1: If no back pointer exists (clean bucket)</li>
 * </ol>
 *
 * <p><strong>Key Structure:</strong>
 * <pre>
 * Primary Index:   ENTRIES/{versionstamp} → metadata
 * Secondary Index: BACK_POINTER/{versionstamp} → marker
 * </pre>
 *
 * <p>Called by {@link IndexBoundaryRoutine} after metadata convergence to determine
 * the document range for {@link IndexBuildingTask} creation.
 *
 * @see IndexBoundaryRoutine
 * @see Boundaries
 * @see IndexSubspaceMagic
 */
public class BoundaryLocator {

    /**
     * Finds the first versionstamp in an index range after a given prefix.
     *
     * <p>Performs a forward range scan to retrieve the first entry beyond the prefix,
     * extracts the versionstamp from position 1 in the unpacked tuple key.
     *
     * <p>Range: (prefix, strinc(prefix)] limited to 1 entry
     *
     * @param tr transaction for range scan
     * @param index index containing the subspace
     * @param prefix byte array prefix defining the range start
     * @return the first versionstamp after prefix, or null if range is empty
     */
    private static Versionstamp locateBoundary(Transaction tr, Index index, byte[] prefix) {
        KeySelector begin = KeySelector.firstGreaterThan(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
        List<KeyValue> entries = tr.getRange(begin, end, 1).asList().join();
        if (entries.isEmpty()) {
            return null;
        }
        KeyValue entry = entries.getFirst();
        Tuple parsedKey = index.subspace().unpack(entry.getKey());
        return (Versionstamp) parsedKey.get(1);
    }

    /**
     * Locates the upper boundary by finding the last entry in the primary index and adding 1.
     *
     * <p>Performs reverse range scan on primary index ENTRIES to find the highest
     * versionstamp, then creates a new versionstamp with userVersion incremented by 1.
     * This ensures the upper boundary is exclusive and includes all existing documents.
     *
     * <p>Used as fallback when secondary index has no BACK_POINTER entries, indicating
     * the index was created on a bucket with existing documents.
     *
     * @param tr transaction for range scan
     * @param primaryIndex primary index to scan
     * @return versionstamp one position beyond the last entry (exclusive upper bound)
     * @throws IndexMaintenanceRoutineException if the primary index is empty
     */
    private static Versionstamp locateUpperBoundaryFromPrimaryIndex(Transaction tr, Index primaryIndex) {
        byte[] primaryIndexPrefix = primaryIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterThan(primaryIndexPrefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(primaryIndexPrefix));
        List<KeyValue> entries = tr.getRange(begin, end, 1, true).asList().join();
        if (entries.isEmpty()) {
            throw new IndexMaintenanceRoutineException("no entries found to determine the highest versionstamp");
        }
        KeyValue entry = entries.getFirst();
        Tuple parsedKey = primaryIndex.subspace().unpack(entry.getKey());
        Versionstamp found = (Versionstamp) parsedKey.get(1);
        return Versionstamp.complete(found.getTransactionVersion(), found.getUserVersion() + 1);
    }

    /**
     * Determines lower and upper versionstamp boundaries for index building.
     *
     * <p>Locates scan boundaries by examining primary and secondary indexes to define
     * the inclusive-exclusive range [lower, upper) of documents to process.
     *
     * <p><strong>Boundary Detection Logic:</strong>
     * <ol>
     *   <li>Lower: First versionstamp in primary index ENTRIES</li>
     *   <li>If lower is null → empty bucket → return (null, null)</li>
     *   <li>Upper: First versionstamp in secondary index BACK_POINTER</li>
     *   <li>If upper is null → use primary index last entry + 1</li>
     * </ol>
     *
     * <p><strong>Return Values:</strong>
     * <ul>
     *   <li>(null, null): Empty bucket, no documents exist</li>
     *   <li>(lower, upper): Non-empty bucket, defines scan range [lower, upper)</li>
     * </ul>
     *
     * <p><strong>Back Pointer Purpose:</strong> Secondary index BACK_POINTER entries are
     * created during document insertion when the index already exists. They mark the highest
     * versionstamp that needs indexing. If no back pointers exist, the index was created
     * on an empty bucket or all documents need reindexing.
     *
     * @param tr transaction for reading index entries
     * @param metadata bucket metadata containing index definitions
     * @param indexId secondary index identifier
     * @return boundaries record with lower (inclusive) and upper (exclusive) versionstamps
     * @throws IndexMaintenanceRoutineException if the primary index is empty but lower boundary found
     * @see IndexBoundaryRoutine#startInternal()
     */
    public static Boundaries locate(Transaction tr, BucketMetadata metadata, long indexId) {
        Index primaryIndex = metadata.indexes().getIndex(DefaultIndexDefinition.ID.selector(), IndexSelectionPolicy.ALL);
        byte[] primaryIndexPrefix = primaryIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        Versionstamp lower = locateBoundary(tr, primaryIndex, primaryIndexPrefix);
        if (lower == null) {
            return new Boundaries(null, null);
        }

        Index secondaryIndex = metadata.indexes().getIndexById(indexId, IndexSelectionPolicy.ALL);
        byte[] secondaryIndexPrefix = secondaryIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
        Versionstamp upper = locateBoundary(tr, secondaryIndex, secondaryIndexPrefix);
        if (upper == null) {
            upper = locateUpperBoundaryFromPrimaryIndex(tr, primaryIndex);
        }

        return new Boundaries(lower, upper);
    }
}
