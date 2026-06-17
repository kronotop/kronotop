/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.PrimaryIndex;

import java.util.List;

/**
 * Locates versionstamp scan boundaries for index building by examining VOLUME_POINTER
 * entries in the primary index.
 *
 * <p>Returns an inclusive lower and exclusive upper boundary defining the document range
 * to be indexed. If the bucket is empty, both boundaries are null.
 *
 * @see IndexBoundaryRoutine
 * @see Boundaries
 */
public class BoundaryLocator {

    /**
     * Finds the first versionstamp after the given prefix via forward range scan.
     *
     * @param tr            transaction for range scan
     * @param indexSubspace subspace used to unpack the key
     * @param prefix        byte array prefix defining the range start
     * @return the first versionstamp after prefix, or null if the range is empty
     */
    private static Versionstamp locateBoundary(ReadTransaction tr, DirectorySubspace indexSubspace, byte[] prefix) {
        KeySelector begin = KeySelector.firstGreaterThan(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
        List<KeyValue> entries = tr.getRange(begin, end, 1).asList().join();
        if (entries.isEmpty()) {
            return null;
        }
        KeyValue entry = entries.getFirst();
        Tuple parsedKey = indexSubspace.unpack(entry.getKey());
        return (Versionstamp) parsedKey.get(1);
    }

    /**
     * Finds the last versionstamp in primary index VOLUME_POINTER and returns userVersion + 1.
     * Used as fallback when the target index has no VOLUME_POINTER entries.
     *
     * @param tr                   transaction for range scan
     * @param primaryIndexSubspace primary index subspace to scan
     * @return versionstamp one beyond the last entry (exclusive upper bound)
     * @throws IndexMaintenanceRoutineException if the primary index is empty
     */
    private static Versionstamp locateUpperBoundaryFromPrimaryIndex(ReadTransaction tr, DirectorySubspace primaryIndexSubspace) {
        byte[] primaryIndexPrefix = primaryIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.VOLUME_POINTER.getValue()));
        KeySelector begin = KeySelector.firstGreaterThan(primaryIndexPrefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(primaryIndexPrefix));
        List<KeyValue> entries = tr.getRange(begin, end, 1, true).asList().join();
        if (entries.isEmpty()) {
            throw new IndexMaintenanceRoutineException("no entries found to determine the highest versionstamp");
        }
        KeyValue entry = entries.getFirst();
        Tuple parsedKey = primaryIndexSubspace.unpack(entry.getKey());
        Versionstamp found = (Versionstamp) parsedKey.get(1);
        return Versionstamp.complete(found.getTransactionVersion(), found.getUserVersion() + 1);
    }

    /**
     * Determines the versionstamp scan range [lower, upper) for index building.
     *
     * <p>The lower boundary is the first primary index VOLUME_POINTER entry (inclusive). The upper
     * boundary is the last primary index entry + 1 (exclusive), so the build covers every document that
     * existed at boundary capture. Returns (null, null) if the bucket is empty.
     *
     * @param tr       transaction for reading index entries
     * @param metadata bucket metadata containing index definitions
     * @return boundaries with lower (inclusive) and upper (exclusive) versionstamps
     * @throws IndexMaintenanceRoutineException if the primary index is unexpectedly empty
     */
    public static Boundaries locate(ReadTransaction tr, BucketMetadata metadata) {
        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.ALL);
        DirectorySubspace primarySubspace = primaryIndex.subspace();
        byte[] primaryIndexPrefix = primarySubspace.pack(Tuple.from(IndexSubspaceMagic.VOLUME_POINTER.getValue()));
        Versionstamp lower = locateBoundary(tr, primarySubspace, primaryIndexPrefix);
        if (lower == null) {
            return new Boundaries(null, null);
        }

        Versionstamp upper = locateUpperBoundaryFromPrimaryIndex(tr, primarySubspace);
        return new Boundaries(lower, upper);
    }
}
