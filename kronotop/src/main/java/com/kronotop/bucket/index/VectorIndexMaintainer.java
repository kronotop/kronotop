/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.bucket.index;

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketMetadata;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Manages vector index entries and mutation logs for crash recovery in FoundationDB.
 */
public final class VectorIndexMaintainer extends IndexMaintainer {

    /**
     * Creates a vector index entry. Also increments the index cardinality.
     */
    public static void setEntry(
            Transaction tr,
            VectorIndex vectorIndex,
            BucketMetadata metadata,
            byte[] objectId,
            byte[] indexEntry,
            float[] vector
    ) {
        DirectorySubspace indexSubspace = vectorIndex.subspace();

        Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), objectId);
        byte[] key = indexSubspace.pack(tuple);
        tr.set(key, VectorIndexValue.encode(indexEntry, vector));

        IndexUtil.mutateCardinality(tr, metadata.subspace(), vectorIndex.definition().id(), 1);
    }

    /**
     * Creates a vector index entry, used during index backfill and the in-place update path.
     * Also increments the index cardinality.
     */
    public static void insertEntry(
            Transaction tr,
            VectorIndex vectorIndex,
            BucketMetadata metadata,
            byte[] objectId,
            int shardId,
            byte[] entryMetadata,
            float[] vector
    ) {
        DirectorySubspace indexSubspace = vectorIndex.subspace();

        byte[] encodedIndexEntry = new IndexEntry(shardId, entryMetadata).encode();
        Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), objectId);
        byte[] key = indexSubspace.pack(tuple);
        tr.set(key, VectorIndexValue.encode(encodedIndexEntry, vector));

        IndexUtil.mutateCardinality(tr, metadata.subspace(), vectorIndex.definition().id(), 1);
    }

    /**
     * Removes a vector index entry and decrements the index cardinality.
     */
    public static void dropEntry(Transaction tr, byte[] objectId, VectorIndex vectorIndex, BucketMetadata metadata) {
        DirectorySubspace indexSubspace = vectorIndex.subspace();

        Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), objectId);
        byte[] key = indexSubspace.pack(tuple);
        tr.clear(key);

        IndexUtil.mutateCardinality(tr, metadata.subspace(), vectorIndex.definition().id(), -1);
    }

    /**
     * Updates the storage metadata of an existing vector index entry while preserving its vector data.
     * No-op if the entry does not exist.
     */
    public static void updateIndexEntry(
            Transaction tr,
            byte[] objectId,
            VectorIndex vectorIndex,
            int shardId,
            byte[] entryMetadata
    ) {
        DirectorySubspace indexSubspace = vectorIndex.subspace();
        Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), objectId);
        byte[] key = indexSubspace.pack(tuple);
        byte[] existing = tr.get(key).join();
        if (existing == null) {
            return;
        }
        VectorIndexValue decoded = VectorIndexValue.decode(existing);
        byte[] encodedIndexEntry = new IndexEntry(shardId, entryMetadata).encode();
        byte[] value = VectorIndexValue.encode(encodedIndexEntry, decoded.vector());
        tr.set(key, value);
    }

    /**
     * Appends an insert or update mutation to the mutation log for crash recovery.
     * The log key is versionstamped to preserve mutation ordering.
     */
    public static void setMutationLog(
            Transaction tr,
            DirectorySubspace indexSubspace,
            MutationLogMarker marker,
            byte[] objectId,
            byte[] encodedIndexEntry,
            float[] vector,
            int userVersion
    ) {
        Tuple tuple = Tuple.from(
                IndexSubspaceMagic.MUTATION_LOG.getValue(),
                Versionstamp.incomplete(userVersion)
        );
        byte[] key = indexSubspace.packWithVersionstamp(tuple);
        byte[] value = MutationLogValue.encode(marker, objectId, encodedIndexEntry, vector);
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, key, value);
    }

    /**
     * Appends a delete mutation to the mutation log for crash recovery.
     * The log key is versionstamped to preserve mutation ordering.
     */
    public static void deleteMutationLog(
            Transaction tr,
            DirectorySubspace indexSubspace,
            byte[] objectId,
            int userVersion
    ) {
        Tuple tuple = Tuple.from(
                IndexSubspaceMagic.MUTATION_LOG.getValue(),
                Versionstamp.incomplete(userVersion)
        );
        byte[] key = indexSubspace.packWithVersionstamp(tuple);
        byte[] value = MutationLogValue.encode(MutationLogMarker.DELETE, objectId);
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, key, value);
    }

    /**
     * Converts a BSON array of doubles to a float array, rejecting any component that overflows float range.
     *
     * @return the parsed vector, or {@code null} if the value is null
     */
    public static float[] parseVector(BsonValue value) {
        if (value == null || value.isNull()) {
            return null;
        }
        BsonArray array = value.asArray();
        float[] vector = new float[array.size()];
        for (int i = 0; i < array.size(); i++) {
            double d = array.get(i).asDouble().getValue();
            float f = (float) d;
            if (Float.isInfinite(f) || Float.isNaN(f)) {
                throw new KronotopException("Vector component at index " + i + " overflows float range: " + d);
            }
            vector[i] = f;
        }
        return vector;
    }

    /**
     * Retrieves and parses the vector from a document using the index's configured field selector.
     *
     * @return the parsed vector, or {@code null} if the field is missing or null
     */
    public static float[] extractVector(VectorIndexDefinition definition, BsonDocument document) {
        BsonValue value = SelectorMatcher.match(definition.selector(), document);
        return parseVector(value);
    }
}
