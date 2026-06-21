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
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Manages vector index entries and mutation logs for crash recovery in FoundationDB.
 */
public final class VectorIndexMaintainer extends IndexMaintainer {

    /**
     * Returns which of the given ObjectIds already have an entry in this vector index.
     *
     * <p>Issues one point request per ObjectId against the {@code (ENTRIES, ObjectId)} key. A vector
     * index keys its entry directly by ObjectId with exactly one entry per ObjectId, so a single get
     * answers existence. All requests are dispatched before any is awaited, so the FoundationDB client
     * pipelines them over a single connection, giving a few round trips instead of one per ObjectId.
     * Each read is not a snapshot read, so it registers a read conflict on exactly that ObjectId's
     * entry key. A concurrent writer that sets the entry for one of these ObjectIds forces this
     * transaction to conflict on commit. This lets the background index builder skip ObjectIds already
     * indexed by online writers without double counting cardinality.
     *
     * @param tr            the FoundationDB transaction
     * @param indexSubspace the vector index's directory subspace
     * @param objectIds     the ObjectIds to probe, each as bytes
     * @return the subset of the given ObjectIds that are already indexed
     */
    public static Set<ObjectId> findIndexedObjectIds(
            Transaction tr,
            DirectorySubspace indexSubspace,
            List<byte[]> objectIds
    ) {
        if (objectIds.isEmpty()) {
            return Set.of();
        }

        // Dispatch all point requests without awaiting so the client pipelines them.
        List<CompletableFuture<byte[]>> futures = new ArrayList<>(objectIds.size());
        for (byte[] objectIdBytes : objectIds) {
            byte[] key = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), objectIdBytes));
            futures.add(tr.get(key));
        }

        Set<ObjectId> indexed = new HashSet<>();
        for (int i = 0; i < objectIds.size(); i++) {
            if (futures.get(i).join() != null) {
                indexed.add(new ObjectId(objectIds.get(i)));
            }
        }
        return indexed;
    }

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
