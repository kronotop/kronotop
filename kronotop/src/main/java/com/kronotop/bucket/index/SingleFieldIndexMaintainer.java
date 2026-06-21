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

package com.kronotop.bucket.index;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.CollatorCache;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Maintains single field indexes in FoundationDB for Bucket data structure.
 *
 * <p>Single field indexes use the following key structures:
 * <ul>
 *   <li>Index entries: {@code (ENTRIES, indexValue, ObjectId) → IndexEntry}</li>
 *   <li>Back pointers: {@code (BACK_POINTER, ObjectId, indexValue) → null}</li>
 * </ul>
 *
 * <p>Back pointers enable efficient deletion by allowing reverse lookup from ObjectId to all
 * index values associated with that document.
 */
public final class SingleFieldIndexMaintainer extends IndexMaintainer {

    /**
     * Constructs the key for a single field index entry.
     *
     * @param index      the index containing the subspace
     * @param indexValue the field value being indexed
     * @param objectId   the document's ObjectId as bytes
     * @return the packed key bytes
     */
    private static byte[] getSingleFieldIndexEntryKey(Index index, Object indexValue, byte[] objectId) {
        // Single Field Index Key Structure: (ENTRIES, indexValue, ObjectId)
        Tuple indexEntryTuple = Tuple.from(
                IndexSubspaceMagic.ENTRIES.getValue(),
                indexValue,
                objectId
        );
        return index.subspace().pack(indexEntryTuple);
    }

    /**
     * Creates a single field index entry with an associated back pointer.
     *
     * @param tr            the FoundationDB transaction
     * @param index         the resolved index
     * @param metadata      the bucket metadata
     * @param indexValue    the field value being indexed
     * @param objectId      the document's ObjectId as bytes
     * @param indexEntry    the pre-encoded IndexEntry bytes
     * @param collatorCache the collator cache for collation-aware indexing
     */
    public static void setEntry(
            Transaction tr,
            Index index,
            BucketMetadata metadata,
            Object indexValue,
            byte[] objectId,
            byte[] indexEntry,
            CollatorCache collatorCache
    ) {
        if (indexValue instanceof ObjectId objectIdValue) {
            indexValue = objectIdValue.toByteArray();
        }
        Collation collation = resolveCollation(index.definition(), metadata);
        indexValue = applyCollation(indexValue, collation, collatorCache);

        byte[] key = getSingleFieldIndexEntryKey(index, indexValue, objectId);
        tr.set(key, indexEntry);

        IndexUtil.mutateCardinality(tr, metadata.subspace(), index.definition().id(), 1);

        // Back pointer: (BACK_POINTER, ObjectId, indexValue)
        Tuple backPointerTuple = Tuple.from(
                IndexSubspaceMagic.BACK_POINTER.getValue(),
                objectId,
                indexValue
        );
        byte[] backPointer = index.subspace().pack(backPointerTuple);
        tr.set(backPointer, NULL_VALUE);
    }

    /**
     * Inserts a single field index entry, building the IndexEntry from the supplied shard and metadata.
     *
     * <p>Unlike {@link #setEntry} which uses an incomplete versionstamp via atomic mutation, this method
     * accepts pre-computed shard and entry metadata and writes the entry directly.
     *
     * @param tr            the FoundationDB transaction
     * @param index         the resolved index
     * @param metadata      the bucket metadata
     * @param objectId      the document's ObjectId as bytes
     * @param indexValue    the field value being indexed
     * @param shardId       the shard containing the document
     * @param entry         the encoded entry metadata
     * @param collatorCache the collator cache for collation-aware indexing
     */
    public static void insertEntry(
            Transaction tr,
            Index index,
            BucketMetadata metadata,
            byte[] objectId,
            Object indexValue,
            int shardId,
            byte[] entry,
            CollatorCache collatorCache
    ) {
        if (indexValue instanceof ObjectId objectIdValue) {
            indexValue = objectIdValue.toByteArray();
        }
        Collation collation = resolveCollation(index.definition(), metadata);
        indexValue = applyCollation(indexValue, collation, collatorCache);
        byte[] key = getSingleFieldIndexEntryKey(index, indexValue, objectId);

        IndexEntry indexEntry = new IndexEntry(shardId, entry);
        tr.set(key, indexEntry.encode());

        IndexUtil.mutateCardinality(tr, metadata.subspace(), index.definition().id(), 1);

        Tuple backPointerTuple = Tuple.from(
                IndexSubspaceMagic.BACK_POINTER.getValue(),
                objectId,
                indexValue
        );
        byte[] backPointer = index.subspace().pack(backPointerTuple);
        tr.set(backPointer, NULL_VALUE);
    }

    /**
     * Returns which of the given ObjectIds already have at least one back pointer in this index.
     *
     * <p>Issues one point request per ObjectId against the {@code (BACK_POINTER, ObjectId, indexValue)}
     * keyspace. All requests are dispatched before any is awaited, so the FoundationDB client
     * pipelines them over a single connection, giving a few round trips instead of one per ObjectId.
     * Each read is not a snapshot read, so it registers a narrow read conflict range over exactly that
     * ObjectId's back pointers. A concurrent writer that adds or removes an entry for one of these
     * ObjectIds forces this transaction to conflict on commit. This lets the background index builder
     * skip ObjectIds already indexed by online writers without double counting cardinality.
     *
     * @param tr            the FoundationDB transaction
     * @param indexSubspace the index's directory subspace
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
        List<CompletableFuture<List<KeyValue>>> futures = new ArrayList<>(objectIds.size());
        for (byte[] objectIdBytes : objectIds) {
            byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue(), objectIdBytes));
            futures.add(tr.getRange(prefix, ByteArrayUtil.strinc(prefix), 1).asList());
        }

        Set<ObjectId> indexed = new HashSet<>();
        for (int i = 0; i < objectIds.size(); i++) {
            if (!futures.get(i).join().isEmpty()) {
                indexed.add(new ObjectId(objectIds.get(i)));
            }
        }
        return indexed;
    }

    /**
     * Removes all index data for a document including entries and back pointers.
     *
     * <p>Uses back pointers to find all indexed values for the document, then clears the
     * corresponding index entries and updates cardinality.
     *
     * @param tr               the FoundationDB transaction
     * @param objectId         the document's ObjectId as bytes
     * @param definition       the index definition
     * @param indexSubspace    the index's directory subspace
     * @param metadataSubspace the bucket's metadata subspace for cardinality updates
     */
    public static void dropEntry(
            Transaction tr,
            byte[] objectId,
            SingleFieldIndexDefinition definition,
            DirectorySubspace indexSubspace,
            DirectorySubspace metadataSubspace
    ) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue(), objectId));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        // Drop index keys
        long total = 0;
        for (KeyValue kv : tr.getRange(begin, end)) {
            Tuple unpacked = indexSubspace.unpack(kv.getKey());
            Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), unpacked.get(2), objectId);
            byte[] indexKey = indexSubspace.pack(tuple);
            tr.clear(indexKey);
            total--;
        }
        IndexUtil.mutateCardinality(tr, metadataSubspace, definition.id(), total);
        // Drop the back pointers
        tr.clear(begin.getKey(), end.getKey());
    }

    /**
     * Updates the entry metadata for all index entries of a document without changing the indexed keys.
     *
     * <p>Uses back pointers to locate all index entries for the document, then overwrites
     * their values with the new IndexEntry bytes.
     *
     * @param tr            the FoundationDB transaction
     * @param objectId      the document's ObjectId as bytes
     * @param indexEntry    the new encoded IndexEntry bytes
     * @param indexSubspace the index's directory subspace
     */
    public static void updateIndexEntry(
            Transaction tr,
            byte[] objectId,
            byte[] indexEntry,
            DirectorySubspace indexSubspace
    ) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue(), objectId));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        List<KeyValue> allBackPointers = tr.getRange(begin, end).asList().join();
        for (KeyValue kv : allBackPointers) {
            Tuple unpacked = indexSubspace.unpack(kv.getKey());
            Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), unpacked.get(2), objectId);
            byte[] key = indexSubspace.pack(tuple);
            tr.set(key, indexEntry);
        }
    }

    /**
     * Creates a single field index entry using an {@link IndexEntryContainer}.
     *
     * <p>Convenience method that extracts all required values from the container and creates
     * the index entry with back pointer and cardinality update.
     *
     * @param tr            the FoundationDB transaction
     * @param objectId      the document's ObjectId as bytes
     * @param container     the container holding index value, subspace, metadata, and shard information
     * @param collatorCache the collator cache for collation-aware indexing
     */
    public static void setEntryByObjectId(Transaction tr, byte[] objectId, IndexEntryContainer container, CollatorCache collatorCache) {
        Object indexValue = container.indexValue();
        if (indexValue instanceof ObjectId objectIdValue) {
            indexValue = objectIdValue.toByteArray();
        }
        Collation collation = resolveCollation(container.indexDefinition(), container.metadata());
        indexValue = applyCollation(indexValue, collation, collatorCache);

        Tuple indexKeyTuple = Tuple.from(
                IndexSubspaceMagic.ENTRIES.getValue(),
                indexValue,
                objectId
        );

        byte[] key = container.indexSubspace().pack(indexKeyTuple);
        IndexEntry indexEntry = new IndexEntry(container.shardId(), container.entryMetadata());
        tr.set(key, indexEntry.encode());

        Tuple backPointerTuple = Tuple.from(
                IndexSubspaceMagic.BACK_POINTER.getValue(),
                objectId,
                indexValue
        );

        byte[] backPointer = container.indexSubspace().pack(backPointerTuple);
        tr.set(backPointer, NULL_VALUE);

        IndexUtil.mutateCardinality(tr, container.metadata().subspace(), container.indexDefinition().id(), 1);
    }
}
