/*
 * Copyright (c) 2023-2025 Burak Sezer
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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.volume.AppendedEntry;

import java.util.List;

/**
 * Provides static methods for building and maintaining index entries in FoundationDB.
 * <p>
 * IndexBuilder handles the lifecycle of index entries including creation, updates, and deletion
 * for both primary (ID) and secondary indexes. Each secondary index entry consists of two parts:
 * <ul>
 *   <li><b>ENTRIES</b>: The forward index mapping (indexValue, versionstamp) → IndexEntry</li>
 *   <li><b>BACK_POINTER</b>: The reverse mapping (versionstamp, indexValue) → NULL_VALUE</li>
 * </ul>
 * <p>
 * Back pointers enable efficient lookup of all index entries for a given document versionstamp,
 * which is essential for update and delete operations where all affected index entries must
 * be located and modified.
 *
 * @see IndexEntry for the value stored in index entries
 * @see IndexSubspaceMagic for subspace key prefixes
 */
public class IndexBuilder {
    /**
     * Placeholder value used for back pointer entries. The actual indexed value is stored
     * in the key tuple, so the value can be minimal.
     */
    public final static byte[] NULL_VALUE = new byte[]{0};

    /**
     * Sets an index entry using a versionstamped key mutation for newly appended documents.
     * Uses FoundationDB's atomic versionstamp generation via SET_VERSIONSTAMPED_KEY mutation.
     *
     * @param tr             the transaction to mutate
     * @param definition     the index definition for cardinality tracking
     * @param indexSubspace  the directory subspace for this index
     * @param shardId        the shard containing the document
     * @param metadata       bucket metadata for cardinality updates
     * @param entryKeyTuple  the pre-constructed key tuple containing the incomplete versionstamp
     * @param entry          the appended entry with encoded metadata
     */
    private static void setIndexForBSONType(
            Transaction tr,
            IndexDefinition definition,
            DirectorySubspace indexSubspace,
            int shardId,
            BucketMetadata metadata,
            Tuple entryKeyTuple,
            AppendedEntry entry
    ) {
        byte[] key = indexSubspace.packWithVersionstamp(entryKeyTuple);
        IndexEntry indexEntry = new IndexEntry(shardId, entry.encodedMetadata());
        byte[] value = indexEntry.encode();

        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, key, value);
        IndexUtil.mutateCardinality(tr, metadata.subspace(), definition.id(), 1);
    }

    /**
     * Sets the ID index for a given set of appended entries in a transactional context.
     *
     * @param tr       The transaction object used to perform mutations against the underlying
     *                 segmented storage system.
     * @param shardId  The identifier of the shard in which the indexed entries reside.
     * @param metadata The bucket metadata containing information about indexes and storage subspaces.
     * @param entries  An array of appended entries to be indexed, each containing metadata and user version details.
     * @throws KronotopException if the required ID index subspace cannot be found in the metadata indexes.
     */
    public static void setPrimaryIndexEntry(Transaction tr, int shardId, BucketMetadata metadata, AppendedEntry[] entries) {
        Index index = metadata.indexes().getIndex(DefaultIndexDefinition.ID.selector(), IndexSelectionPolicy.READWRITE);
        if (index == null) {
            throw new KronotopException("Index '" + DefaultIndexDefinition.ID.name() + "' not found");
        }
        DirectorySubspace indexSubspace = index.subspace();

        for (AppendedEntry entry : entries) {
            Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), Versionstamp.incomplete(entry.userVersion()));
            setIndexForBSONType(tr, DefaultIndexDefinition.ID, indexSubspace, shardId, metadata, tuple, entry);
        }
    }

    /**
     * Sets an entry in the index subspace for a given index definition and value within a transactional context.
     * This method is responsible for handling the creation of both the index entry and its back pointer,
     * ensuring the consistency of index operations.
     *
     * @param tr         The transaction object used to perform mutations against the storage system.
     * @param definition The definition of the index, which includes metadata such as name, selector, and type.
     * @param shardId    The identifier of the shard in which the indexed entries reside.
     * @param metadata   The bucket metadata containing information about indexes, subspaces, and storage hierarchy.
     * @param indexValue The value of the index being stored, which serves as the index key during indexing operations.
     * @param entry      The appended entry containing details such as user version and metadata associated with the entry.
     * @throws KronotopException if the required index subspace cannot be retrieved from the metadata indexes.
     */
    public static void setIndexEntry(
            Transaction tr,
            IndexDefinition definition,
            int shardId,
            BucketMetadata metadata,
            Object indexValue,
            AppendedEntry entry
    ) {
        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READWRITE);
        if (index == null) {
            throw new KronotopException("Index '" + definition.name() + "' not found");
        }
        DirectorySubspace indexSubspace = index.subspace();

        Tuple entryKeyTuple = Tuple.from(
                IndexSubspaceMagic.ENTRIES.getValue(),
                indexValue,
                Versionstamp.incomplete(entry.userVersion())
        );
        setIndexForBSONType(tr, definition, indexSubspace, shardId, metadata, entryKeyTuple, entry);

        Tuple backPointerTuple = Tuple.from(
                IndexSubspaceMagic.BACK_POINTER.getValue(),
                Versionstamp.incomplete(entry.userVersion()),
                indexValue
        );

        byte[] backPointer = indexSubspace.packWithVersionstamp(backPointerTuple);
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, backPointer, NULL_VALUE);
    }

    /**
     * Inserts an index entry using an existing versionstamp rather than generating a new one.
     * Creates both the forward index entry and back pointer for the given document.
     * Used when reindexing or updating documents where the versionstamp is already known.
     *
     * @param tr           the transaction to mutate
     * @param definition   the index definition
     * @param metadata     bucket metadata for index lookup and cardinality
     * @param versionstamp the document's existing versionstamp
     * @param indexValue   the value to index (extracted from the document field)
     * @param shardId      the shard containing the document
     * @param entry        the encoded entry metadata
     * @throws KronotopException if the index subspace cannot be found
     */
    public static void insertIndexEntry(
            Transaction tr,
            IndexDefinition definition,
            BucketMetadata metadata,
            Versionstamp versionstamp,
            Object indexValue,
            int shardId,
            byte[] entry
    ) {
        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READWRITE);
        if (index == null) {
            throw new KronotopException("Index '" + definition.name() + "' not found");
        }
        DirectorySubspace indexSubspace = index.subspace();

        Tuple indexEntryTuple = Tuple.from(
                IndexSubspaceMagic.ENTRIES.getValue(),
                indexValue,
                versionstamp
        );
        byte[] indexEntryKey = indexSubspace.pack(indexEntryTuple);
        IndexEntry indexEntry = new IndexEntry(shardId, entry);
        tr.set(indexEntryKey, indexEntry.encode());
        IndexUtil.mutateCardinality(tr, metadata.subspace(), definition.id(), 1);

        Tuple backPointerTuple = Tuple.from(
                IndexSubspaceMagic.BACK_POINTER.getValue(),
                versionstamp,
                indexValue
        );
        byte[] backPointer = indexSubspace.pack(backPointerTuple);
        tr.set(backPointer, NULL_VALUE);
    }

    /**
     * Removes an entry from the primary (ID) index for a deleted document.
     * Decrements the index cardinality counter accordingly.
     *
     * @param tr           the transaction to mutate
     * @param versionstamp the versionstamp of the document being deleted
     * @param metadata     bucket metadata for index lookup and cardinality
     * @throws KronotopException if the primary index subspace cannot be found
     */
    public static void dropPrimaryIndexEntry(Transaction tr, Versionstamp versionstamp, BucketMetadata metadata) {
        Index index = metadata.indexes().getIndex(DefaultIndexDefinition.ID.selector(), IndexSelectionPolicy.READWRITE);
        if (index == null) {
            throw new KronotopException("Index '" + DefaultIndexDefinition.ID.name() + "' not found");
        }
        DirectorySubspace indexSubspace = index.subspace();
        Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), versionstamp);
        byte[] key = indexSubspace.pack(tuple);
        tr.clear(key);
        IndexUtil.mutateCardinality(tr, metadata.subspace(), DefaultIndexDefinition.ID.id(), -1);
    }

    /**
     * Removes all index entries for a document from a secondary index.
     * Uses back pointers to locate all entries for the given versionstamp, clears both the
     * forward index entries and back pointers, and decrements the cardinality counter.
     *
     * @param tr               the transaction to mutate
     * @param versionstamp     the versionstamp of the document being removed from this index
     * @param definition       the index definition for cardinality tracking
     * @param indexSubspace    the directory subspace for this index
     * @param metadataSubspace the bucket's metadata subspace for cardinality updates
     */
    public static void dropIndexEntry(
            Transaction tr,
            Versionstamp versionstamp,
            IndexDefinition definition,
            DirectorySubspace indexSubspace,
            DirectorySubspace metadataSubspace
    ) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue(), versionstamp));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        // Drop index keys
        long total = 0;
        List<KeyValue> allBackPointers = tr.getRange(begin, end).asList().join();
        for (KeyValue kv : allBackPointers) {
            Tuple unpacked = indexSubspace.unpack(kv.getKey());
            Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), unpacked.get(2), versionstamp);
            byte[] indexKey = indexSubspace.pack(tuple);
            tr.clear(indexKey);
            total--;
        }
        IndexUtil.mutateCardinality(tr, metadataSubspace, definition.id(), total);
        // Drop the back pointers
        tr.clear(begin.getKey(), end.getKey());
    }

    /**
     * Updates the metadata value of all index entries for a document without changing the indexed key.
     * Uses back pointers to locate all entries for the given versionstamp and overwrites their values.
     * Used when document metadata (e.g., segment location) changes but the indexed field value remains the same.
     *
     * @param tr            the transaction to mutate
     * @param versionstamp  the versionstamp identifying the document's index entries
     * @param entryMetadata the new encoded metadata to set on each entry
     * @param indexSubspace the directory subspace for this index
     */
    public static void updateEntryMetadata(
            Transaction tr,
            Versionstamp versionstamp,
            byte[] entryMetadata,
            DirectorySubspace indexSubspace
    ) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue(), versionstamp));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        List<KeyValue> allBackPointers = tr.getRange(begin, end).asList().join();
        for (KeyValue kv : allBackPointers) {
            Tuple unpacked = indexSubspace.unpack(kv.getKey());
            Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), unpacked.get(2), versionstamp);
            byte[] indexKey = indexSubspace.pack(tuple);
            tr.set(indexKey, entryMetadata);
        }
    }

    /**
     * Updates the metadata in the primary (ID) index entry for a document.
     * Overwrites the existing entry value with new metadata while keeping the same versionstamp key.
     * Used after document updates when the volume location changes.
     *
     * @param tr            the transaction to mutate
     * @param versionstamp  the document's versionstamp
     * @param metadata      bucket metadata for index lookup
     * @param shardId       the shard containing the document
     * @param entryMetadata the new encoded metadata
     * @throws KronotopException if the primary index subspace cannot be found
     */
    public static void updatePrimaryIndex(
            Transaction tr,
            Versionstamp versionstamp,
            BucketMetadata metadata,
            int shardId,
            byte[] entryMetadata
    ) {
        Index index = metadata.indexes().getIndex(DefaultIndexDefinition.ID.selector(), IndexSelectionPolicy.READWRITE);
        if (index == null) {
            throw new KronotopException("Index '" + DefaultIndexDefinition.ID.name() + "' not found");
        }
        DirectorySubspace indexSubspace = index.subspace();
        Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), versionstamp);
        byte[] key = indexSubspace.pack(tuple);
        IndexEntry indexEntry = new IndexEntry(shardId, entryMetadata);
        tr.set(key, indexEntry.encode());
    }

    /**
     * Creates or updates an index entry and its corresponding back pointer using a provided versionstamp.
     * This method sets both the index entry and back pointer directly using the given versionstamp,
     * rather than relying on FoundationDB's versionstamp generation.
     *
     * @param tr           The transaction object used to perform mutations against the database.
     * @param versionstamp The versionstamp to be used as part of both the index entry key and back pointer key.
     * @param container    The container object holding necessary metadata, index value, subspaces, shard ID,
     *                     and entry metadata required for constructing and storing the index entry and back pointer.
     */
    public static void setIndexEntryByVersionstamp(Transaction tr, Versionstamp versionstamp, IndexEntryContainer container) {
        Tuple indexKeyTuple = Tuple.from(
                IndexSubspaceMagic.ENTRIES.getValue(),
                container.indexValue(),
                versionstamp
        );

        byte[] indexKey = container.indexSubspace().pack(indexKeyTuple);
        IndexEntry indexEntry = new IndexEntry(container.shardId(), container.entryMetadata());

        tr.set(indexKey, indexEntry.encode());

        Tuple backPointerTuple = Tuple.from(
                IndexSubspaceMagic.BACK_POINTER.getValue(),
                versionstamp,
                container.indexValue()
        );

        byte[] backPointer = container.indexSubspace().pack(backPointerTuple);
        tr.set(backPointer, NULL_VALUE);

        IndexUtil.mutateCardinality(tr, container.metadata().subspace(), container.indexDefinition().id(), 1);
    }
}
