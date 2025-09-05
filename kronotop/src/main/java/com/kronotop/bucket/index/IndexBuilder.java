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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.volume.AppendedEntry;

public class IndexBuilder {
    private final static byte[] NULL_VALUE = new byte[]{0};

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
        IndexUtil.increaseCardinality(tr, metadata.subspace(), definition.id());
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
    public static void setIDIndexEntry(Transaction tr, int shardId, BucketMetadata metadata, AppendedEntry[] entries) {
        DirectorySubspace indexSubspace = metadata.indexes().getSubspace(DefaultIndexDefinition.ID.selector());
        if (indexSubspace == null) {
            throw new KronotopException("Index '" + DefaultIndexDefinition.ID.name() + "' not found");
        }

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
        DirectorySubspace indexSubspace = metadata.indexes().getSubspace(definition.selector());
        if (indexSubspace == null) {
            throw new KronotopException("Index '" + definition.name() + "' not found");
        }

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
}
