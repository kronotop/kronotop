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

    private static void setIndexForBSONType(
            Transaction tr,
            IndexDefinition definition,
            int shardId,
            BucketMetadata metadata,
            Tuple tuple,
            AppendedEntry entry
    ) {
        DirectorySubspace indexSubspace = metadata.indexes().getSubspace(definition.selector());
        if (indexSubspace == null) {
            throw new KronotopException("Index '" + definition.name() + "' not found");
        }
        byte[] key = indexSubspace.packWithVersionstamp(tuple);
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
        for (AppendedEntry entry : entries) {
            Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), Versionstamp.incomplete(entry.userVersion()));
            setIndexForBSONType(tr, DefaultIndexDefinition.ID, shardId, metadata, tuple, entry);
        }
    }

    /**
     * Sets an index entry for a given value in a transactional context. This method constructs a
     * tuple for the index entry based on the provided definition, shard ID, bucket metadata, and
     * index value, and delegates the setting operation to a BSON-type-specific handler.
     *
     * @param tr         The transaction object used to perform mutations against the underlying
     *                   segmented storage system.
     * @param definition The definition of the index, containing metadata such as its name, selector,
     *                   BSON type, and sort order.
     * @param shardId    The identifier of the shard in which the indexed entry resides.
     * @param metadata   The bucket metadata containing information about indexes and storage subspaces.
     * @param indexValue The value being indexed. This value is used as part of the index key in the table.
     * @param entry      The appended entry containing metadata and user-defined version details
     *                   associated with the indexed entry.
     */
    public static void setIndexEntry(Transaction tr, IndexDefinition definition, int shardId, BucketMetadata metadata, Object indexValue, AppendedEntry entry) {
        Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), indexValue, Versionstamp.incomplete(entry.userVersion()));
        setIndexForBSONType(tr, definition, shardId, metadata, tuple, entry);
    }
}
