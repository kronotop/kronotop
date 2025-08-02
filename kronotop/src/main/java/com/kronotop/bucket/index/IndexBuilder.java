// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

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
    public static void setIDIndex(Transaction tr, int shardId, BucketMetadata metadata, AppendedEntry[] entries) {
        DirectorySubspace idIndexSubspace = metadata.indexes().getSubspace(DefaultIndexDefinition.ID);
        if (idIndexSubspace == null) {
            throw new KronotopException("Index '" + DefaultIndexDefinition.ID.name() + "' not found");
        }
        for (AppendedEntry entry : entries) {
            Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), Versionstamp.incomplete(entry.userVersion()));
            byte[] key = idIndexSubspace.packWithVersionstamp(tuple);
            IndexEntry indexEntry = new IndexEntry(shardId, entry.encodedMetadata());
            byte[] value = indexEntry.encode();

            tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, key, value);
            IndexUtil.increaseCardinality(tr, metadata.subspace(), DefaultIndexDefinition.ID.id());
        }
    }
}
