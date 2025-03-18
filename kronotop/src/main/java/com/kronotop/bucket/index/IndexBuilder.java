// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.index;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BucketSubspace;
import com.kronotop.bucket.DefaultIndexes;
import com.kronotop.volume.Prefix;

public class IndexBuilder {
    private static final byte[] NULL_BYTES = new byte[]{0};

    /**
     * Sets an ID-based index entry in the given transaction and subspace for a specified shard and prefix.
     *
     * @param tr          the FoundationDB transaction used to write the index entry
     * @param subspace    the bucket subspace corresponding to the namespace and shard for the index
     * @param shardId     the identifier of the shard where the index entry will be stored
     * @param prefix      the Prefix object representing the key space subset for the index entry
     * @param userVersion the user-defined version associated with the versionstamp in the index key
     */
    public static void setIdIndex(Transaction tr, BucketSubspace subspace, int shardId, Prefix prefix, int userVersion) {
        Subspace indexSubspace = subspace.getBucketIndexSubspace(shardId, prefix);
        Tuple idIndexTemplate = Tuple.from(DefaultIndexes.ID.name(), Versionstamp.incomplete(userVersion));
        byte[] idIndexKey = indexSubspace.packWithVersionstamp(idIndexTemplate);
        tr.set(idIndexKey, NULL_BYTES);
    }
}
