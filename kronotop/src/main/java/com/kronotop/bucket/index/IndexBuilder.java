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
import com.kronotop.bucket.DefaultIndexes;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.volume.Prefix;

public class IndexBuilder {
    private static final byte[] NULL_BYTES = new byte[]{0};

    /**
     * Sets an ID index in the specified transaction for the given shard and prefix within a namespace.
     * This operation creates a key in the appropriate subspace of the provided namespace and associates it
     * with a versionstamp and default value.
     *
     * @param tr          the FoundationDB transaction within which the index is being set
     * @param namespace   the logical namespace that provides access to the bucket index subspaces
     * @param shardId     the identifier of the shard where the index will be stored
     * @param prefix      the prefix representing the key space subset within the shard
     * @param userVersion the user-defined version used to generate an incomplete versionstamp for the index
     */
    public static void setIdIndex(Transaction tr, Namespace namespace, int shardId, Prefix prefix, int userVersion) {
        Subspace indexSubspace = namespace.getBucketIndexSubspace(shardId, prefix);
        Tuple idIndexTemplate = Tuple.from(DefaultIndexes.ID.name(), Versionstamp.incomplete(userVersion));
        byte[] idIndexKey = indexSubspace.packWithVersionstamp(idIndexTemplate);
        tr.set(idIndexKey, NULL_BYTES);
    }
}
