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
import com.kronotop.volume.Prefix;

public class IndexBuilder {

    /**
     * Sets an index with the specified parameters in the transaction.
     *
     * @param tr          the Transaction object used to set the index key-value pair
     * @param subspace    the BucketSubspace object providing the index subspace
     * @param shardId     the identifier for the shard where the index resides
     * @param prefix      the Prefix object representing the namespace prefix for the bucket
     * @param userVersion the user-specified version used for the version-stamped operation
     * @param index       the Index object containing details such as name, path, and type
     * @param metadata    the byte array representing {@code EntryMetadata}
     */
    public static void setIndex(Transaction tr, BucketSubspace subspace, int shardId, Prefix prefix, int userVersion, Index index, byte[] metadata) {
        // index-subspace / index-name / field-path / bson-type / versionstamped-key
        Subspace indexSubspace = subspace.getBucketIndexSubspace(shardId, prefix);
        Tuple indexTemplate = Tuple.from(index.name(), index.path(), index.type().getValue(), Versionstamp.incomplete(userVersion));
        byte[] indexKey = indexSubspace.packWithVersionstamp(indexTemplate);
        tr.set(indexKey, metadata);
    }
}
