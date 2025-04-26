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
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BucketSubspace;
import com.kronotop.volume.Prefix;
import org.bson.BsonType;

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
    public static void packIndex(Transaction tr, BucketSubspace subspace, int shardId, Prefix prefix, int userVersion, Index index, byte[] metadata) {
        // index-subspace / index-name / field-path / bson-type / versionstamped-key
        Subspace indexSubspace = subspace.getBucketIndexSubspace(shardId, prefix);
        Tuple indexTemplate = indexTemplate(index, userVersion);
        byte[] indexKey = indexSubspace.packWithVersionstamp(indexTemplate);
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, indexKey, metadata);
    }

    /**
     * Constructs a {@code Tuple} representing an index template.
     * The tuple consists of the index name, path, BSON type as a numeric value,
     * and a versionstamp using the provided user version.
     *
     * @param index       the {@code Index} object containing the name, path, and BSON type
     * @param userVersion the user-specified version used to create an incomplete versionstamp
     * @return a {@code Tuple} containing the index name, path, BSON type value, and the incomplete versionstamp
     */
    private static Tuple indexTemplate(Index index, int userVersion) {
        return Tuple.from(index.name(), index.path(), index.type().getValue(), Versionstamp.incomplete(userVersion));
    }

    /**
     * Unpacks the given byte array to reconstruct an {@code UnpackedIndex} object.
     * Extracts the information about index name, path, BSON type, and versionstamp
     * from the encoded data stored in the subspace.
     *
     * @param indexSubspace the {@code Subspace} object representing the index subspace
     * @param data          the byte array containing the packed index data
     * @return an {@code UnpackedIndex} object that contains the extracted index details
     *         and versionstamp
     * @throws IllegalStateException if the unpacked data does not contain exactly four elements
     */
    public static UnpackedIndex unpackIndex(Subspace indexSubspace, byte[] data) {
        Tuple unpacked = indexSubspace.unpack(data);
        if (unpacked.size() != 4) {
            throw new IllegalStateException("Unpacked index size must be 4");
        }
        String name = (String) unpacked.get(0);
        String path = (String) unpacked.get(1);
        long type = (long) unpacked.get(2);
        BsonType bsonType = BsonType.findByValue((int)type);

        Index index = new Index(name, path, bsonType);
        Versionstamp versionstamp = (Versionstamp) unpacked.get(3);
        return new UnpackedIndex(versionstamp, index);
    }
}
