// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.hash.HashCode;
import com.kronotop.Context;
import com.kronotop.volume.Prefix;
import com.kronotop.volume.PrefixUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.google.common.hash.Hashing.sipHash24;

public class BucketPrefix {
    /**
     * Creates a unique prefix by generating a random UUID, converting it into a hash, and registering it with the given
     * bucket key in the specified transaction.
     *
     * @param context   The context of the Kronotop instance for the current operation.
     * @param tr        The transaction to be used for database operations.
     * @param bucketKey The key representing the bucket for which the prefix is to be registered.
     * @return A newly created {@link Prefix} object uniquely associated with the bucket key.
     */
    private static Prefix createPrefix(Context context, Transaction tr, byte[] bucketKey) {
        // We need a unique sequence of bytes. UUID will provide this.
        UUID uuid = UUID.randomUUID();
        HashCode hashCode = sipHash24().hashBytes(uuid.toString().getBytes());

        Prefix prefix = new Prefix(hashCode);
        PrefixUtils.register(context, tr, bucketKey, prefix);
        return prefix;
    }

    /**
     * Retrieves the bucket prefix for the specified bucket, or creates and sets a new one if it does not already exist.
     * This function checks if the prefix is present in the {@link BucketSubspace}, and if not, retrieves it from the
     * database using a {@link Transaction}. If still absent, a new prefix is created, set, and returned.
     *
     * @param context  The {@link Context} instance representing the state and configuration of the current operation.
     * @param tr       The {@link Transaction} used for database read and write operations.
     * @param subspace The {@link BucketSubspace} representing the logical grouping of bucket data.
     * @param bucket   The name of the bucket whose prefix is to be retrieved or set.
     * @return The {@link Prefix} object corresponding to the specified bucket.
     */
    public static Prefix getOrSetBucketPrefix(Context context, Transaction tr, BucketSubspace subspace, String bucket) {
        Prefix prefix = subspace.getBucketPrefix(bucket);
        if (prefix != null) {
            return prefix;
        }

        byte[] bucketKey = subspace.getBucketKey(bucket);
        byte[] rawPrefix = tr.get(bucketKey).join();
        if (rawPrefix != null) {
            prefix = Prefix.fromBytes(rawPrefix);
            subspace.setBucketPrefix(bucket, prefix);
            return prefix;
        }
        return createPrefix(context, tr, bucketKey);
    }

    /**
     * Retrieves a map of all bucket prefixes stored within the provided subspace.
     * The method queries the database using the transaction to retrieve all key-value pairs
     * in the range defined by the prefixes subspace and constructs a mapping of
     * bucket names to their corresponding {@link Prefix} objects.
     *
     * @param tr       The {@link Transaction} used for database operations.
     * @param subspace The {@link BucketSubspace} instance representing the subspace
     *                 to query for bucket prefixes.
     * @return A {@link Map} where the keys are bucket names (as strings) and the values
     * are {@link Prefix} objects corresponding to those buckets.
     */
    public static Map<String, Prefix> listBucketPrefixes(Transaction tr, BucketSubspace subspace) {
        Subspace prefixesSubspace = subspace.getPrefixesSubspace();
        byte[] begin = prefixesSubspace.pack();
        byte[] end = ByteArrayUtil.strinc(begin);

        HashMap<String, Prefix> result = new HashMap<>();
        Range range = new Range(begin, end);
        for (KeyValue entry : tr.getRange(range)) {
            Tuple key = prefixesSubspace.unpack(entry.getKey());
            Prefix prefix = Prefix.fromBytes(entry.getValue());
            result.put(key.getString(0), prefix);
        }
        return result;
    }
}
