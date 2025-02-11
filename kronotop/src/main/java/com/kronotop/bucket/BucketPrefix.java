// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.hash.HashCode;
import com.kronotop.Context;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.volume.Prefix;

import java.util.UUID;

import static com.google.common.hash.Hashing.sipHash24;

public class BucketPrefix {
    /**
     * Creates a new prefix for the specified bucket within the given namespace, associates the prefix with
     * the bucket, and stores it in the database using the provided transaction. The method generates a unique
     * identifier for the prefix and ensures it is properly linked within the global and namespace-specific
     * subspaces.
     *
     * @param context   The context of the Kronotop instance for the current operation.
     * @param tr        The transaction to be used for database operations.
     * @param namespace The namespace where the bucket is located.
     * @param bucket    The name of the bucket to associate with the newly created prefix.
     * @return The newly created prefix associated with the specified bucket.
     */
    private static Prefix createPrefix(Context context, Transaction tr, Namespace namespace, String bucket) {
        UUID uuid = UUID.randomUUID();
        HashCode hashCode = sipHash24().hashBytes(uuid.toString().getBytes());

        byte[] bucketKey = namespace.getBucketPrefixesSubspace().pack(bucket);
        byte[] rawPrefix = hashCode.asBytes();
        tr.set(bucketKey, rawPrefix);

        KronotopDirectoryNode node = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                prefixes();
        DirectorySubspace globalPrefixesSubspace = DirectoryLayer.getDefault().createOrOpen(tr, node.toList()).join();

        Prefix prefix = new Prefix(hashCode);
        byte[] prefixKey = globalPrefixesSubspace.pack(Tuple.from((Object) rawPrefix));
        tr.set(prefixKey, bucketKey);
        return prefix;
    }

    /**
     * Retrieves the prefix associated with the specified bucket within the given namespace. If the prefix does not
     * exist, it creates a new prefix, stores it in the namespace's bucket prefix subspace, and associates it with
     * the given bucket.
     *
     * @param context   The context of the Kronotop instance for the current operation.
     * @param tr        The transaction to be used for database operations.
     * @param namespace The namespace under which the bucket resides.
     * @param bucket    The name of the bucket for which the prefix is being retrieved or created.
     * @return The prefix associated with the specified bucket.
     */
    public static Prefix getOrSetBucketPrefix(Context context, Transaction tr, Namespace namespace, String bucket) {
        Prefix prefix = namespace.getBucketPrefix(bucket);
        if (prefix != null) {
            return prefix;
        }

        byte[] bucketKey = namespace.getBucketPrefixesSubspace().pack(bucket);
        byte[] rawPrefix = tr.get(bucketKey).join();
        if (rawPrefix != null) {
            prefix = Prefix.fromBytes(rawPrefix);
            namespace.setBucketPrefix(bucket, prefix);
            return prefix;
        }
        return createPrefix(context, tr, namespace, bucket);
    }
}
