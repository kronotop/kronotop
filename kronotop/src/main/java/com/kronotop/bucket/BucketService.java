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
import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.bucket.handlers.BucketInsertHandler;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.server.ServerKind;
import com.kronotop.volume.Prefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.hash.Hashing.sipHash24;

public class BucketService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "Bucket";
    protected static final Logger LOGGER = LoggerFactory.getLogger(BucketService.class);
    private final ConcurrentHashMap<Integer, BucketShard> shards = new ConcurrentHashMap<>();

    public BucketService(Context context) {
        super(context, NAME);

        int numberOfShards = context.getConfig().getInt("bucket.shards");
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            BucketShard shard = new AbstractBucketShard(context, shardId);
            shards.put(shardId, shard);
        }

        handlerMethod(ServerKind.EXTERNAL, new BucketInsertHandler(this));
    }

    public BucketShard getShard(int shardId) {
        return shards.get(shardId);
    }

    /**
     * Creates a new prefix for the specified bucket within the given namespace and associates it
     * with a globally unique identifier. The prefix is stored in the namespace's bucket prefixes
     * subspace and updated in the global prefixes subspace.
     *
     * @param tr The transaction to be used for database operations.
     * @param namespace The namespace under which the bucket resides.
     * @param bucket The name of the bucket for which the prefix is being created.
     * @return The created prefix associated with the specified bucket.
     */
    private Prefix createPrefix(Transaction tr, Namespace namespace, String bucket) {
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
     * Retrieves the existing prefix for the specified bucket in the given namespace or creates a new one
     * if it does not exist. It first checks if the prefix is available within the namespace, then looks it
     * up in the transaction. If the prefix is still not found, a new prefix will be created.
     *
     * @param tr The transaction to be used for operations in the database.
     * @param namespace The namespace under which the bucket is located.
     * @param bucket The name of the bucket for which the prefix is retrieved or set.
     * @return The prefix associated with the specified bucket in the namespace.
     */
    public Prefix getOrSetBucketPrefix(Transaction tr, Namespace namespace, String bucket) {
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
        return createPrefix(tr, namespace, bucket);
    }

    public void start() {

    }
}
