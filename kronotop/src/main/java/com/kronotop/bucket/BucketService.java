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
     * Creates a new {@link Prefix} for the given {@link Namespace} and sets up the necessary mappings in the
     * specified transaction by using a randomly generated UUID.
     *
     * @param tr         The {@link Transaction} in which the prefix is being created and all necessary mappings
     *                   are being set. This transaction represents the atomic context for operations in the database.
     * @param namespace  The {@link Namespace} for which the prefix is being created. This namespace contains the
     *                   metadata and bucket mappings for organizing the directory structure.
     * @return The newly created {@link Prefix} instance. This prefix contains the unique identifier for the namespace's
     *         bucket and is stored in both the namespace and the transaction.
     */
    private Prefix createPrefix(Transaction tr, Namespace namespace) {
        UUID uuid = UUID.randomUUID();
        HashCode hashCode = sipHash24().hashBytes(uuid.toString().getBytes());

        byte[] rawPrefix = hashCode.asBytes();
        tr.set(namespace.getBucketPrefixKey(), rawPrefix);

        KronotopDirectoryNode node = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                prefixes();
        DirectorySubspace subspace = DirectoryLayer.getDefault().createOrOpen(tr, node.toList()).join();

        Prefix prefix = new Prefix(hashCode);
        byte[] key = subspace.pack(Tuple.from((Object) rawPrefix));
        tr.set(key, namespace.getBucket().pack());
        namespace.setBucketPrefix(prefix);
        return prefix;
    }

    /**
     * Attempts to retrieve the bucket {@link Prefix} for the specified {@link Namespace}. If the prefix
     * is not already set, it retrieves the value from the database or creates a new prefix if no value exists.
     *
     * @param tr        The {@link Transaction} used to retrieve or create the bucket prefix. This transaction
     *                  ensures atomic operations when accessing or setting the prefix in the database.
     * @param namespace The {@link Namespace} whose bucket prefix is being retrieved or created. This namespace
     *                  contains metadata and mappings necessary for organizing and accessing buckets.
     * @return The {@link Prefix} associated with the namespace's bucket. If the prefix does not exist, a new prefix
     *         is created and returned.
     */
    public Prefix getOrSetBucketPrefix(Transaction tr, Namespace namespace) {
        Prefix prefix = namespace.getBucketPrefix();
        if (prefix != null) {
            return prefix;
        }

        byte[] rawPrefix = tr.get(namespace.getBucketPrefixKey()).join();
        if (rawPrefix != null) {
            prefix = Prefix.fromBytes(rawPrefix);
            namespace.setBucketPrefix(prefix);
            return prefix;
        }
        return createPrefix(tr, namespace);
    }

    public void start() {

    }
}
