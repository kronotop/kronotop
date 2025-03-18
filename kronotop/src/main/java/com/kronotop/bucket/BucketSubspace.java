// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket;

import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.foundationdb.namespace.SubspaceMagic;
import com.kronotop.volume.Prefix;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BucketSubspace {
    private final Namespace namespace;
    private final Subspace prefixesSubspace;
    private final Subspace indexSubspace;
    private final Map<String, Prefix> prefixes = new HashMap<>();
    private final Map<Integer, Map<Prefix, Subspace>> indexes = new HashMap<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();


    public BucketSubspace(Namespace namespace) {
        this.namespace = namespace;
        this.prefixesSubspace = namespace.getBucket().subspace(Tuple.from(SubspaceMagic.BUCKET_PREFIX.getValue()));
        this.indexSubspace = namespace.getBucket().subspace(Tuple.from(SubspaceMagic.BUCKET_INDEX.getValue()));
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public Subspace getPrefixesSubspace() {
        return prefixesSubspace;
    }

    /**
     * Retrieves the key associated with the specified bucket name within the namespace.
     *
     * @param bucket the name of the bucket for which the key is being retrieved
     * @return a byte array representing the key for the specified bucket
     */
    public byte[] getBucketKey(String bucket) {
        return prefixesSubspace.pack(bucket);
    }

    /**
     * Sets the prefix for a given bucket in the namespace.
     *
     * @param bucket the name of the bucket for which the prefix is being set
     * @param prefix the Prefix object to associate with the specified bucket
     */
    public void setBucketPrefix(String bucket, Prefix prefix) {
        lock.writeLock().lock();
        try {
            prefixes.putIfAbsent(bucket, prefix);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Retrieves the prefix associated with the specified bucket in the namespace.
     *
     * @param bucket the name of the bucket for which the prefix is to be retrieved
     * @return the Prefix object associated with the specified bucket, or null if no prefix is found
     */
    public Prefix getBucketPrefix(String bucket) {
        lock.readLock().lock();
        try {
            return prefixes.get(bucket);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Cleans up resources associated with a specified bucket within the namespace.
     * This involves removing the prefix associated with the bucket and any related indexes.
     *
     * @param bucket the name of the bucket to be cleaned up
     */
    public void cleanupBucket(String bucket) {
        lock.writeLock().lock();
        try {
            Prefix prefix = prefixes.remove(bucket);
            if (prefix != null) {
                indexes.values().forEach((index) -> {
                    index.remove(prefix);
                });
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Subspace getBucketIndexSubspaceIfExists(int shardId, Prefix prefix) {
        Map<Prefix, Subspace> index = indexes.get(shardId);
        if (index == null) {
            return null;
        }
        return index.get(prefix);
    }

    private Subspace createBucketIndexSubspace(int shardId, Prefix prefix) {
        Subspace subspace = indexSubspace.subspace(Tuple.from(shardId, prefix.asLong()));
        indexes.compute(shardId, (id, index) -> {
            if (index == null) {
                index = new HashMap<>();
            }
            index.put(prefix, subspace);
            return index;
        });
        return subspace;
    }

    /**
     * Retrieves the bucket index subspace for the specified shard and prefix.
     * If the subspace does not exist, it creates and initializes a new one.
     * <p>
     * This method employs a read-write locking mechanism to ensure thread-safe access and
     * modification of the subspace map.
     *
     * @param shardId the identifier of the shard for which the bucket index subspace is being retrieved
     * @param prefix  the Prefix object representing the key space subset in the shard
     * @return the Subspace object corresponding to the bucket index for the given shard and prefix
     */
    public Subspace getBucketIndexSubspace(int shardId, Prefix prefix) {
        lock.readLock().lock();
        try {
            Subspace subspace = getBucketIndexSubspaceIfExists(shardId, prefix);
            if (subspace != null) {
                return subspace;
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            // Check to read it from cache last time under the write lock.
            Subspace subspace = getBucketIndexSubspaceIfExists(shardId, prefix);
            if (subspace != null) {
                return subspace;
            }
            return createBucketIndexSubspace(shardId, prefix);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
