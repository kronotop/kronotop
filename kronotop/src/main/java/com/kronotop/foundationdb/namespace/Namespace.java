/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.foundationdb.namespace;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.volume.Prefix;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Represents a logical namespace within a FoundationDB structure, providing subspaces
 * for managing Zmap data, bucket prefixes, and indexes. The namespace serves as an
 * abstraction layer for organizing and accessing data hierarchically.
 * <p>
 * This class is thread-safe for concurrent read and write operations through the use
 * of a {@link ReadWriteLock}.
 */
public class Namespace {
    private static final byte ZMapSubspaceMagic = 0x01;
    private static final byte BucketSubspaceMagic = 0x02;
    private static final byte BucketPrefixSubspaceMagic = 0x03;
    private static final byte BucketIndexSubspaceMagic = 0x04;
    private final String name;
    private final Subspace zmapSubspace;
    private final Subspace bucketPrefixesSubspace;
    private final Subspace bucketIndexSubspace;
    private final Map<String, Prefix> prefixes = new HashMap<>();
    private final Map<Prefix, Subspace> indexes = new HashMap<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public Namespace(@Nonnull String name, @Nonnull DirectorySubspace root) {
        this.name = name;
        this.zmapSubspace = root.subspace(Tuple.from(ZMapSubspaceMagic));
        Subspace bucketSubspace = root.subspace(Tuple.from(BucketSubspaceMagic));
        this.bucketPrefixesSubspace = bucketSubspace.subspace(Tuple.from(BucketPrefixSubspaceMagic));
        this.bucketIndexSubspace = bucketSubspace.subspace(Tuple.from(BucketIndexSubspaceMagic));
    }

    /**
     * Returns the name of the Namespace.
     *
     * @return The name of the Namespace.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the Zmap subspace associated with the namespace.
     *
     * @return The Zmap subspace.
     */
    public Subspace getZMap() {
        return zmapSubspace;
    }

    /**
     * Retrieves the subspace used to store bucket prefixes within the namespace.
     *
     * @return The Subspace object representing the bucket prefixes subspace.
     */
    public Subspace getBucketPrefixesSubspace() {
        return bucketPrefixesSubspace;
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
                indexes.remove(prefix);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Retrieves the subspace associated with the given prefix from the bucket index subspace.
     * If the subspace for the specified prefix does not already exist, it is created and added to the cache.
     *
     * @param prefix the {@code Prefix} object for which the bucket index subspace is to be retrieved
     * @return the {@code Subspace} associated with the given prefix
     */
    public Subspace getBucketIndexSubspace(Prefix prefix) {
        lock.readLock().lock();
        try {
            Subspace subspace = indexes.get(prefix);
            if (subspace != null) {
                return subspace;
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            Subspace subspace = indexes.get(prefix);
            if (subspace != null) {
                return subspace;
            }
            subspace = bucketIndexSubspace.subspace(Tuple.from(prefix.asLong()));
            indexes.put(prefix, subspace);
            return subspace;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
