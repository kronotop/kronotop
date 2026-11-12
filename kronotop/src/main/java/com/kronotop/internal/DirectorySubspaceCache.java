/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.internal;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.kronotop.KronotopException;
import com.kronotop.NoSuchDirectorySubspaceException;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.transaction.TransactionManager;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * DirectorySubspaceCache provides caching for DirectorySubspace instances based on different keys and shard IDs.
 * It uses a LoadingCache to manage DirectorySubspace instances for a specific duration.
 */
public final class DirectorySubspaceCache {
    private final String cluster;
    private final TransactionManager transactionManager;
    private final DirectoryLayer directoryLayer;

    private final EnumMap<Key, List<String>> subpaths = new EnumMap<>(Key.class);
    private final ConcurrentHashMap<Integer, List<String>> shards = new ConcurrentHashMap<>();

    private final LoadingCache<List<String>, DirectorySubspace> cache = CacheBuilder.newBuilder()
            .expireAfterAccess(Duration.ofSeconds(10))
            .build(new DirectorySubspaceLoader());

    public DirectorySubspaceCache(String cluster, TransactionManager transactionManager, DirectoryLayer directoryLayer) {
        this.cluster = cluster;
        this.transactionManager = transactionManager;
        this.directoryLayer = directoryLayer;

        for (Key name : Key.values()) {
            switch (name) {
                case CLUSTER_METADATA -> {
                    KronotopDirectoryNode directory = KronotopDirectory.
                            kronotop().
                            cluster(cluster).
                            metadata();
                    subpaths.put(Key.CLUSTER_METADATA, directory.toList());
                }
                case PREFIXES -> {
                    KronotopDirectoryNode node = KronotopDirectory.
                            kronotop().
                            cluster(cluster).
                            metadata().
                            prefixes();
                    subpaths.put(Key.PREFIXES, node.toList());
                }
                case NAMESPACE_TOMBSTONES -> {
                    KronotopDirectoryNode node = KronotopDirectory.
                            kronotop().
                            cluster(cluster).
                            metadata().
                            namespaceTombstones();
                    subpaths.put(Key.NAMESPACE_TOMBSTONES, node.toList());
                }
            }
        }
    }

    /**
     * Retrieves a DirectorySubspace instance from the cache based on the provided subpath.
     *
     * @param subpath the list of strings representing the subpath used as the key to fetch the DirectorySubspace from the cache.
     * @return the DirectorySubspace associated with the given subpath.
     * @throws RuntimeException if an ExecutionException occurs when accessing the cache.
     */
    public DirectorySubspace get(List<String> subpath) {
        try {
            return cache.get(subpath);
        } catch (ExecutionException | UncheckedExecutionException e) {
            if (e.getCause() instanceof CompletionException ex) {
                if (ex.getCause() instanceof NoSuchDirectoryException) {
                    throw new NoSuchDirectorySubspaceException(subpath);
                }
            }
            throw new KronotopException(e);
        }
    }

    /**
     * Retrieves a DirectorySubspace instance based on the provided key.
     *
     * @param key Enum value acting as an identifier to fetch the associated DirectorySubspace.
     * @return the DirectorySubspace associated with the given key.
     * @throws RuntimeException if an ExecutionException occurs when accessing the cache.
     */
    public DirectorySubspace get(Key key) {
        List<String> subpath = subpaths.get(key);
        return get(subpath);
    }

    /**
     * Returns the DirectorySubspace of the given shard.
     *
     * @param shardId the shard ID
     * @return the DirectorySubspace of the shard
     * @throws NoSuchDirectorySubspaceException if the shard has no directory
     */
    public DirectorySubspace get(int shardId) {
        List<String> subpath = shards.computeIfAbsent(shardId, k -> KronotopDirectory.
                kronotop().
                cluster(cluster).
                metadata().
                shards().
                shard(shardId).
                toList());

        return get(subpath);
    }

    public enum Key {
        CLUSTER_METADATA,
        PREFIXES,
        NAMESPACE_TOMBSTONES
    }

    /**
     * DirectorySubspaceLoader is a type of CacheLoader designed to handle the loading of DirectorySubspace
     * instances given a list of strings representing a path. It opens the path in a transaction
     * from the TransactionManager using the default DirectoryLayer.
     */
    private class DirectorySubspaceLoader extends CacheLoader<List<String>, DirectorySubspace> {
        @Override
        public @Nonnull DirectorySubspace load(@Nonnull List<String> path) {
            try (Transaction tr = transactionManager.create()) {
                return directoryLayer.open(tr, path).join();
            }
        }
    }
}
