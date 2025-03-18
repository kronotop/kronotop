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

package com.kronotop;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.common.KronotopException;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.directory.Shards;

import javax.annotation.Nonnull;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * DirectorySubspaceCache provides caching for DirectorySubspace instances based on different keys and shard kinds.
 * It uses a LoadingCache to manage DirectorySubspace instances for a specific duration.
 */
public final class DirectorySubspaceCache {
    private final String cluster;
    private final Database database;

    private final EnumMap<Key, List<String>> subpaths = new EnumMap<>(Key.class);
    private final EnumMap<ShardKind, ConcurrentHashMap<Integer, List<String>>> shards = new EnumMap<>(ShardKind.class);
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, List<String>>> buckets = new ConcurrentHashMap<>();

    private final LoadingCache<List<String>, DirectorySubspace> cache = CacheBuilder.newBuilder()
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .build(new DirectorySubspaceLoader());

    public DirectorySubspaceCache(String cluster, Database database) {
        this.cluster = cluster;
        this.database = database;

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
            }
        }

        for (ShardKind shardKind : ShardKind.values()) {
            shards.put(shardKind, new ConcurrentHashMap<>());
        }
    }

    /**
     * Retrieves a DirectorySubspace instance from the cache based on the provided subpath.
     *
     * @param subpath the list of strings representing the subpath used as the key to fetch the DirectorySubspace from the cache.
     * @return the DirectorySubspace associated with the given subpath.
     * @throws RuntimeException if an ExecutionException occurs when accessing the cache.
     */
    private DirectorySubspace get(List<String> subpath) {
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
     * Retrieves a DirectorySubspace instance based on the provided ShardKind and shard ID.
     *
     * @param kind    the kind of shard to be fetched.
     * @param shardId the identifier for the specific shard.
     * @return the DirectorySubspace associated with the given ShardKind and shard ID.
     * @throws RuntimeException if the provided ShardKind is unknown or an error occurs when accessing the cache.
     */
    public DirectorySubspace get(ShardKind kind, int shardId) {
        List<String> subpath = shards.get(kind).computeIfAbsent(shardId, k -> {
            Shards root = KronotopDirectory.
                    kronotop().
                    cluster(cluster).
                    metadata().
                    shards();
            if (kind.equals(ShardKind.REDIS)) {
                return root.redis().shard(shardId).toList();
            } else if (kind.equals(ShardKind.BUCKET)) {
                return root.bucket().shard(shardId).toList();
            }
            throw new RuntimeException("Unknown shard kind: " + kind);
        });

        return get(subpath);
    }

    public DirectorySubspace get(String namespace, String bucket) {
        List<String> subpath = buckets.computeIfAbsent(namespace, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(bucket, k -> new LinkedList<>());
        return get(subpath);
    }

    public enum Key {
        CLUSTER_METADATA,
        PREFIXES,
    }

    /**
     * DirectorySubspaceLoader is a type of CacheLoader designed to handle the loading of DirectorySubspace
     * instances given a list of strings representing a path. It interacts with a database to open the specified
     * path using the default DirectoryLayer.
     */
    private class DirectorySubspaceLoader extends CacheLoader<List<String>, DirectorySubspace> {
        @Override
        public @Nonnull DirectorySubspace load(@Nonnull List<String> path) {
            return database.run(tr -> DirectoryLayer.
                    getDefault().
                    open(tr, path).
                    join());
        }
    }
}
