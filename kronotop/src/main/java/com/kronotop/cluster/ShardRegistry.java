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

package com.kronotop.cluster;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.kronotop.KronotopException;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.internal.KronotopDirectoryLayer;
import com.typesafe.config.Config;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Cached registry of known shard IDs per {@link ShardKind}.
 *
 * <p>STASH shard IDs are always derived from config (contiguous 0..N-1).
 * BUCKET shard IDs are discovered from FoundationDB's DirectoryLayer and may be non-contiguous.
 */
public class ShardRegistry {
    private final Config config;
    private final String clusterName;
    private final DirectoryLayer directoryLayer;
    private final ReentrantLock lock = new ReentrantLock();
    private final Set<ShardKind> enabledKinds;
    private final List<Integer> stashShardIds;
    private volatile BucketShards bucketShards = new BucketShards(List.of(), Set.of());
    public ShardRegistry(Config config, String clusterName) {
        this.config = config;
        this.clusterName = clusterName;
        this.directoryLayer = KronotopDirectoryLayer.fromConfig(config);

        EnumSet<ShardKind> kinds = EnumSet.of(ShardKind.BUCKET);

        boolean stashEnabled = config.getBoolean("stash.enabled");
        if (stashEnabled) {
            kinds.add(ShardKind.STASH);
            int count = config.getInt("stash.shards");
            if (count <= 0) {
                throw new KronotopException("stash.shards must be greater than 0");
            }
            List<Integer> ids = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                ids.add(i);
            }
            stashShardIds = Collections.unmodifiableList(ids);
        } else {
            stashShardIds = List.of();
        }

        this.enabledKinds = Collections.unmodifiableSet(kinds);
    }

    /**
     * Returns the set of shard kinds that are enabled in the current configuration.
     */
    public Set<ShardKind> getShardKinds() {
        return enabledKinds;
    }

    /**
     * Returns the sorted list of shard IDs for the given kind.
     *
     * @param kind the shard kind
     * @return sorted, unmodifiable list of shard IDs
     */
    public List<Integer> getShardIds(ShardKind kind) {
        if (kind == ShardKind.STASH) {
            return stashShardIds;
        }
        return bucketShards.ids();
    }

    /**
     * Checks whether the given shard ID is valid for the specified kind.
     *
     * @param kind    the shard kind
     * @param shardId the shard ID to validate
     * @return true if the shard ID exists
     */
    public boolean isValidShardId(ShardKind kind, int shardId) {
        if (kind == ShardKind.STASH) {
            int count = config.getInt("stash.shards");
            return shardId >= 0 && shardId < count;
        }
        return bucketShards.idSet().contains(shardId);
    }

    private List<Integer> refreshCommon(ReadTransaction tr, List<String> path) {
        List<String> children;
        try {
            children = directoryLayer.list(tr, path).join();
        } catch (Exception e) {
            // Directory may not exist yet (pre-initialization)
            return List.of();
        }

        List<Integer> ids = new ArrayList<>(children.size());
        for (String child : children) {
            ids.add(Integer.parseInt(child));
        }
        Collections.sort(ids);
        return ids;
    }

    private void refreshBucket(ReadTransaction tr) {
        List<String> bucketDirPath = KronotopDirectory
                .kronotop()
                .cluster(clusterName)
                .metadata()
                .shards()
                .bucket()
                .toList();

        List<Integer> ids = refreshCommon(tr, bucketDirPath);
        this.bucketShards = new BucketShards(
                Collections.unmodifiableList(ids),
                Set.copyOf(ids)
        );
    }

    /**
     * Reloads the BUCKET shard list from FoundationDB's DirectoryLayer.
     *
     * @param tr a read transaction to use for directory listing
     */
    public void refresh(ReadTransaction tr, ShardKind shardKind) {
        lock.lock();
        try {
            if (shardKind == ShardKind.BUCKET) {
                refreshBucket(tr);
            }
            // swallow STASH, it's static.
        } finally {
            lock.unlock();
        }
    }

    private record BucketShards(List<Integer> ids, Set<Integer> idSet) {
    }
}
