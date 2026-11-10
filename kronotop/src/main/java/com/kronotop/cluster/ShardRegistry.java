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
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.internal.KronotopDirectoryLayer;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Caches the shard ids of the cluster. The ids are read from the FoundationDB directory layer and may be
 * non-contiguous.
 */
public class ShardRegistry {
    private final DirectoryLayer directoryLayer;
    private final List<String> shardsPath;
    private final ReentrantLock lock = new ReentrantLock();
    private volatile ShardIds shardIds = new ShardIds(List.of(), Set.of());

    public ShardRegistry(Config config, String clusterName) {
        this.directoryLayer = KronotopDirectoryLayer.fromConfig(config);
        this.shardsPath = KronotopDirectory.kronotop().cluster(clusterName).metadata().shards().toList();
    }

    /**
     * Returns the shard ids of the cluster.
     *
     * @return sorted, unmodifiable list of shard ids
     */
    public List<Integer> getShardIds() {
        return shardIds.ids();
    }

    /**
     * Checks whether a shard with the given id exists.
     *
     * @param shardId the shard id to check
     * @return true if the shard exists
     */
    public boolean isValidShardId(int shardId) {
        return shardIds.idSet().contains(shardId);
    }

    /**
     * Reloads the shard ids from the FoundationDB directory layer.
     *
     * @param tr the read transaction used to list the directory
     */
    public void refresh(ReadTransaction tr) {
        lock.lock();
        try {
            List<Integer> ids = listShardIds(tr);
            this.shardIds = new ShardIds(Collections.unmodifiableList(ids), Set.copyOf(ids));
        } finally {
            lock.unlock();
        }
    }

    private List<Integer> listShardIds(ReadTransaction tr) {
        List<String> children;
        try {
            children = directoryLayer.list(tr, shardsPath).join();
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

    private record ShardIds(List<Integer> ids, Set<Integer> idSet) {
    }
}
