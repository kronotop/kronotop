/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.vector;

import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.VectorIndex;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Supplier;

/**
 * A thread-safe registry for managing vector graph index groups organized by namespace, bucket, and index ID.
 *
 * <p>Groups are stored in a three-level hierarchy: namespace → bucket → indexId → VectorGraphIndexGroup.
 * The namespace key supports prefix-based removal using a sorted map, enabling efficient
 * cleanup of entire namespace trees.
 */
public class VectorGraphIndexRegistry {
    private final ConcurrentSkipListMap<String, ConcurrentHashMap<String, ConcurrentHashMap<Long, VectorGraphIndexGroup>>> registry = new ConcurrentSkipListMap<>();

    /**
     * Retrieves the index group registered under the specified namespace, bucket, and index ID.
     *
     * @return the VectorGraphIndexGroup, or null if not found
     */
    public VectorGraphIndexGroup get(String namespace, String bucket, long indexId) {
        ConcurrentHashMap<String, ConcurrentHashMap<Long, VectorGraphIndexGroup>> buckets = registry.get(namespace);
        if (buckets == null) {
            return null;
        }
        ConcurrentHashMap<Long, VectorGraphIndexGroup> indexes = buckets.get(bucket);
        if (indexes == null) {
            return null;
        }
        return indexes.get(indexId);
    }

    /**
     * Retrieves the index group for the given key. If absent, invokes the supplier to load or create
     * the group. If the supplier returns null, an empty group is created as a fallback.
     * ConcurrentHashMap.computeIfAbsent guarantees the supplier runs exactly once per key —
     * concurrent callers block until the supplier completes and receive the same group instance.
     * This ensures a single background bootstrap is triggered per vector index.
     */
    public VectorGraphIndexGroup computeIfAbsent(
            BucketMetadata metadata,
            VectorIndex index,
            Supplier<VectorGraphIndexGroup> supplier
    ) {
        ConcurrentHashMap<String, ConcurrentHashMap<Long, VectorGraphIndexGroup>> buckets =
                registry.computeIfAbsent(metadata.namespace(), ignored -> new ConcurrentHashMap<>());

        ConcurrentHashMap<Long, VectorGraphIndexGroup> indexes =
                buckets.computeIfAbsent(metadata.name(), ignored -> new ConcurrentHashMap<>());

        return indexes.computeIfAbsent(index.definition().id(), ignored -> supplier.get());
    }

    /**
     * Removes a single index group identified by namespace, bucket, and index ID.
     * Closes all indexes in the group.
     *
     * @return true if the group was removed, false if not found
     */
    public boolean remove(String namespace, String bucket, long indexId) {
        ConcurrentHashMap<String, ConcurrentHashMap<Long, VectorGraphIndexGroup>> buckets = registry.get(namespace);
        if (buckets == null) {
            return false;
        }
        ConcurrentHashMap<Long, VectorGraphIndexGroup> indexes = buckets.get(bucket);
        if (indexes == null) {
            return false;
        }
        VectorGraphIndexGroup group = indexes.remove(indexId);
        if (group != null) {
            group.closeAll();
        }
        return group != null;
    }

    /**
     * Removes all index groups for a specific bucket within a namespace.
     * Closes all indexes in the removed groups.
     */
    public void remove(String namespace, String bucket) {
        ConcurrentHashMap<String, ConcurrentHashMap<Long, VectorGraphIndexGroup>> buckets = registry.get(namespace);
        if (buckets == null) {
            return;
        }
        ConcurrentHashMap<Long, VectorGraphIndexGroup> indexes = buckets.remove(bucket);
        if (indexes != null) {
            for (VectorGraphIndexGroup group : indexes.values()) {
                group.closeAll();
            }
        }
    }

    /**
     * Removes all index groups under namespaces matching the given prefix.
     * Closes all indexes in the removed groups.
     *
     * @return the set of bucket UUIDs from the removed groups
     */
    public Set<UUID> remove(String prefix) {
        Set<UUID> bucketIds = new HashSet<>();
        NavigableMap<String, ConcurrentHashMap<String, ConcurrentHashMap<Long, VectorGraphIndexGroup>>> subMap =
                registry.subMap(prefix, true, prefix + Character.MAX_VALUE, true);
        for (ConcurrentHashMap<String, ConcurrentHashMap<Long, VectorGraphIndexGroup>> buckets : subMap.values()) {
            for (ConcurrentHashMap<Long, VectorGraphIndexGroup> indexes : buckets.values()) {
                for (VectorGraphIndexGroup group : indexes.values()) {
                    bucketIds.add(group.getBucketId());
                    group.closeAll();
                }
            }
        }
        subMap.clear();
        return bucketIds;
    }

    /**
     * Flushes all unflushed on-heap indexes in every group to disk.
     */
    public void flushAll(Path dataDir) {
        for (ConcurrentHashMap<String, ConcurrentHashMap<Long, VectorGraphIndexGroup>> buckets : registry.values()) {
            for (ConcurrentHashMap<Long, VectorGraphIndexGroup> indexes : buckets.values()) {
                for (VectorGraphIndexGroup group : indexes.values()) {
                    group.flush(dataDir);
                }
            }
        }
    }

    /**
     * Closes all registered index groups and clears the registry.
     */
    public void closeAll() {
        for (ConcurrentHashMap<String, ConcurrentHashMap<Long, VectorGraphIndexGroup>> buckets : registry.values()) {
            for (ConcurrentHashMap<Long, VectorGraphIndexGroup> indexes : buckets.values()) {
                for (VectorGraphIndexGroup group : indexes.values()) {
                    group.closeAll();
                }
            }
        }
        registry.clear();
    }
}
