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

package com.kronotop.bucket;

import com.kronotop.bucket.pipeline.PipelineNode;
import it.unimi.dsi.fastutil.longs.Long2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.locks.StampedLock;

/**
 * Thread-safe cache for query execution plans, keyed by namespace, bucket ID, and query shape hash.
 *
 * <p>Uses fastutil's RBTreeMap for namespace keys to enable efficient prefix-based invalidation
 * (e.g., removing "production" invalidates "production.users.johndoe"). Uses StampedLock for
 * optimistic read access, optimized for read-heavy workloads typical of plan caching.</p>
 *
 * <p>Implements FIFO eviction per bucket when size exceeds {@link #MAX_ENTRIES_PER_BUCKET}.</p>
 */
public class PlanCache {
    private static final int MAX_ENTRIES_PER_BUCKET = 200;

    private final Object2ObjectRBTreeMap<String, HashMap<UUID, Long2ObjectLinkedOpenHashMap<CachedPlan>>> cache = new Object2ObjectRBTreeMap<>();
    private final StampedLock lock = new StampedLock();

    /**
     * Retrieves a cached plan for the given namespace, bucket, and query shape.
     *
     * @param namespace the namespace name (e.g., "production.users.johndoe")
     * @param bucketId  the bucket identifier
     * @param shapeHash the query shape hash
     * @return the cached plan wrapper, or null if not found
     */
    public CachedPlan get(String namespace, UUID bucketId, long shapeHash) {
        // Try optimistic read first
        long stamp = lock.tryOptimisticRead();
        CachedPlan cachedPlan = lookup(namespace, bucketId, shapeHash);

        if (lock.validate(stamp)) {
            return cachedPlan;
        }

        // Fall back to read lock
        stamp = lock.readLock();
        try {
            return lookup(namespace, bucketId, shapeHash);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    private CachedPlan lookup(String namespace, UUID bucketId, long shapeHash) {
        HashMap<UUID, Long2ObjectLinkedOpenHashMap<CachedPlan>> namespaceCache = cache.get(namespace);
        if (namespaceCache == null) {
            return null;
        }
        Long2ObjectLinkedOpenHashMap<CachedPlan> bucketCache = namespaceCache.get(bucketId);
        if (bucketCache == null) {
            return null;
        }
        return bucketCache.get(shapeHash);
    }

    /**
     * Stores a plan in the cache. Evicts oldest entry if the bucket exceeds the size limit.
     *
     * @param namespace the namespace name (e.g., "production.users.johndoe")
     * @param bucketId  the bucket identifier
     * @param shapeHash the query shape hash
     * @param plan      the execution plan to cache
     */
    public void put(String namespace, UUID bucketId, long shapeHash, PipelineNode plan) {
        long stamp = lock.writeLock();
        try {
            HashMap<UUID, Long2ObjectLinkedOpenHashMap<CachedPlan>> namespaceCache = cache.computeIfAbsent(namespace, k -> new HashMap<>());
            Long2ObjectLinkedOpenHashMap<CachedPlan> bucketCache = namespaceCache.get(bucketId);
            if (bucketCache == null) {
                bucketCache = new Long2ObjectLinkedOpenHashMap<>();
                namespaceCache.put(bucketId, bucketCache);
            }
            bucketCache.put(shapeHash, new CachedPlan(plan, System.currentTimeMillis()));

            // FIFO eviction: remove the oldest entry if size exceeds the limit
            if (bucketCache.size() > MAX_ENTRIES_PER_BUCKET) {
                bucketCache.removeFirst();
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Invalidates all cached plans for a bucket.
     * Call this when bucket indexes change.
     *
     * @param namespace the namespace name
     * @param bucketId  the bucket identifier
     */
    public void invalidateBucket(String namespace, UUID bucketId) {
        long stamp = lock.writeLock();
        try {
            HashMap<UUID, Long2ObjectLinkedOpenHashMap<CachedPlan>> namespaceCache = cache.get(namespace);
            if (namespaceCache != null) {
                namespaceCache.remove(bucketId);
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Invalidates all cached plans for an exact namespace.
     *
     * @param namespace the namespace name
     */
    public void invalidateNamespace(String namespace) {
        long stamp = lock.writeLock();
        try {
            cache.remove(namespace);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Invalidates all cached plans for namespaces matching the given prefix.
     * For example, the prefix "production" invalidates "production", "production.users",
     * "production.users.johndoe", etc.
     *
     * @param prefix the namespace prefix to match
     */
    public void invalidateByPrefix(String prefix) {
        long stamp = lock.writeLock();
        try {
            // Get all keys from prefix to prefix + MAX_VALUE (lexicographically)
            ObjectSortedSet<String> keysToRemove = cache.subMap(prefix, prefix + Character.MAX_VALUE).keySet();
            // Remove in a separate step to avoid ConcurrentModificationException
            for (String key : keysToRemove.toArray(new String[0])) {
                cache.remove(key);
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Clears the entire cache.
     */
    public void clear() {
        long stamp = lock.writeLock();
        try {
            cache.clear();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Wrapper for cached pipeline nodes with insertion timestamp.
     *
     * @param plan       the cached execution plan
     * @param insertedAt timestamp in milliseconds when the plan was cached
     */
    public record CachedPlan(PipelineNode plan, long insertedAt) {
    }
}
