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

import com.kronotop.Context;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.StampedLock;
import java.util.function.LongSupplier;

/**
 * Caches bucket metadata per namespace, backed by a {@link BucketMetadataRegistry} for each
 * namespace.
 *
 * <p>Entries can be invalidated by bucket or by namespace prefix. A background eviction worker
 * drops entries that have not been accessed within their TTL, so the cache stays bounded.
 */
public class BucketMetadataCache {
    private final Context context;
    private final StampedLock lock = new StampedLock();
    private final ConcurrentSkipListMap<String, BucketMetadataRegistry> cache;

    public BucketMetadataCache(Context context) {
        this.context = context;
        this.cache = new ConcurrentSkipListMap<>();
    }

    private BucketMetadataRegistry getBucketMetadataRegistry(String namespace) {
        return cache.computeIfAbsent(namespace, (k) -> new BucketMetadataRegistry(context));
    }

    /**
     * Retrieves the metadata for a specific bucket within a given namespace.
     *
     * @param namespace the namespace under which the bucket resides.
     * @param bucket    the name of the bucket for which metadata is being retrieved.
     * @return the {@code BucketMetadata} associated with the given bucket,
     * or {@code null} if no metadata is found for the bucket.
     */
    public BucketMetadata get(String namespace, String bucket) {
        long stamp = lock.tryOptimisticRead();
        BucketMetadataRegistry registry = getBucketMetadataRegistry(namespace);
        BucketMetadata metadata = registry.getBucketMetadata(bucket);
        if (lock.validate(stamp)) {
            return metadata;
        }

        stamp = lock.readLock();
        try {
            registry = getBucketMetadataRegistry(namespace);
            return registry.getBucketMetadata(bucket);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    /**
     * Removes the cached metadata for a specific bucket within the given namespace.
     *
     * @param namespace the namespace containing the bucket
     * @param bucket    the name of the bucket to invalidate
     */
    public void invalidate(String namespace, String bucket) {
        long stamp = lock.writeLock();
        try {
            BucketMetadataRegistry registry = cache.get(namespace);
            if (registry == null) {
                return;
            }
            registry.remove(bucket);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Removes the cached metadata for a namespace and all its child namespaces.
     * For example, invalidating "a.b" removes "a.b", "a.b.c", "a.b.c.d", etc.,
     * but keeps "a" and "a.b2".
     *
     * @param prefix the namespace prefix to invalidate
     */
    public void invalidate(String prefix) {
        long stamp = lock.writeLock();
        try {
            cache.remove(prefix);

            String childPrefix = prefix + ".";
            String endKey = childPrefix + Character.MAX_VALUE;

            cache.subMap(childPrefix, true, endKey, false).clear();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Associates the given bucket and namespace with the specified metadata.
     *
     * @param namespace the namespace under which the bucket resides
     * @param bucket    the name of the bucket for which metadata is being set
     * @param metadata  the {@code BucketMetadata} to be associated with the specified bucket
     */
    public void set(String namespace, String bucket, BucketMetadata metadata) {
        long stamp = lock.writeLock();
        try {
            BucketMetadataRegistry registry = getBucketMetadataRegistry(namespace);
            registry.register(bucket, metadata);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Creates a new eviction worker that performs periodic cleanup of expired bucket metadata
     * from the {@link BucketMetadataCache}.
     *
     * @param timeSupplier a supplier for the current time in milliseconds
     * @param ttl          the time-to-live (TTL) duration in milliseconds, which determines how long
     *                     bucket metadata entries are retained before becoming eligible for eviction.
     * @return a {@link Runnable} instance of {@link EvictionWorker}, configured with the
     * specified time supplier and TTL value.
     */
    public Runnable createEvictionWorker(LongSupplier timeSupplier, long ttl) {
        return new EvictionWorker(timeSupplier, ttl);
    }

    /**
     * Removes bucket metadata that has outlived its TTL. Runs on a separate thread and is meant to
     * be scheduled periodically.
     *
     * <p>It walks the registries in the cache, drops entries whose last access is older than
     * {@code now - ttl}, and removes a registry once it becomes empty. Each sweep drops at most
     * {@value #MAX_ENTRIES_PER_CLEANUP} entries, and a run visits at most that many registries, to
     * bound the work.
     */
    class EvictionWorker implements Runnable {
        private static final int MAX_ENTRIES_PER_CLEANUP = 10000;
        private final LongSupplier timeSupplier;
        private final long ttl;

        private EvictionWorker(LongSupplier timeSupplier, long ttl) {
            this.timeSupplier = timeSupplier;
            this.ttl = ttl;
        }

        private void cleanupBucketMetadataRegistry(BucketMetadataRegistry registry) {
            Iterator<Map.Entry<String, BucketMetadataRegistry.BucketMetadataWrapper>> it = registry.entries().iterator();
            int total = 0;
            while (it.hasNext() && total < MAX_ENTRIES_PER_CLEANUP) {
                Map.Entry<String, BucketMetadataRegistry.BucketMetadataWrapper> entry = it.next();
                if (entry.getValue().getLastAccess() < timeSupplier.getAsLong() - ttl) {
                    it.remove();
                    total++;
                }
            }
        }

        @Override
        public void run() {
            long stamp = lock.writeLock();
            try {
                int total = 0;
                for (Map.Entry<String, BucketMetadataRegistry> entry : cache.entrySet()) {
                    if (total >= MAX_ENTRIES_PER_CLEANUP) {
                        break;
                    }
                    BucketMetadataRegistry registry = entry.getValue();
                    cleanupBucketMetadataRegistry(registry);
                    if (registry.isEmpty()) {
                        cache.remove(entry.getKey(), registry);
                    }
                    total++;
                }
            } finally {
                lock.unlockWrite(stamp);
            }
        }
    }
}
