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

package com.kronotop.bucket;

import com.kronotop.CachedTimeService;
import com.kronotop.Context;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BucketMetadataCache is responsible for managing and caching metadata associated
 * with buckets within a specific namespace. This class provides efficient storage
 * and retrieval of bucket metadata by internally maintaining a cache of
 * {@link BucketMetadataRegistry} instances for namespaces.
 * <p>
 * The class also supports eviction of expired metadata entries through a dedicated
 * worker thread, ensuring that the cache does not grow indefinitely.
 */
public class BucketMetadataCache {
    private final Context context;
    private final Map<String, BucketMetadataRegistry> cache;

    public BucketMetadataCache(Context context) {
        this.context = context;
        this.cache = new ConcurrentHashMap<>();
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
        BucketMetadataRegistry registry = getBucketMetadataRegistry(namespace);
        return registry.getBucketMetadata(bucket);
    }

    /**
     * Associates the given bucket and namespace with the specified metadata.
     *
     * @param namespace the namespace under which the bucket resides
     * @param bucket    the name of the bucket for which metadata is being set
     * @param metadata  the {@code BucketMetadata} to be associated with the specified bucket
     */
    public void set(String namespace, String bucket, BucketMetadata metadata) {
        BucketMetadataRegistry registry = getBucketMetadataRegistry(namespace);
        registry.register(bucket, metadata);
    }

    /**
     * Creates a new eviction worker that performs periodic cleanup of expired bucket metadata
     * from the {@link BucketMetadataCache}.
     *
     * @param cachedTime the {@link CachedTimeService} used to provide efficient, cached,
     *                   and up-to-date system time for determining expiration.
     * @param ttl        the time-to-live (TTL) duration in milliseconds, which determines how long
     *                   bucket metadata entries are retained before becoming eligible for eviction.
     * @return a {@link Runnable} instance of {@link EvictionWorker}, configured with the
     * specified {@link CachedTimeService} and TTL value.
     */
    public Runnable createEvictionWorker(CachedTimeService cachedTime, long ttl) {
        return new EvictionWorker(cachedTime, ttl);
    }

    /**
     * The EvictionWorker class is used to manage the cleanup of expired bucket metadata
     * from a {@link BucketMetadataCache}. It implements the {@link Runnable} interface, allowing
     * it to be executed in a separate thread for periodic eviction tasks.
     * <p>
     * This worker operates by iterating over all registered {@link BucketMetadataRegistry}
     * instances and removing metadata entries that have exceeded their time-to-live (TTL) value.
     * Once a registry is cleared of all expired entries, it is removed from the cache if it
     * becomes empty.
     * <p>
     * The eviction process uses the following rules:
     * - An entry is considered expired if its last access time is older than the current time
     * minus the TTL value.
     * - A maximum of 10,000 entries are cleaned up in a single call to the
     * {@code cleanupBucketMetadataRegistry} method to avoid excessive processing.
     * <p>
     * This worker relies on the {@link CachedTimeService} to provide the current system
     * time in a cached and efficient manner.
     */
    class EvictionWorker implements Runnable {
        private static final int MAX_ENTRIES_PER_CLEANUP = 10000;
        private final CachedTimeService cachedTimeService;
        private final long ttl;

        private EvictionWorker(CachedTimeService cachedTimeService, long ttl) {
            this.cachedTimeService = cachedTimeService;
            this.ttl = ttl;
        }

        private void cleanupBucketMetadataRegistry(BucketMetadataRegistry registry) {
            Iterator<Map.Entry<String, BucketMetadataRegistry.BucketMetadataWrapper>> it = registry.entries().iterator();
            int total = 0;
            while (it.hasNext() && total < MAX_ENTRIES_PER_CLEANUP) {
                Map.Entry<String, BucketMetadataRegistry.BucketMetadataWrapper> entry = it.next();
                if (entry.getValue().getLastAccess() < cachedTimeService.getCurrentTimeInMilliseconds() - ttl) {
                    it.remove();
                    total++;
                }
            }
        }

        @Override
        public void run() {
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
        }
    }
}
