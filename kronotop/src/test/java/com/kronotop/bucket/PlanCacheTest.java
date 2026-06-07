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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class PlanCacheTest {

    private static final UUID BUCKET_1 = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private static final UUID BUCKET_2 = UUID.fromString("00000000-0000-0000-0000-000000000002");

    private PlanCache cache;

    @BeforeEach
    void setUp() {
        cache = new PlanCache();
    }

    private PipelineNode createDummyPlan(int id) {
        return new PipelineNode() {
            @Override
            public int id() {
                return id;
            }

            @Override
            public PipelineNode next() {
                return null;
            }

            @Override
            public void connectNext(PipelineNode node) {
            }
        };
    }

    @Test
    void shouldReturnNullForMissingEntry() {
        assertNull(cache.get("production", BUCKET_1, 1L));
    }

    @Test
    void shouldStoreAndRetrievePlan() {
        PipelineNode plan = createDummyPlan(1);
        cache.put("production", BUCKET_1, 100L, plan);

        PlanCache.CachedPlan cached = cache.get("production", BUCKET_1, 100L);
        assertNotNull(cached);
        assertSame(plan, cached.plan());
    }

    @Test
    void shouldSetInsertedAtTimestamp() {
        long before = System.currentTimeMillis();
        cache.put("production", BUCKET_1, 100L, createDummyPlan(1));
        long after = System.currentTimeMillis();

        PlanCache.CachedPlan cached = cache.get("production", BUCKET_1, 100L);
        assertNotNull(cached);
        assertTrue(cached.insertedAt() >= before);
        assertTrue(cached.insertedAt() <= after);
    }

    @Test
    void shouldIsolateDifferentNamespaces() {
        PipelineNode plan1 = createDummyPlan(1);
        PipelineNode plan2 = createDummyPlan(2);

        cache.put("production", BUCKET_1, 100L, plan1);
        cache.put("staging", BUCKET_1, 100L, plan2);

        assertSame(plan1, cache.get("production", BUCKET_1, 100L).plan());
        assertSame(plan2, cache.get("staging", BUCKET_1, 100L).plan());
    }

    @Test
    void shouldIsolateDifferentBuckets() {
        PipelineNode plan1 = createDummyPlan(1);
        PipelineNode plan2 = createDummyPlan(2);

        cache.put("production", BUCKET_1, 100L, plan1);
        cache.put("production", BUCKET_2, 100L, plan2);

        assertSame(plan1, cache.get("production", BUCKET_1, 100L).plan());
        assertSame(plan2, cache.get("production", BUCKET_2, 100L).plan());
    }

    @Test
    void shouldIsolateDifferentShapes() {
        PipelineNode plan1 = createDummyPlan(1);
        PipelineNode plan2 = createDummyPlan(2);

        cache.put("production", BUCKET_1, 100L, plan1);
        cache.put("production", BUCKET_1, 200L, plan2);

        assertSame(plan1, cache.get("production", BUCKET_1, 100L).plan());
        assertSame(plan2, cache.get("production", BUCKET_1, 200L).plan());
    }

    @Test
    void shouldEvictOldestEntryWhenExceedingLimit() {
        String namespace = "production";
        UUID bucketId = BUCKET_1;

        // Fill cache to limit (200 entries)
        for (int i = 0; i < 200; i++) {
            cache.put(namespace, bucketId, i, createDummyPlan(i));
        }

        // All entries should be present
        assertNotNull(cache.get(namespace, bucketId, 0L));
        assertNotNull(cache.get(namespace, bucketId, 199L));

        // Add one more entry, should evict the first (shapeHash = 0)
        cache.put(namespace, bucketId, 200L, createDummyPlan(200));

        // First entry should be evicted
        assertNull(cache.get(namespace, bucketId, 0L));
        // New entry should be present
        assertNotNull(cache.get(namespace, bucketId, 200L));
        // Second entry should still be present
        assertNotNull(cache.get(namespace, bucketId, 1L));
    }

    @Test
    void shouldEvictInFifoOrder() {
        String namespace = "production";
        UUID bucketId = BUCKET_1;

        // Fill cache beyond limit
        for (int i = 0; i < 210; i++) {
            cache.put(namespace, bucketId, i, createDummyPlan(i));
        }

        // First 10 entries should be evicted (0-9)
        for (int i = 0; i < 10; i++) {
            assertNull(cache.get(namespace, bucketId, i), "Entry " + i + " should be evicted");
        }

        // Entries 10-209 should still be present
        for (int i = 10; i < 210; i++) {
            assertNotNull(cache.get(namespace, bucketId, i), "Entry " + i + " should be present");
        }
    }

    @Test
    void shouldInvalidateBucket() {
        cache.put("production", BUCKET_1, 100L, createDummyPlan(1));
        cache.put("production", BUCKET_1, 200L, createDummyPlan(2));
        cache.put("production", BUCKET_2, 100L, createDummyPlan(3));

        cache.invalidateBucket("production", BUCKET_1);

        assertNull(cache.get("production", BUCKET_1, 100L));
        assertNull(cache.get("production", BUCKET_1, 200L));
        // Other bucket should be unaffected
        assertNotNull(cache.get("production", BUCKET_2, 100L));
    }

    @Test
    void shouldInvalidateNamespace() {
        cache.put("production", BUCKET_1, 100L, createDummyPlan(1));
        cache.put("production", BUCKET_2, 100L, createDummyPlan(2));
        cache.put("staging", BUCKET_1, 100L, createDummyPlan(3));

        cache.invalidateNamespace("production");

        assertNull(cache.get("production", BUCKET_1, 100L));
        assertNull(cache.get("production", BUCKET_2, 100L));
        // Other namespace should be unaffected
        assertNotNull(cache.get("staging", BUCKET_1, 100L));
    }

    @Test
    void shouldInvalidateByPrefix() {
        cache.put("production", BUCKET_1, 100L, createDummyPlan(1));
        cache.put("production.users", BUCKET_1, 100L, createDummyPlan(2));
        cache.put("production.users.johndoe", BUCKET_1, 100L, createDummyPlan(3));
        cache.put("staging", BUCKET_1, 100L, createDummyPlan(4));
        cache.put("staging.users", BUCKET_1, 100L, createDummyPlan(5));

        cache.invalidateByPrefix("production");

        // All production namespaces should be invalidated
        assertNull(cache.get("production", BUCKET_1, 100L));
        assertNull(cache.get("production.users", BUCKET_1, 100L));
        assertNull(cache.get("production.users.johndoe", BUCKET_1, 100L));
        // Staging namespaces should be unaffected
        assertNotNull(cache.get("staging", BUCKET_1, 100L));
        assertNotNull(cache.get("staging.users", BUCKET_1, 100L));
    }

    @Test
    void shouldInvalidateByPrefixWithDotSeparator() {
        cache.put("prod", BUCKET_1, 100L, createDummyPlan(1));
        cache.put("prod.users", BUCKET_1, 100L, createDummyPlan(2));
        cache.put("production", BUCKET_1, 100L, createDummyPlan(3));
        cache.put("production.users", BUCKET_1, 100L, createDummyPlan(4));

        cache.invalidateByPrefix("prod.");

        // Only "prod.users" should be invalidated (starts with "prod.")
        assertNotNull(cache.get("prod", BUCKET_1, 100L));
        assertNull(cache.get("prod.users", BUCKET_1, 100L));
        assertNotNull(cache.get("production", BUCKET_1, 100L));
        assertNotNull(cache.get("production.users", BUCKET_1, 100L));
    }

    @Test
    void shouldClearAllEntries() {
        cache.put("production", BUCKET_1, 100L, createDummyPlan(1));
        cache.put("staging", BUCKET_2, 200L, createDummyPlan(2));

        cache.clear();

        assertNull(cache.get("production", BUCKET_1, 100L));
        assertNull(cache.get("staging", BUCKET_2, 200L));
    }

    @Test
    void shouldHandleConcurrentAccess() throws InterruptedException {
        int numThreads = 10;
        int opsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger errors = new AtomicInteger(0);

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < opsPerThread; i++) {
                        long shapeHash = threadId * opsPerThread + i;
                        PipelineNode plan = createDummyPlan((int) shapeHash);

                        cache.put("production", BUCKET_1, shapeHash, plan);
                        cache.get("production", BUCKET_1, shapeHash);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        assertEquals(0, errors.get(), "No errors should occur during concurrent access");
    }

    @Test
    void shouldOverwriteExistingEntry() {
        PipelineNode plan1 = createDummyPlan(1);
        PipelineNode plan2 = createDummyPlan(2);

        cache.put("production", BUCKET_1, 100L, plan1);
        cache.put("production", BUCKET_1, 100L, plan2);

        assertSame(plan2, cache.get("production", BUCKET_1, 100L).plan());
    }

    @Test
    void shouldHandleConcurrentLookupAndInvalidation() throws InterruptedException {
        // Behavior: Concurrent lookups and invalidations should not cause exceptions or data corruption
        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try {
            int opsPerThread = 500;
            CountDownLatch latch = new CountDownLatch(numThreads);
            AtomicInteger errors = new AtomicInteger(0);

            // Pre-populate cache
            for (int i = 0; i < 100; i++) {
                cache.put("production", BUCKET_1, i, createDummyPlan(i));
            }

            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < opsPerThread; i++) {
                            if (threadId % 2 == 0) {
                                // Reader/writer threads
                                cache.get("production", BUCKET_1, i);
                                cache.put("production", BUCKET_1, i, createDummyPlan(i));
                            } else {
                                // Invalidation threads
                                cache.invalidateBucket("production", BUCKET_1);
                            }
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            assertTrue(latch.await(30, TimeUnit.SECONDS));
            assertEquals(0, errors.get());
        } finally {
            executor.shutdown();
        }
    }

    @Test
    void shouldHandleHighWriteContention() throws InterruptedException {
        // Behavior: High write contention should not corrupt reads
        int numWriters = 5;
        int numReaders = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numWriters + numReaders);

        try {
            int opsPerThread = 500;
            CountDownLatch latch = new CountDownLatch(numWriters + numReaders);
            AtomicInteger errors = new AtomicInteger(0);

            // Writer threads
            for (int t = 0; t < numWriters; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < opsPerThread; i++) {
                            long shapeHash = (threadId * opsPerThread + i) % 300;
                            cache.put("production", BUCKET_1, shapeHash, createDummyPlan((int) shapeHash));
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            // Reader threads
            for (int t = 0; t < numReaders; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < opsPerThread; i++) {
                            long shapeHash = (threadId * opsPerThread + i) % 300;
                            PlanCache.CachedPlan cached = cache.get("production", BUCKET_1, shapeHash);
                            // Either null or valid - never corrupt
                            if (cached != null) {
                                assertNotNull(cached.plan());
                                assertTrue(cached.insertedAt() > 0);
                            }
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            assertTrue(latch.await(30, TimeUnit.SECONDS));
            assertEquals(0, errors.get());
        } finally {
            executor.shutdown();
        }
    }

    @Test
    void shouldHandleEmptyPrefixInvalidation() {
        // Behavior: Empty prefix invalidation should not throw exceptions
        cache.put("production", BUCKET_1, 100L, createDummyPlan(1));
        cache.put("staging", BUCKET_1, 100L, createDummyPlan(2));

        assertDoesNotThrow(() -> cache.invalidateByPrefix(""));
    }

    @Test
    void shouldEvictCorrectlyWhenFarExceedingLimit() {
        // Behavior: Inserting 250 entries should evict entries 0-49, keeping 50-249
        String namespace = "production";
        UUID bucketId = BUCKET_1;

        for (int i = 0; i < 250; i++) {
            cache.put(namespace, bucketId, i, createDummyPlan(i));
        }

        // Entries 0-49 should be evicted
        for (int i = 0; i < 50; i++) {
            assertNull(cache.get(namespace, bucketId, i), "Entry " + i + " should be evicted");
        }

        // Entries 50-249 should be present
        for (int i = 50; i < 250; i++) {
            assertNotNull(cache.get(namespace, bucketId, i), "Entry " + i + " should be present");
        }
    }
}
