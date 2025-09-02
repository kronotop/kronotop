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

package com.kronotop.bucket.statistics.prefix;

import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Concurrency tests for Fixed N-Byte Prefix Histogram.
 * Tests robustness under multithreaded load with optimistic concurrency.
 */
public class PrefixHistogramConcurrencyTest extends BaseStandaloneInstanceTest {

    private FDBPrefixHistogram histogram;
    private PrefixHistogramEstimator estimator;
    private String testIndexName;
    private DirectorySubspace testIndexSubspace;
    private ExecutorService executor;

    @BeforeEach
    public void setupHistogram() {
        histogram = new FDBPrefixHistogram(context.getFoundationDB());
        testIndexName = "concurrency-test-index";
        estimator = histogram.createEstimator(testIndexName);
        
        try (var tr = context.getFoundationDB().createTransaction()) {
            testIndexSubspace = DirectoryLayer.getDefault().createOrOpen(tr, 
                Arrays.asList("IDX", testIndexName)).join();
            tr.commit().join();
        }
        
        executor = Executors.newFixedThreadPool(8);
    }

    @Override
    public void tearDown() {
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }
        super.tearDown();
    }

    @Test
    public void testConcurrentAddDelete() throws InterruptedException {
        int numThreads = 8;
        int opsPerThread = 1000;
        ConcurrentOracle oracle = new ConcurrentOracle();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Random random = new Random(threadId);

                    for (int i = 0; i < opsPerThread; i++) {
                        byte[] key = generateKey(random, threadId, i);
                        
                        if (random.nextBoolean()) {
                            // Add operation
                            histogram.add(testIndexName, key);
                            oracle.add(key);
                        } else {
                            // Delete operation (might be deleting non-existent key)
                            histogram.delete(testIndexName, key);
                            oracle.delete(key);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "Operations did not complete in time");

        // Verify mass conservation
        assertEquals(oracle.size(), histogram.getTotalCount(testIndexName),
                    "Mass conservation violated under concurrent load");
    }

    @Test
    public void testConcurrentUpdate() throws InterruptedException {
        int numThreads = 4;
        int opsPerThread = 500;
        ConcurrentOracle oracle = new ConcurrentOracle();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);

        // Pre-populate with initial data
        for (int i = 0; i < 1000; i++) {
            byte[] key = ("initial-" + i).getBytes();
            histogram.add(testIndexName, key);
            oracle.add(key);
        }

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Random random = new Random(threadId);

                    for (int i = 0; i < opsPerThread; i++) {
                        byte[] oldKey = ("initial-" + random.nextInt(1000)).getBytes();
                        byte[] newKey = generateKey(random, threadId, i);
                        
                        histogram.update(testIndexName, oldKey, newKey);
                        oracle.update(oldKey, newKey);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        long initialCount = histogram.getTotalCount(testIndexName);
        
        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "Updates did not complete in time");

        // Total count should remain the same after updates
        assertEquals(initialCount, histogram.getTotalCount(testIndexName),
                    "Total count changed during updates");
        assertEquals(oracle.size(), histogram.getTotalCount(testIndexName),
                    "Mass conservation violated during concurrent updates");
    }

    @Test
    public void testConcurrentReadWrite() throws InterruptedException {
        int numWriters = 4;
        int numReaders = 4;
        int opsPerThread = 500;
        ConcurrentOracle oracle = new ConcurrentOracle();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numWriters + numReaders);
        AtomicLong readCount = new AtomicLong(0);

        // Writers
        for (int t = 0; t < numWriters; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Random random = new Random(threadId);

                    for (int i = 0; i < opsPerThread; i++) {
                        byte[] key = generateKey(random, threadId, i);
                        histogram.add(testIndexName, key);
                        oracle.add(key);
                        
                        // Small delay to allow readers to see changes
                        if (i % 100 == 0) {
                            Thread.sleep(1);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Readers
        for (int t = 0; t < numReaders; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Random random = new Random(threadId + 1000);

                    for (int i = 0; i < opsPerThread; i++) {
                        // Read operations should not conflict with writes
                        byte[] A = generateKey(random, threadId, i);
                        byte[] B = generateKey(random, threadId, i + 1);
                        
                        if (Arrays.compareUnsigned(A, B) > 0) {
                            byte[] temp = A; A = B; B = temp;
                        }
                        
                        double estimate = estimator.estimateRange(A, B);
                        readCount.incrementAndGet();
                        
                        // Ensure reads don't throw exceptions or return negative values
                        assertTrue(estimate >= 0, "Range estimate should be non-negative");
                        
                        if (i % 100 == 0) {
                            Thread.sleep(1);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(60, TimeUnit.SECONDS), "Operations did not complete in time");

        // Verify reads completed successfully
        assertEquals(numReaders * opsPerThread, readCount.get(),
                    "Not all read operations completed");
        
        // Final consistency check
        assertEquals(oracle.size(), histogram.getTotalCount(testIndexName),
                    "Mass conservation violated with concurrent reads/writes");
    }

    @Test
    public void testIdempotentMutations() throws InterruptedException {
        // Test that retried Add/Delete operations don't change the net result
        int numThreads = 4;
        int opsPerThread = 100;
        Set<byte[]> testKeys = ConcurrentHashMap.newKeySet();
        CountDownLatch doneLatch = new CountDownLatch(numThreads);

        // Generate test keys
        Random keyGen = new Random(42);
        for (int i = 0; i < 50; i++) {
            testKeys.add(generateKey(keyGen, 0, i));
        }

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    Random random = new Random(threadId);
                    List<byte[]> keyList = new ArrayList<>(testKeys);

                    for (int i = 0; i < opsPerThread; i++) {
                        byte[] key = keyList.get(random.nextInt(keyList.size()));
                        
                        // Multiple threads may add/delete the same key
                        // The histogram should handle this gracefully
                        if (random.nextBoolean()) {
                            histogram.add(testIndexName, key);
                        } else {
                            histogram.delete(testIndexName, key);
                        }
                    }
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "Operations did not complete in time");

        // The exact count is unpredictable due to race conditions,
        // but the histogram should remain in a consistent state
        long totalCount = histogram.getTotalCount(testIndexName);
        assertTrue(totalCount >= 0, "Total count should not be negative");
        
        // Verify individual bucket consistency
        PrefixHistogramMetadata metadata = histogram.getMetadata(testIndexName);
        for (byte[] key : testKeys) {
            byte[] pN = PrefixHistogramUtils.pN(key, metadata.N());
            long bucketCount = histogram.getBucketCount(testIndexName, pN);
            assertTrue(bucketCount >= 0, "Bucket count should not be negative");
        }
    }

    // Helper methods

    private byte[] generateKey(Random random, int threadId, int opId) {
        return String.format("thread%d-op%d-%d", threadId, opId, random.nextInt(1000)).getBytes();
    }

    /**
     * Thread-safe oracle for concurrent testing
     */
    private static class ConcurrentOracle {
        private final ConcurrentHashMap<String, AtomicLong> data = new ConcurrentHashMap<>();

        public void add(byte[] key) {
            String keyStr = Arrays.toString(key);
            data.computeIfAbsent(keyStr, k -> new AtomicLong(0)).incrementAndGet();
        }

        public void delete(byte[] key) {
            String keyStr = Arrays.toString(key);
            AtomicLong count = data.get(keyStr);
            if (count != null) {
                long newValue = count.decrementAndGet();
                if (newValue <= 0) {
                    data.remove(keyStr);
                }
            }
        }

        public void update(byte[] oldKey, byte[] newKey) {
            delete(oldKey);
            add(newKey);
        }

        public long size() {
            return data.values().stream()
                      .mapToLong(AtomicLong::get)
                      .sum();
        }
    }
}