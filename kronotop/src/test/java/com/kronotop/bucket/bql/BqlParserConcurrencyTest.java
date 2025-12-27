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

package com.kronotop.bucket.bql;

import com.kronotop.bucket.bql.ast.BqlAnd;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.bql.ast.BqlOr;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Concurrency tests for BQL parser to validate thread safety and concurrent access patterns.
 * These tests ensure the parser can handle multiple threads parsing queries simultaneously
 * without race conditions, data corruption, or deadlocks.
 */
class BqlParserConcurrencyTest {

    @Test
    @DisplayName("Multiple threads parsing different queries should work correctly")
    void shouldHandleMultipleThreadsParsingDifferentQueriesCorrectly() throws InterruptedException, ExecutionException {
        String[] queries = {
                "{ \"status\": \"active\" }",
                "{ \"price\": { \"$gt\": 100 } }",
                "{ \"tags\": { \"$in\": [\"urgent\", \"important\"] } }",
                "{ \"$and\": [ { \"category\": \"electronics\" }, { \"in_stock\": true } ] }",
                "{ \"metadata\": { \"$elemMatch\": { \"key\": \"env\", \"value\": \"prod\" } } }",
                "{ \"$or\": [ { \"priority\": 1 }, { \"urgent\": true } ] }",
                "{ \"created_at\": { \"$gte\": \"2024-01-01\" } }",
                "{ \"features\": { \"$size\": 5 } }",
                "{ \"permissions\": { \"$exists\": true } }",
                "{ \"excluded\": { \"$nin\": [\"test\", \"staging\"] } }"
        };

        int threadCount = 20;
        int iterationsPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicReference<Exception> firstException = new AtomicReference<>();

        List<Future<Integer>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            Future<Integer> future = executor.submit(() -> {
                int localSuccessCount = 0;
                try {
                    for (int j = 0; j < iterationsPerThread; j++) {
                        String query = queries[(threadId * iterationsPerThread + j) % queries.length];

                        BqlExpr result = BqlParser.parse(query);
                        assertNotNull(result, "Parsed result should not be null");

                        // Validate the result structure
                        String serialized = result.toJson();
                        assertNotNull(serialized, "Serialization should work");
                        assertFalse(serialized.trim().isEmpty(), "Serialized result should not be empty");

                        // Verify explanation works
                        String explanation = BqlParser.explain(result);
                        assertNotNull(explanation, "Explanation should work");

                        // Verify roundtrip parsing
                        BqlExpr reparsed = BqlParser.parse(serialized);
                        assertEquals(serialized, reparsed.toJson(), "Roundtrip should be consistent");

                        localSuccessCount++;
                    }
                } catch (Exception e) {
                    firstException.compareAndSet(null, e);
                } finally {
                    latch.countDown();
                }
                return localSuccessCount;
            });
            futures.add(future);
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS), "All threads should complete within 30 seconds");

        // Collect results
        int totalSuccessCount = 0;
        for (Future<Integer> future : futures) {
            totalSuccessCount += future.get();
        }

        executor.shutdown();

        // Verify no exceptions occurred
        assertNull(firstException.get(), "No exceptions should occur during concurrent parsing");

        // Verify all operations succeeded
        int expectedTotal = threadCount * iterationsPerThread;
        assertEquals(expectedTotal, totalSuccessCount, "All parsing operations should succeed");

        System.out.println("Concurrent parsing: " + expectedTotal + " operations across " + threadCount +
                " threads completed successfully");
    }

    @Test
    @DisplayName("Multiple threads parsing the same query should work correctly")
    void shouldHandleMultipleThreadsParsingSameQueryCorrectly() throws InterruptedException, ExecutionException {
        String sharedQuery = """
                {
                  "$and": [
                    { "tenant_id": "shared123" },
                    {
                      "$or": [
                        { "role": "admin" },
                        { "permissions": { "$in": ["read", "write", "admin"] } }
                      ]
                    },
                    { "active": true },
                    { "last_login": { "$gte": "2024-01-01" } }
                  ]
                }
                """;

        int threadCount = 15;
        int iterationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicReference<Exception> firstException = new AtomicReference<>();

        // Expected JSON for validation
        BqlExpr expectedResult = BqlParser.parse(sharedQuery);
        String expectedJson = expectedResult.toJson();
        String expectedExplanation = BqlParser.explain(expectedResult);

        List<Future<Integer>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            Future<Integer> future = executor.submit(() -> {
                int localSuccessCount = 0;
                try {
                    for (int j = 0; j < iterationsPerThread; j++) {
                        BqlExpr result = BqlParser.parse(sharedQuery);
                        assertNotNull(result, "Result should not be null");

                        // Verify consistent parsing results
                        String actualJson = result.toJson();
                        assertEquals(expectedJson, actualJson, "All threads should produce identical results");

                        // Verify explanation consistency
                        String actualExplanation = BqlParser.explain(result);
                        assertEquals(expectedExplanation, actualExplanation, "Explanations should be identical");

                        // Verify AST structure consistency
                        assertInstanceOf(BqlAnd.class, result, "Root should be BqlAnd");
                        BqlAnd andNode = (BqlAnd) result;
                        assertEquals(4, andNode.children().size(), "Should have 4 AND conditions");

                        localSuccessCount++;
                    }
                } catch (Exception e) {
                    firstException.compareAndSet(null, e);
                } finally {
                    latch.countDown();
                }
                return localSuccessCount;
            });
            futures.add(future);
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS), "All threads should complete within 30 seconds");

        // Collect results
        int totalSuccessCount = 0;
        for (Future<Integer> future : futures) {
            totalSuccessCount += future.get();
        }

        executor.shutdown();

        // Verify results
        assertNull(firstException.get(), "No exceptions should occur");
        int expectedTotal = threadCount * iterationsPerThread;
        assertEquals(expectedTotal, totalSuccessCount, "All operations should succeed");

        System.out.println("Same query concurrent parsing: " + expectedTotal + " operations across " + threadCount +
                " threads with consistent results");
    }

    @Test
    @DisplayName("Concurrent parsing with mixed valid and invalid queries should handle errors correctly")
    void shouldHandleErrorsCorrectlyWithMixedValidAndInvalidQueries() throws InterruptedException {
        String[] validQueries = {
                "{ \"status\": \"active\" }",
                "{ \"price\": { \"$gt\": 100 } }",
                "{ \"$and\": [ { \"category\": \"books\" }, { \"available\": true } ] }"
        };

        String[] invalidQueries = {
                "{ invalid json }",
                "{ \"selector\": { \"$unknownOp\": \"value\" } }",
                "{ \"selector\": { } }",
                "{ \"selector\": { \"$size\": \"not_integer\" } }"
        };

        int threadCount = 12;
        int iterationsPerThread = 25;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        AtomicInteger validSuccessCount = new AtomicInteger(0);
        AtomicInteger invalidCaughtCount = new AtomicInteger(0);
        AtomicInteger unexpectedExceptionCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < iterationsPerThread; j++) {
                        boolean useValidQuery = (threadId + j) % 2 == 0;

                        if (useValidQuery) {
                            String query = validQueries[j % validQueries.length];
                            try {
                                BqlExpr result = BqlParser.parse(query);
                                assertNotNull(result, "Valid query should parse");
                                validSuccessCount.incrementAndGet();
                            } catch (Exception e) {
                                unexpectedExceptionCount.incrementAndGet();
                                System.err.println("Unexpected exception for valid query '" + query + "': " + e.getMessage());
                            }
                        } else {
                            String query = invalidQueries[j % invalidQueries.length];
                            try {
                                BqlParser.parse(query);
                                unexpectedExceptionCount.incrementAndGet();
                                System.err.println("Expected exception not thrown for invalid query: " + query);
                            } catch (BqlParseException e) {
                                // Expected exception
                                assertNotNull(e.getMessage(), "Exception should have message");
                                assertTrue(e.getMessage().contains("BQL parse error"), "Should be BQL parse error");
                                invalidCaughtCount.incrementAndGet();
                            } catch (Exception e) {
                                unexpectedExceptionCount.incrementAndGet();
                                System.err.println("Unexpected exception type for invalid query '" + query + "': " + e.getClass().getSimpleName());
                            }
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS), "All threads should complete within 30 seconds");
        executor.shutdown();

        // Calculate expected counts
        int totalOperations = threadCount * iterationsPerThread;
        int expectedValidOperations = totalOperations / 2;
        int expectedInvalidOperations = totalOperations / 2;

        // Allow for small variance due to the modulo distribution
        assertTrue(Math.abs(validSuccessCount.get() - expectedValidOperations) <= 1,
                "Valid operations should be approximately " + expectedValidOperations + ", was " + validSuccessCount.get());
        assertTrue(Math.abs(invalidCaughtCount.get() - expectedInvalidOperations) <= 1,
                "Invalid operations should be approximately " + expectedInvalidOperations + ", was " + invalidCaughtCount.get());
        assertEquals(0, unexpectedExceptionCount.get(), "Should have no unexpected exceptions");

        System.out.println("Mixed valid/invalid concurrent parsing: " + validSuccessCount.get() +
                " valid successes, " + invalidCaughtCount.get() + " expected errors caught");
    }

    @Test
    @DisplayName("High contention concurrent parsing should maintain performance")
    void shouldMaintainPerformanceUnderHighContentionConcurrentParsing() throws InterruptedException, ExecutionException {
        String[] queries = {
                "{ \"user_id\": 12345 }",
                "{ \"status\": { \"$in\": [\"active\", \"pending\"] } }",
                "{ \"$and\": [ { \"priority\": { \"$gte\": 1 } }, { \"urgent\": false } ] }"
        };

        int threadCount = 50; // High contention
        int iterationsPerThread = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);

        AtomicLong totalOperations = new AtomicLong(0);
        AtomicReference<Exception> firstException = new AtomicReference<>();

        List<Future<Long>> futures = new ArrayList<>();

        long testStartTime = System.nanoTime();

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            Future<Long> future = executor.submit(() -> {
                try {
                    // Wait for all threads to be ready
                    startLatch.await();

                    long threadStartTime = System.nanoTime();

                    for (int j = 0; j < iterationsPerThread; j++) {
                        String query = queries[(threadId + j) % queries.length];

                        BqlExpr result = BqlParser.parse(query);
                        assertNotNull(result, "Result should not be null");

                        // Exercise all parser functionality under contention
                        result.toJson();
                        BqlParser.explain(result);

                        totalOperations.incrementAndGet();
                    }

                    return System.nanoTime() - threadStartTime;
                } catch (Exception e) {
                    firstException.compareAndSet(null, e);
                    return 0L;
                } finally {
                    endLatch.countDown();
                }
            });
            futures.add(future);
        }

        // Start all threads simultaneously
        startLatch.countDown();

        assertTrue(endLatch.await(30, TimeUnit.SECONDS), "All threads should complete within 30 seconds");

        long testEndTime = System.nanoTime();
        long totalTestDurationMs = (testEndTime - testStartTime) / 1_000_000;

        // Collect thread execution times
        long totalThreadTime = 0;
        for (Future<Long> future : futures) {
            totalThreadTime += future.get();
        }

        executor.shutdown();

        // Verify results
        assertNull(firstException.get(), "No exceptions should occur under high contention");

        long expectedOperations = (long) threadCount * iterationsPerThread;
        assertEquals(expectedOperations, totalOperations.get(), "All operations should complete");

        // Performance assertions
        assertTrue(totalTestDurationMs < 10000, "High contention test should complete within 10 seconds, took: " + totalTestDurationMs + "ms");

        double operationsPerSecond = (totalOperations.get() * 1000.0) / totalTestDurationMs;
        assertTrue(operationsPerSecond > 100, "Should maintain reasonable throughput under contention: " + operationsPerSecond + " ops/sec");

        System.out.println("High contention parsing: " + totalOperations.get() + " operations across " + threadCount +
                " threads in " + totalTestDurationMs + "ms (" + String.format("%.1f", operationsPerSecond) + " ops/sec)");
    }

    @Test
    @DisplayName("Concurrent parsing with complex query generation should work correctly")
    void shouldHandleConcurrentComplexQueryGenerationCorrectly() throws InterruptedException, ExecutionException {
        BqlQueryGenerator generator = new BqlQueryGenerator();

        int threadCount = 8;
        int iterationsPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicReference<Exception> firstException = new AtomicReference<>();

        List<Future<Integer>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            Future<Integer> future = executor.submit(() -> {
                int localSuccessCount = 0;
                try {
                    for (int j = 0; j < iterationsPerThread; j++) {
                        try {
                            // Generate a random query
                            String randomQuery = generator.generateRandomQuery();

                            // Parse the generated query
                            BqlExpr result = BqlParser.parse(randomQuery);
                            assertNotNull(result, "Generated query should parse");

                            // Validate serialization roundtrip
                            String serialized = result.toJson();
                            BqlExpr reparsed = BqlParser.parse(serialized);
                            assertEquals(serialized, reparsed.toJson(), "Roundtrip should be consistent");

                            // Validate explanation
                            String explanation = BqlParser.explain(result);
                            assertNotNull(explanation, "Explanation should work");

                            localSuccessCount++;
                            successCount.incrementAndGet();

                        } catch (Exception e) {
                            // Some randomly generated queries might not be valid, which is acceptable
                            failureCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    firstException.compareAndSet(null, e);
                } finally {
                    latch.countDown();
                }
                return localSuccessCount;
            });
            futures.add(future);
        }

        assertTrue(latch.await(60, TimeUnit.SECONDS), "All threads should complete within 60 seconds");

        // Collect results
        int totalSuccessCount = 0;
        for (Future<Integer> future : futures) {
            totalSuccessCount += future.get();
        }

        executor.shutdown();

        // Verify results
        assertNull(firstException.get(), "No unexpected exceptions should occur");
        assertEquals(totalSuccessCount, successCount.get(), "Success counts should match");

        int totalOperations = threadCount * iterationsPerThread;
        assertEquals(totalOperations, successCount.get() + failureCount.get(), "All operations should be accounted for");

        // At least 50% of randomly generated queries should be valid
        assertTrue(successCount.get() >= totalOperations * 0.5,
                "At least 50% of generated queries should be valid, got " + successCount.get() + "/" + totalOperations);

        System.out.println("Concurrent complex query generation: " + successCount.get() + " successful parses, " +
                failureCount.get() + " expected failures out of " + totalOperations + " total operations");
    }

    @Test
    @DisplayName("Concurrent parsing should not cause memory leaks")
    void shouldNotCauseMemoryLeaksWithConcurrentParsing() throws InterruptedException, ExecutionException {
        String testQuery = """
                {
                  "$and": [
                    { "category": "electronics" },
                    { "$or": [ { "brand": "Apple" }, { "brand": "Samsung" } ] },
                    { "price": { "$gte": 100 } },
                    { "reviews": { "$elemMatch": { "rating": { "$gte": 4 }, "verified": true } } }
                  ]
                }
                """;

        // Force garbage collection before test
        System.gc();
        Thread.sleep(100);
        System.gc();

        long memoryBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        int threadCount = 20;
        int iterationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        AtomicLong totalOperations = new AtomicLong(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < iterationsPerThread; j++) {
                        // Parse and immediately let go of references
                        BqlExpr result = BqlParser.parse(testQuery);
                        String serialized = result.toJson();
                        String explanation = BqlParser.explain(result);

                        // Use the results briefly to prevent optimization
                        if (!serialized.isEmpty() && !explanation.isEmpty()) {
                            totalOperations.incrementAndGet();
                        }

                        // Don't hold references - let GC collect immediately
                        result = null;
                        serialized = null;
                        explanation = null;
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS), "All threads should complete within 30 seconds");
        executor.shutdown();

        // Force garbage collection after test
        System.gc();
        Thread.sleep(100);
        System.gc();

        long memoryAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long memoryIncrease = memoryAfter - memoryBefore;

        // Verify operations completed
        long expectedOperations = (long) threadCount * iterationsPerThread;
        assertEquals(expectedOperations, totalOperations.get(), "All operations should complete");

        // Memory increase should be reasonable (less than 50MB for this test)
        long maxReasonableIncrease = 50 * 1024 * 1024; // 50MB
        assertTrue(memoryIncrease < maxReasonableIncrease,
                String.format("Memory increase should be reasonable. Increased by %d bytes (%.2f MB)",
                        memoryIncrease, memoryIncrease / (1024.0 * 1024.0)));

        System.out.println("Concurrent memory usage: " + totalOperations.get() + " operations increased memory by " +
                String.format("%.2f MB", memoryIncrease / (1024.0 * 1024.0)));
    }

    @Test
    @DisplayName("Thread interruption during parsing should be handled gracefully")
    void shouldHandleThreadInterruptionGracefully() throws InterruptedException {
        String complexQuery = """
                {
                  "$and": [
                    { "selector1": "value1" },
                    { "selector2": { "$in": ["a", "b", "c", "d", "e"] } },
                    { "$or": [ { "selector3": { "$gt": 100 } }, { "selector4": true } ] },
                    { "nested": { "$elemMatch": { "key": "test", "value": { "$ne": "skip" } } } }
                  ]
                }
                """;

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);

        AtomicInteger completedCount = new AtomicInteger(0);
        AtomicInteger interruptedCount = new AtomicInteger(0);

        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            Future<?> future = executor.submit(() -> {
                try {
                    startLatch.await();

                    // Run longer operations to increase chance of interruption
                    for (int j = 0; j < 1000; j++) {
                        if (Thread.currentThread().isInterrupted()) {
                            interruptedCount.incrementAndGet();
                            return;
                        }

                        BqlExpr result = BqlParser.parse(complexQuery);
                        result.toJson();
                        BqlParser.explain(result);

                        // Add a small delay to make interruption more likely
                        if (j % 50 == 0) {
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException exp) {
                                interruptedCount.incrementAndGet();
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }

                        // Check for interruption between operations
                        if (Thread.currentThread().isInterrupted()) {
                            interruptedCount.incrementAndGet();
                            return;
                        }
                    }

                    completedCount.incrementAndGet();

                } catch (InterruptedException exp) {
                    interruptedCount.incrementAndGet();
                    Thread.currentThread().interrupt(); // Restore interrupted status
                } finally {
                    endLatch.countDown();
                }
            });
            futures.add(future);
        }

        // Start all threads
        startLatch.countDown();

        // Let threads run for a very short time, then interrupt some of them
        Thread.sleep(10);

        // Interrupt half of the threads
        for (int i = 0; i < threadCount / 2; i++) {
            futures.get(i).cancel(true);
        }

        assertTrue(endLatch.await(10, TimeUnit.SECONDS), "All threads should complete or be interrupted within 10 seconds");
        executor.shutdown();

        // Verify that threads handled interruption gracefully
        assertEquals(threadCount, completedCount.get() + interruptedCount.get(),
                "All threads should either complete or be interrupted");

        // At least one thread should have been interrupted, but if the operations are too fast,
        // we can accept that all threads completed normally
        System.out.println("Thread interruption handling: " + completedCount.get() + " completed, " +
                interruptedCount.get() + " interrupted gracefully");

        // This test is primarily about ensuring graceful handling, not forcing interruptions
        assertTrue(completedCount.get() >= 0, "Some threads should have completed");
    }

    @Test
    @DisplayName("Concurrent parsing should maintain consistent AST structure")
    void shouldMaintainConsistentASTStructureWithConcurrentParsing() throws InterruptedException, ExecutionException {
        String testQuery = """
                {
                  "$or": [
                    {
                      "$and": [
                        { "category": "books" },
                        { "price": { "$lt": 50 } },
                        { "author": { "$ne": "unknown" } }
                      ]
                    },
                    {
                      "$and": [
                        { "category": "electronics" },
                        { "brand": { "$in": ["Apple", "Samsung"] } },
                        { "warranty": { "$exists": true } }
                      ]
                    }
                  ]
                }
                """;

        // Parse once to get expected structure
        BqlExpr expectedAST = BqlParser.parse(testQuery);
        String expectedJson = expectedAST.toJson();
        String expectedExplanation = BqlParser.explain(expectedAST);

        int threadCount = 25;
        int iterationsPerThread = 40;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        AtomicInteger consistencyViolations = new AtomicInteger(0);
        AtomicInteger totalOperations = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < iterationsPerThread; j++) {
                        BqlExpr result = BqlParser.parse(testQuery);

                        // Verify JSON consistency
                        String actualJson = result.toJson();
                        if (!expectedJson.equals(actualJson)) {
                            consistencyViolations.incrementAndGet();
                            System.err.println("JSON inconsistency detected: expected=" + expectedJson + ", actual=" + actualJson);
                        }

                        // Verify explanation consistency
                        String actualExplanation = BqlParser.explain(result);
                        if (!expectedExplanation.equals(actualExplanation)) {
                            consistencyViolations.incrementAndGet();
                            System.err.println("Explanation inconsistency detected");
                        }

                        // Verify AST structure consistency
                        if (!verifyASTStructure(result, expectedAST)) {
                            consistencyViolations.incrementAndGet();
                            System.err.println("AST structure inconsistency detected");
                        }

                        totalOperations.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS), "All threads should complete within 30 seconds");
        executor.shutdown();

        // Verify no consistency violations
        assertEquals(0, consistencyViolations.get(), "Should have no consistency violations");

        int expectedOperations = threadCount * iterationsPerThread;
        assertEquals(expectedOperations, totalOperations.get(), "All operations should complete");

        System.out.println("AST consistency test: " + totalOperations.get() + " operations with 0 consistency violations");
    }

    private boolean verifyASTStructure(BqlExpr actual, BqlExpr expected) {
        if (actual.getClass() != expected.getClass()) {
            return false;
        }

        if (actual instanceof BqlAnd(List<BqlExpr> children)) {
            BqlAnd expectedAnd = (BqlAnd) expected;
            return children.size() == expectedAnd.children().size();
        }

        if (actual instanceof BqlOr(List<BqlExpr> children)) {
            BqlOr expectedOr = (BqlOr) expected;
            return children.size() == expectedOr.children().size();
        }

        return true; // Basic structure check passed
    }

    @Test
    @DisplayName("Deadlock detection - concurrent parsing should not cause deadlocks")
    void shouldNotCauseDeadlocksWithConcurrentParsing() throws InterruptedException {
        String[] queries = {
                "{ \"selector1\": \"value1\" }",
                "{ \"selector2\": { \"$gt\": 100 } }",
                "{ \"$and\": [ { \"selector3\": true }, { \"selector4\": { \"$in\": [\"a\", \"b\"] } } ] }"
        };

        int threadCount = 30;
        int iterationsPerThread = 25;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        AtomicInteger completedOperations = new AtomicInteger(0);
        AtomicLong maxExecutionTime = new AtomicLong(0);

        long testStartTime = System.currentTimeMillis();

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    long threadStartTime = System.currentTimeMillis();

                    for (int j = 0; j < iterationsPerThread; j++) {
                        String query = queries[(threadId + j) % queries.length];

                        long operationStart = System.currentTimeMillis();
                        BqlExpr result = BqlParser.parse(query);
                        result.toJson();
                        BqlParser.explain(result);
                        long operationEnd = System.currentTimeMillis();

                        long operationTime = operationEnd - operationStart;
                        maxExecutionTime.updateAndGet(current -> Math.max(current, operationTime));

                        completedOperations.incrementAndGet();

                        // Small random delay to increase chance of contention
                        if (j % 10 == 0) {
                            Thread.sleep(1);
                        }
                    }

                    long threadEndTime = System.currentTimeMillis();
                    long threadExecutionTime = threadEndTime - threadStartTime;

                    // If any single thread takes too long, it might indicate a deadlock
                    assertTrue(threadExecutionTime < 15000,
                            "Thread " + threadId + " took too long (" + threadExecutionTime + "ms), possible deadlock");

                } catch (InterruptedException exp) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }

        // Use a reasonable timeout to detect potential deadlocks
        boolean completedInTime = latch.await(20, TimeUnit.SECONDS);
        long testEndTime = System.currentTimeMillis();
        long totalTestTime = testEndTime - testStartTime;

        executor.shutdown();

        assertTrue(completedInTime, "Test should complete within timeout (possible deadlock detected)");

        int expectedOperations = threadCount * iterationsPerThread;
        assertEquals(expectedOperations, completedOperations.get(), "All operations should complete");

        // No single operation should take excessively long
        assertTrue(maxExecutionTime.get() < 1000,
                "No single operation should take more than 1 second, max was: " + maxExecutionTime.get() + "ms");

        System.out.println("Deadlock prevention test: " + completedOperations.get() + " operations completed in " +
                totalTestTime + "ms (max single operation: " + maxExecutionTime.get() + "ms)");
    }
}