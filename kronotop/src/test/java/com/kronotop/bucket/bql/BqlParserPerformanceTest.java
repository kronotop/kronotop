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

import com.kronotop.bucket.bql.ast.BqlExpr;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance and load tests for BQL parser to validate scalability and performance characteristics.
 * These tests ensure the parser can handle high-volume operations and complex queries efficiently.
 */
class BqlParserPerformanceTest {

    @Test
    @DisplayName("Simple query parsing performance should be fast")
    @Disabled
    void testSimpleQueryParsingPerformance() {
        String simpleQuery = "{ \"status\": \"active\" }";
        int iterations = 10000;

        // Warmup
        for (int i = 0; i < 1000; i++) {
            BqlParser.parse(simpleQuery);
        }

        long startTime = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            BqlExpr result = BqlParser.parse(simpleQuery);
            assertNotNull(result, "Result should not be null");
        }

        long endTime = System.nanoTime();
        long totalDurationMs = (endTime - startTime) / 1_000_000;
        double avgTimePerQueryMs = (double) totalDurationMs / iterations;

        assertTrue(totalDurationMs < 1000, "10K simple queries should complete within 1 second, took: " + totalDurationMs + "ms");
        assertTrue(avgTimePerQueryMs < 0.1, "Average time per simple query should be < 0.1ms, was: " + avgTimePerQueryMs + "ms");

        System.out.println("Simple query performance: " + iterations + " queries in " + totalDurationMs + "ms (avg: " + String.format("%.4f", avgTimePerQueryMs) + "ms per query)");
    }

    @Test
    @DisplayName("Complex query parsing performance should be reasonable")
    @Disabled
    void testComplexQueryParsingPerformance() {
        String complexQuery = """
                {
                  "$and": [
                    { "status": "active" },
                    {
                      "$or": [
                        { "priority": { "$gte": 5 } },
                        { "tags": { "$in": ["urgent", "critical", "high"] } }
                      ]
                    },
                    {
                      "metadata": {
                        "$elemMatch": {
                          "key": "environment",
                          "value": { "$ne": "test" }
                        }
                      }
                    }
                  ]
                }
                """;

        int iterations = 1000;

        // Warmup
        for (int i = 0; i < 100; i++) {
            BqlParser.parse(complexQuery);
        }

        long startTime = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            BqlExpr result = BqlParser.parse(complexQuery);
            assertNotNull(result, "Result should not be null");
        }

        long endTime = System.nanoTime();
        long totalDurationMs = (endTime - startTime) / 1_000_000;
        double avgTimePerQueryMs = (double) totalDurationMs / iterations;

        assertTrue(totalDurationMs < 5000, "1K complex queries should complete within 5 seconds, took: " + totalDurationMs + "ms");
        assertTrue(avgTimePerQueryMs < 5.0, "Average time per complex query should be < 5ms, was: " + avgTimePerQueryMs + "ms");

        System.out.println("Complex query performance: " + iterations + " queries in " + totalDurationMs + "ms (avg: " + String.format("%.4f", avgTimePerQueryMs) + "ms per query)");
    }

    @Test
    @DisplayName("Large query with many conditions should parse efficiently")
    @Disabled
    void testLargeQueryPerformance() {
        // Generate a query with 50 conditions
        StringBuilder largeQueryBuilder = new StringBuilder();
        largeQueryBuilder.append("{ \"$and\": [");

        for (int i = 0; i < 50; i++) {
            if (i > 0) largeQueryBuilder.append(", ");
            largeQueryBuilder.append("{ \"selector").append(i).append("\": \"value").append(i).append("\" }");
        }

        largeQueryBuilder.append("] }");
        String largeQuery = largeQueryBuilder.toString();

        int iterations = 100;

        // Warmup
        for (int i = 0; i < 10; i++) {
            BqlParser.parse(largeQuery);
        }

        long startTime = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            BqlExpr result = BqlParser.parse(largeQuery);
            assertNotNull(result, "Large query should parse successfully");
        }

        long endTime = System.nanoTime();
        long totalDurationMs = (endTime - startTime) / 1_000_000;
        double avgTimePerQueryMs = (double) totalDurationMs / iterations;

        assertTrue(totalDurationMs < 2000, "100 large queries should complete within 2 seconds, took: " + totalDurationMs + "ms");
        assertTrue(avgTimePerQueryMs < 20.0, "Average time per large query should be < 20ms, was: " + avgTimePerQueryMs + "ms");

        System.out.println("Large query performance: " + iterations + " queries with 50 conditions each in " + totalDurationMs + "ms (avg: " + String.format("%.4f", avgTimePerQueryMs) + "ms per query)");
    }

    @Test
    @DisplayName("Serialization performance should be fast")
    @Disabled
    void testSerializationPerformance() {
        String complexQuery = """
                {
                  "$or": [
                    {
                      "$and": [
                        { "category": "electronics" },
                        { "price": { "$lt": 1000 } },
                        { "tags": { "$in": ["mobile", "phone", "smartphone"] } }
                      ]
                    },
                    {
                      "$and": [
                        { "category": "books" },
                        { "author": { "$ne": "unknown" } },
                        { "pages": { "$gte": 100 } }
                      ]
                    }
                  ]
                }
                """;

        BqlExpr parsedQuery = BqlParser.parse(complexQuery);
        int iterations = 5000;

        // Warmup
        for (int i = 0; i < 500; i++) {
            parsedQuery.toJson();
        }

        long startTime = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            String serialized = parsedQuery.toJson();
            assertNotNull(serialized, "Serialized result should not be null");
            assertFalse(serialized.trim().isEmpty(), "Serialized result should not be empty");
        }

        long endTime = System.nanoTime();
        long totalDurationMs = (endTime - startTime) / 1_000_000;
        double avgTimePerSerializationMs = (double) totalDurationMs / iterations;

        assertTrue(totalDurationMs < 1000, "5K serializations should complete within 1 second, took: " + totalDurationMs + "ms");
        assertTrue(avgTimePerSerializationMs < 0.2, "Average serialization time should be < 0.2ms, was: " + avgTimePerSerializationMs + "ms");

        System.out.println("Serialization performance: " + iterations + " serializations in " + totalDurationMs + "ms (avg: " + String.format("%.4f", avgTimePerSerializationMs) + "ms per serialization)");
    }

    @Test
    @DisplayName("Explanation generation performance should be reasonable")
    @Disabled
    void testExplanationPerformance() {
        String complexQuery = """
                {
                  "$and": [
                    { "tenant_id": "tenant123" },
                    {
                      "$or": [
                        { "role": "admin" },
                        {
                          "permissions": {
                            "$elemMatch": {
                              "resource": "documents",
                              "actions": { "$all": ["read", "write"] }
                            }
                          }
                        }
                      ]
                    }
                  ]
                }
                """;

        BqlExpr parsedQuery = BqlParser.parse(complexQuery);
        int iterations = 2000;

        // Warmup
        for (int i = 0; i < 200; i++) {
            BqlParser.explain(parsedQuery);
        }

        long startTime = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            String explanation = BqlParser.explain(parsedQuery);
            assertNotNull(explanation, "Explanation should not be null");
            assertFalse(explanation.trim().isEmpty(), "Explanation should not be empty");
        }

        long endTime = System.nanoTime();
        long totalDurationMs = (endTime - startTime) / 1_000_000;
        double avgTimePerExplanationMs = (double) totalDurationMs / iterations;

        assertTrue(totalDurationMs < 2000, "2K explanations should complete within 2 seconds, took: " + totalDurationMs + "ms");
        assertTrue(avgTimePerExplanationMs < 1.0, "Average explanation time should be < 1ms, was: " + avgTimePerExplanationMs + "ms");

        System.out.println("Explanation performance: " + iterations + " explanations in " + totalDurationMs + "ms (avg: " + String.format("%.4f", avgTimePerExplanationMs) + "ms per explanation)");
    }

    @Test
    @DisplayName("Memory usage should be reasonable for many queries")
    @Disabled
    void testMemoryUsage() {
        String testQuery = """
                {
                  "$and": [
                    { "status": "active" },
                    { "category": { "$in": ["electronics", "books", "clothing"] } },
                    { "$and": [ { "price": { "$gte": 10 } }, { "price": { "$lte": 1000 } } ] }
                  ]
                }
                """;

        // Force garbage collection before test
        System.gc();
        Thread.yield();
        System.gc();

        long memoryBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        // Parse many queries without holding references (to allow GC)
        int queryCount = 10000;
        for (int i = 0; i < queryCount; i++) {
            BqlExpr result = BqlParser.parse(testQuery);
            // Exercise the result briefly then let it be eligible for GC
            result.toJson();
            BqlParser.explain(result);
        }

        // Force garbage collection after test
        System.gc();
        Thread.yield();
        System.gc();

        long memoryAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long memoryIncrease = memoryAfter - memoryBefore;

        // Memory increase should be reasonable (less than 20MB for 10K queries)
        long maxReasonableIncrease = 20 * 1024 * 1024; // 20MB
        assertTrue(memoryIncrease < maxReasonableIncrease,
                String.format("Memory increase should be reasonable. Increased by %d bytes (%.2f MB)",
                        memoryIncrease, memoryIncrease / (1024.0 * 1024.0)));

        System.out.println("Memory usage: " + queryCount + " queries increased memory by " +
                String.format("%.2f MB", memoryIncrease / (1024.0 * 1024.0)));
    }

    @Test
    @DisplayName("Concurrent parsing should work correctly and efficiently")
    @Disabled
    void testConcurrentParsing() throws InterruptedException, ExecutionException {
        String[] testQueries = {
                "{ \"status\": \"active\" }",
                "{ \"$and\": [ { \"category\": \"electronics\" }, { \"price\": { \"$lt\": 1000 } } ] }",
                "{ \"$or\": [ { \"priority\": 1 }, { \"urgent\": true } ] }",
                "{ \"tags\": { \"$in\": [\"important\", \"critical\"] } }",
                "{ \"metadata\": { \"$elemMatch\": { \"key\": \"env\", \"value\": \"prod\" } } }"
        };

        int threadCount = 10;
        int iterationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        List<Future<Long>> futures = new ArrayList<>();

        long testStartTime = System.nanoTime();

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            Future<Long> future = executor.submit(() -> {
                long threadStartTime = System.nanoTime();
                try {
                    for (int j = 0; j < iterationsPerThread; j++) {
                        String query = testQueries[j % testQueries.length];
                        BqlExpr result = BqlParser.parse(query);
                        assertNotNull(result, "Result should not be null");

                        // Exercise the result
                        result.toJson();
                        BqlParser.explain(result);
                    }
                } finally {
                    latch.countDown();
                }
                return System.nanoTime() - threadStartTime;
            });
            futures.add(future);
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS), "All threads should complete within 30 seconds");

        long testEndTime = System.nanoTime();
        long totalTestDurationMs = (testEndTime - testStartTime) / 1_000_000;

        // Verify all futures completed successfully
        long totalThreadTime = 0;
        for (Future<Long> future : futures) {
            long threadDuration = future.get();
            totalThreadTime += threadDuration;
        }

        long avgThreadDurationMs = (totalThreadTime / threadCount) / 1_000_000;
        int totalOperations = threadCount * iterationsPerThread;

        assertTrue(totalTestDurationMs < 10000, "Concurrent test should complete within 10 seconds, took: " + totalTestDurationMs + "ms");
        assertTrue(avgThreadDurationMs < 5000, "Average thread duration should be reasonable, was: " + avgThreadDurationMs + "ms");

        executor.shutdown();

        System.out.println("Concurrent parsing: " + totalOperations + " operations across " + threadCount +
                " threads completed in " + totalTestDurationMs + "ms (avg thread time: " + avgThreadDurationMs + "ms)");
    }

    @Test
    @DisplayName("Stress test with mixed operations should handle high load")
    @Disabled
    void testStressTestMixedOperations() {
        BqlQueryGenerator generator = new BqlQueryGenerator();
        int queryCount = 5000;
        long operationCount = 0;

        long startTime = System.nanoTime();

        for (int i = 0; i < queryCount; i++) {
            try {
                // Generate and parse a random query
                String randomQuery = generator.generateRandomQuery();
                BqlExpr result = BqlParser.parse(randomQuery);
                operationCount++;

                // Serialize the result
                String serialized = result.toJson();
                operationCount++;

                // Generate explanation
                String explanation = BqlParser.explain(result);
                operationCount++;

                // Verify roundtrip parsing
                BqlExpr reparsed = BqlParser.parse(serialized);
                operationCount++;

                // Basic validation
                assertNotNull(result, "Original result should not be null");
                assertNotNull(serialized, "Serialized result should not be null");
                assertNotNull(explanation, "Explanation should not be null");
                assertNotNull(reparsed, "Reparsed result should not be null");

            } catch (Exception e) {
                // In stress test, log failures but continue
                System.err.println("Stress test failure on iteration " + i + ": " + e.getMessage());
            }
        }

        long endTime = System.nanoTime();
        long totalDurationMs = (endTime - startTime) / 1_000_000;
        double operationsPerSecond = (operationCount * 1000.0) / totalDurationMs;

        assertTrue(totalDurationMs < 30000, "Stress test should complete within 30 seconds, took: " + totalDurationMs + "ms");
        assertTrue(operationsPerSecond > 100, "Should handle at least 100 operations per second, achieved: " + operationsPerSecond);

        System.out.println("Stress test: " + operationCount + " operations in " + totalDurationMs + "ms (" +
                String.format("%.1f", operationsPerSecond) + " ops/sec)");
    }

    @Test
    @DisplayName("Parser should handle repeated parsing of same query efficiently")
    @Disabled
    void testRepeatedQueryCaching() {
        String repeatedQuery = """
                {
                  "$and": [
                    { "user_id": "user123" },
                    { "status": "active" },
                    {
                      "$or": [
                        { "role": "admin" },
                        { "permissions": { "$in": ["read", "write"] } }
                      ]
                    }
                  ]
                }
                """;

        int iterations = 3000;

        // First run - includes any initialization overhead
        long firstRunStart = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            BqlExpr result = BqlParser.parse(repeatedQuery);
            assertNotNull(result, "Result should not be null");
        }
        long firstRunEnd = System.nanoTime();
        long firstRunDurationMs = (firstRunEnd - firstRunStart) / 1_000_000;

        // Second run - should be consistent (no caching expected, but no init overhead)
        long secondRunStart = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            BqlExpr result = BqlParser.parse(repeatedQuery);
            assertNotNull(result, "Result should not be null");
        }
        long secondRunEnd = System.nanoTime();
        long secondRunDurationMs = (secondRunEnd - secondRunStart) / 1_000_000;

        // Performance should be consistent (within 100% variance due to JVM timing variations)
        double variance = Math.abs(firstRunDurationMs - secondRunDurationMs) / (double) Math.min(firstRunDurationMs, secondRunDurationMs);
        assertTrue(variance < 1.0, "Performance should be consistent between runs, variance was: " + (variance * 100) + "%");

        assertTrue(firstRunDurationMs < 3000, "First run should complete within 3 seconds, took: " + firstRunDurationMs + "ms");
        assertTrue(secondRunDurationMs < 3000, "Second run should complete within 3 seconds, took: " + secondRunDurationMs + "ms");

        System.out.println("Repeated query performance: First run: " + firstRunDurationMs + "ms, Second run: " +
                secondRunDurationMs + "ms (variance: " + String.format("%.1f", variance * 100) + "%)");
    }

    @Test
    @DisplayName("Throughput test should achieve reasonable queries per second")
    @Disabled
    void testThroughputBenchmark() {
        String[] benchmarkQueries = {
                "{ \"id\": 12345 }",
                "{ \"status\": { \"$in\": [\"active\", \"pending\"] } }",
                "{ \"$and\": [ { \"category\": \"electronics\" }, { \"price\": { \"$lt\": 500 } } ] }",
                "{ \"tags\": { \"$elemMatch\": { \"name\": \"priority\", \"value\": \"high\" } } }",
                "{ \"$or\": [ { \"user_type\": \"premium\" }, { \"trial_active\": true } ] }"
        };

        int testDurationSeconds = 5;
        long endTime = System.nanoTime() + (testDurationSeconds * 1_000_000_000L);
        int queryCount = 0;

        // Warmup
        for (int i = 0; i < 1000; i++) {
            BqlParser.parse(benchmarkQueries[i % benchmarkQueries.length]);
        }

        long startTime = System.nanoTime();

        while (System.nanoTime() < endTime) {
            String query = benchmarkQueries[queryCount % benchmarkQueries.length];
            BqlExpr result = BqlParser.parse(query);
            assertNotNull(result, "Result should not be null");
            queryCount++;
        }

        long actualDurationMs = (System.nanoTime() - startTime) / 1_000_000;
        double queriesPerSecond = (queryCount * 1000.0) / actualDurationMs;

        assertTrue(queriesPerSecond > 1000, "Should achieve at least 1000 queries/sec, achieved: " + queriesPerSecond);

        System.out.println("Throughput benchmark: " + queryCount + " queries in " + actualDurationMs + "ms (" +
                String.format("%.1f", queriesPerSecond) + " queries/sec)");
    }
}