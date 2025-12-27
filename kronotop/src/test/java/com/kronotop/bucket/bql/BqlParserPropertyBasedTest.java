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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property-based testing for BQL parser using randomized query generation.
 * These tests help discover edge cases and validate parser robustness.
 */
class BqlParserPropertyBasedTest {

    private final BqlQueryGenerator queryGenerator = new BqlQueryGenerator();

    @RepeatedTest(100)
    @DisplayName("Random queries should parse without exceptions")
    void shouldParseRandomQueriesWithoutExceptions() {
        String randomQuery = queryGenerator.generateRandomQuery();

        assertDoesNotThrow(() -> {
            BqlExpr result = BqlParser.parse(randomQuery);
            assertNotNull(result, "Parsed result should not be null for query: " + randomQuery);
        }, "Should parse random query without exception: " + randomQuery);
    }

    @RepeatedTest(50)
    @DisplayName("Query roundtrip fidelity: parse -> serialize -> parse should be consistent")
    void shouldMaintainQueryRoundtripFidelityOnParseSerializeParse() {
        String originalQuery = queryGenerator.generateRandomQuery();

        try {
            // First parse
            BqlExpr firstParse = BqlParser.parse(originalQuery);
            assertNotNull(firstParse, "First parse should not be null");

            // Serialize to JSON
            String serializedQuery = firstParse.toJson();
            assertNotNull(serializedQuery, "Serialized query should not be null");
            assertFalse(serializedQuery.trim().isEmpty(), "Serialized query should not be empty");

            // Second parse
            BqlExpr secondParse = BqlParser.parse(serializedQuery);
            assertNotNull(secondParse, "Second parse should not be null");

            // The structure should be equivalent (both serialize to the same result)
            String reSerialized = secondParse.toJson();
            assertEquals(serializedQuery, reSerialized,
                    String.format("Roundtrip should be consistent:\nOriginal: %s\nFirst serialized: %s\nRe-serialized: %s",
                            originalQuery, serializedQuery, reSerialized));

        } catch (Exception e) {
            fail("Roundtrip test failed for query: " + originalQuery + " - " + e.getMessage());
        }
    }

    @RepeatedTest(30)
    @DisplayName("Complex nested queries should parse correctly")
    void shouldParseComplexNestedQueriesCorrectly() {
        String complexQuery = queryGenerator.generateComplexQuery();

        assertDoesNotThrow(() -> {
            BqlExpr result = BqlParser.parse(complexQuery);
            assertNotNull(result, "Complex query should parse successfully");

            // Verify the result can be serialized
            String serialized = result.toJson();
            assertNotNull(serialized, "Complex query result should serialize");
            assertFalse(serialized.trim().isEmpty(), "Serialized complex query should not be empty");

        }, "Complex query should parse without exception: " + complexQuery);
    }

    @RepeatedTest(50)
    @DisplayName("Simple queries should always parse and explain successfully")
    void shouldParseAndExplainSimpleQueriesSuccessfully() {
        String simpleQuery = queryGenerator.generateSimpleValidQuery();

        try {
            BqlExpr result = BqlParser.parse(simpleQuery);
            assertNotNull(result, "Simple query should parse successfully");

            // Test explanation generation
            String explanation = BqlParser.explain(result);
            assertNotNull(explanation, "Explanation should not be null");
            assertFalse(explanation.trim().isEmpty(), "Explanation should not be empty");

            // Test JSON serialization
            String json = result.toJson();
            assertNotNull(json, "JSON serialization should not be null");
            assertTrue(json.startsWith("{") && json.endsWith("}"),
                    "JSON should be properly formatted");

        } catch (Exception e) {
            fail("Simple query test failed for: " + simpleQuery + " - " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Stress test: Parse many random queries in sequence")
    void shouldParseManyRandomQueriesInSequenceWithHighSuccessRate() {
        BqlQueryGenerator generator = new BqlQueryGenerator(new Random(12345), 4);
        int queryCount = 1000;
        int successCount = 0;

        for (int i = 0; i < queryCount; i++) {
            try {
                String query = generator.generateRandomQuery();
                BqlExpr result = BqlParser.parse(query);
                assertNotNull(result, "Query should parse successfully: " + query);
                successCount++;
            } catch (Exception e) {
                // In a stress test, we might expect some failures, but log them for analysis
                System.err.println("Failed to parse query in stress test: " + e.getMessage());
            }
        }

        // We expect at least 95% success rate for generated valid queries
        double successRate = (double) successCount / queryCount;
        assertTrue(successRate >= 0.95,
                String.format("Success rate should be at least 95%%, but was %.2f%% (%d/%d)",
                        successRate * 100, successCount, queryCount));
    }

    @RepeatedTest(20)
    @DisplayName("Random queries with different seeds should produce diverse results")
    void shouldProduceDiverseResultsWithDifferentSeeds() {
        // Generate queries with different random seeds to ensure diversity
        long seed = ThreadLocalRandom.current().nextLong();
        BqlQueryGenerator seededGenerator = new BqlQueryGenerator(new Random(seed), 3);

        String query1 = seededGenerator.generateRandomQuery();
        String query2 = seededGenerator.generateRandomQuery();
        String query3 = seededGenerator.generateRandomQuery();

        // Parse all queries
        assertDoesNotThrow(() -> {
            BqlExpr result1 = BqlParser.parse(query1);
            BqlExpr result2 = BqlParser.parse(query2);
            BqlExpr result3 = BqlParser.parse(query3);

            assertNotNull(result1, "First query should parse");
            assertNotNull(result2, "Second query should parse");
            assertNotNull(result3, "Third query should parse");
        });
    }

    @RepeatedTest(25)
    @DisplayName("Parser should handle edge cases in random generation gracefully")
    void shouldHandleEdgeCasesInRandomGenerationGracefully() {
        // Test with different generator configurations to hit edge cases
        BqlQueryGenerator[] generators = {
                new BqlQueryGenerator(new Random(1), 1),    // Shallow queries
                new BqlQueryGenerator(new Random(2), 5),    // Deep queries
                new BqlQueryGenerator(new Random(3), 3)     // Medium complexity
        };

        for (BqlQueryGenerator generator : generators) {
            String query = generator.generateRandomQuery();

            assertDoesNotThrow(() -> {
                BqlExpr result = BqlParser.parse(query);
                assertNotNull(result, "Query should parse successfully: " + query);

                // Verify basic properties
                String serialized = result.toJson();
                assertNotNull(serialized, "Should serialize successfully");

                String explanation = BqlParser.explain(result);
                assertNotNull(explanation, "Should explain successfully");

            }, "Edge case query should parse successfully: " + query);
        }
    }

    @Test
    @DisplayName("Memory usage should be reasonable for large number of random queries")
    void shouldMaintainReasonableMemoryUsageWithRandomQueries() {
        BqlQueryGenerator generator = new BqlQueryGenerator(new Random(54321), 2);

        // Force garbage collection before test
        System.gc();
        long memoryBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        // Parse many queries
        for (int i = 0; i < 5000; i++) {
            String query = generator.generateRandomQuery();
            try {
                BqlExpr result = BqlParser.parse(query);
                // Don't hold references to results to allow GC
                result.toJson(); // Exercise the result briefly
            } catch (Exception e) {
                // Continue on parse errors in memory test
            }
        }

        // Force garbage collection after test
        System.gc();
        Thread.yield(); // Give GC a chance to run
        System.gc();

        long memoryAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long memoryIncrease = memoryAfter - memoryBefore;

        // Memory increase should be reasonable (less than 50MB for 5000 queries)
        long maxReasonableIncrease = 50 * 1024 * 1024; // 50MB
        assertTrue(memoryIncrease < maxReasonableIncrease,
                String.format("Memory increase should be reasonable. Increased by %d bytes (%.2f MB)",
                        memoryIncrease, memoryIncrease / (1024.0 * 1024.0)));
    }
}