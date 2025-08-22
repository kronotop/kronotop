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

package com.kronotop.bucket.executor;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.SortOrder;
import org.bson.BsonBinaryReader;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance benchmark tests for cursor coordination in mixed queries.
 * This class measures and validates performance characteristics of the
 * node-based cursor coordination system.
 */
class CursorCoordinationBenchmarkTest extends BasePlanExecutorTest {

    @Test
    void benchmarkMixedVsPureIndexedQueries() {
        final String TEST_BUCKET_NAME = "test-benchmark-mixed-vs-pure";

        // Create indexes for benchmark
        IndexDefinition categoryIndex = IndexDefinition.create("category-index", "category", BsonType.STRING, SortOrder.ASCENDING);
        IndexDefinition statusIndex = IndexDefinition.create("status-index", "status", BsonType.STRING, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, categoryIndex, statusIndex);

        // Insert benchmark data
        List<byte[]> documents = new ArrayList<>();
        String[] categories = {"electronics", "books", "clothing", "tools", "sports"};
        String[] statuses = {"active", "pending", "inactive", "archived"};
        String[] descriptions = {"premium product", "standard quality", "basic item"};

        for (int i = 0; i < 100; i++) { // Moderate size for compilation testing
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format(
                "{'category': '%s', 'status': '%s', 'price': %d, 'description': '%s', 'counter': %d}",
                categories[i % categories.length],
                statuses[i % statuses.length],
                (int)(Math.random() * 200) + 10,
                descriptions[i % descriptions.length],
                i
            )));
        }

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Pure indexed query (baseline)
        String pureIndexedQuery = "{ $or: [ { category: \"electronics\" }, { status: \"active\" } ] }";
        
        // Mixed query (with cursor coordination)
        String mixedQuery = "{ $or: [ { category: \"electronics\" }, { price: { $gte: 100 } } ] }";

        // Benchmark pure indexed query
        BenchmarkResult pureResult = benchmarkQuery(metadata, pureIndexedQuery, "Pure Indexed OR");
        
        // Benchmark mixed query  
        BenchmarkResult mixedResult = benchmarkQuery(metadata, mixedQuery, "Mixed OR with Coordination");
        
        System.out.println("\n🏆 Mixed vs Pure Indexed Query Benchmark:");
        System.out.println("Pure Indexed - " + pureResult);
        System.out.println("Mixed Query  - " + mixedResult);
        
        double performanceRatio = (double) mixedResult.avgDuration / pureResult.avgDuration;
        System.out.println("Performance ratio (Mixed/Pure): " + String.format("%.2f", performanceRatio));
        
        // Mixed queries should be at most 5x slower than pure indexed for small datasets
        assertTrue(performanceRatio < 5.0,
            "Mixed query performance should be reasonable compared to pure indexed queries");
        
        // Both should find substantial results
        assertTrue(pureResult.avgResults >= 5, "Pure indexed should find substantial results");
        assertTrue(mixedResult.avgResults >= 5, "Mixed query should find substantial results");
    }

    @Test
    void benchmarkCursorCoordinationOverhead() {
        final String TEST_BUCKET_NAME = "test-cursor-coordination-overhead";

        // Create indexes
        IndexDefinition categoryIndex = IndexDefinition.create("category-index", "category", BsonType.STRING, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, categoryIndex);

        // Insert test data
        List<byte[]> documents = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format(
                "{'category': '%s', 'description': '%s', 'counter': %d}",
                i % 2 == 0 ? "books" : "other",
                i % 3 == 0 ? "premium content" : "standard content",
                i
            )));
        }

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        String coordinatedQuery = "{ $or: [ { category: \"books\" }, { description: \"premium content\" } ] }";
        
        long startTime = System.currentTimeMillis();
        BenchmarkResult result = benchmarkQuery(metadata, coordinatedQuery, "Coordinated Query");
        long totalDuration = System.currentTimeMillis() - startTime;
        
        System.out.println("\n⚡ Cursor Coordination Overhead Benchmark:");
        System.out.println("Average duration: " + String.format("%.2f", result.avgDuration) + "ms");
        System.out.println("Total duration: " + totalDuration + "ms");
        System.out.println("Results found: " + (int)result.avgResults);
        
        // Coordination overhead should be reasonable for small datasets
        assertTrue(result.avgDuration < 1000, "Average coordination overhead should be under 1 second for small datasets");
        assertTrue(result.avgResults > 0, "Should find some results");
    }

    private BenchmarkResult benchmarkQuery(BucketMetadata metadata, String query, String description) {
        final int BENCHMARK_RUNS = 3; // Reduced for compilation testing
        
        List<Long> durations = new ArrayList<>();
        List<Integer> resultCounts = new ArrayList<>();
        
        for (int run = 0; run < BENCHMARK_RUNS; run++) {
            long startTime = System.nanoTime();
            
            PlanExecutor executor = createPlanExecutorForQuery(metadata, query, 50);
            
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Map<?, ByteBuffer> results = executor.execute(tr);
                
                long duration = System.nanoTime() - startTime;
                durations.add(duration / 1_000_000); // Convert to milliseconds
                resultCounts.add(results.size());
            }
        }
        
        double avgDuration = durations.stream().mapToLong(Long::longValue).average().orElse(0);
        double avgResults = resultCounts.stream().mapToInt(Integer::intValue).average().orElse(0);
        
        return new BenchmarkResult(description, avgDuration, avgResults, durations.size());
    }

    private static class BenchmarkResult {
        final String description;
        final double avgDuration;
        final double avgResults;
        final int runs;
        
        BenchmarkResult(String description, double avgDuration, double avgResults, int runs) {
            this.description = description;
            this.avgDuration = avgDuration;
            this.avgResults = avgResults;
            this.runs = runs;
        }
        
        @Override
        public String toString() {
            return String.format("%s: %.2fms avg, %.1f results avg (%d runs)", 
                description, avgDuration, avgResults, runs);
        }
    }
}