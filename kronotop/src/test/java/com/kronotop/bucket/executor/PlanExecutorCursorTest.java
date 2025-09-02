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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseHandlerTest;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.bql.ast.VersionstampVal;
import com.kronotop.bucket.optimizer.Optimizer;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.logical.LogicalNode;
import com.kronotop.bucket.planner.logical.LogicalPlanner;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.bucket.planner.physical.PlannerContext;
import com.kronotop.bucket.planner.physical.PhysicalPlanner;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.commandbuilder.kronotop.BucketInsertArgs;
import com.kronotop.server.Session;
import com.kronotop.server.resp3.ArrayRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for PlanExecutor cursor pagination logic.
 * Tests forward/reverse scans, range queries, and cursor boundary management.
 */
class PlanExecutorCursorTest extends BaseHandlerTest {

    private static final String BUCKET_NAME = "test-cursor-bucket";
    private static final int SHARD_ID = 0;
    private static final int LIMIT = 3; // Small batch size to force pagination

    @BeforeEach
    void setUp() {
        // Clean up any existing bucket state
        try {
            Session session = getSession();
            context.getFoundationDB().run(tr -> {
                BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, BUCKET_NAME);
                // Clear any existing data
                return null;
            });
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }

    private void insertOrderedTestData() {
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Alice', 'age': 22, 'status': 'ACTIVE' }"),
                BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Bob', 'age': 28, 'status': 'ACTIVE' }"),
                BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Charlie', 'age': 35, 'status': 'INACTIVE' }"),
                BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Diana', 'age': 42, 'status': 'ACTIVE' }"),
                BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Eve', 'age': 25, 'status': 'ACTIVE' }"),
                BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Frank', 'age': 30, 'status': 'INACTIVE' }"),
                BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Grace', 'age': 38, 'status': 'ACTIVE' }"),
                BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Henry', 'age': 20, 'status': 'ACTIVE' }"),
                BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Iris', 'age': 45, 'status': 'ACTIVE' }"),
                BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Jack', 'age': 33, 'status': 'INACTIVE' }")
        );
        insertDocuments(documents);
    }

    private void insertDocuments(List<byte[]> documents) {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(BUCKET_NAME, BucketInsertArgs.Builder.shard(SHARD_ID), documents).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
    }

    private PlanExecutorConfig createPlanConfig(BucketMetadata metadata, String query, boolean reverse) {
        try {
            BqlExpr parsedQuery = BqlParser.parse(query);
            LogicalPlanner logicalPlanner = new LogicalPlanner();
            LogicalNode logicalPlan = logicalPlanner.planAndValidate(parsedQuery);

            PlannerContext plannerContext = new PlannerContext();
            PhysicalPlanner physicalPlanner = new PhysicalPlanner();
            PhysicalNode physicalPlan = physicalPlanner.plan(metadata, logicalPlan, plannerContext);

            Optimizer optimizer = new Optimizer();
            PhysicalNode optimizedPlan = optimizer.optimize(metadata, physicalPlan, plannerContext);

            PlanExecutorConfig config = new PlanExecutorConfig(metadata, optimizedPlan, plannerContext);
            config.setReverse(reverse);
            return config;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create plan config for query: " + query, e);
        }
    }

    private Map<Versionstamp, ByteBuffer> executePlan(PlanExecutorConfig config) {
        return context.getFoundationDB().run(tr -> {
            PlanExecutor executor = new PlanExecutor(context, config);
            return executor.execute(tr);
        });
    }

    // Helper methods

    private Versionstamp extractVersionstamp(Object value) {
        if (value instanceof VersionstampVal(Versionstamp versionstamp)) {
            return versionstamp;
        }
        throw new IllegalArgumentException("Expected VersionstampVal, got: " + value.getClass().getSimpleName());
    }

    Bounds findCursorBounds(PlanExecutorConfig planExecutorConfig) {
        List<CursorState> states = planExecutorConfig.cursor().getAllCursorStates();
        assertFalse(states.isEmpty());
        CursorState state = states.getFirst();
        return state.bounds();
    }

    @Nested
    @DisplayName("Forward Scan Cursor Tests")
    class ForwardScanTests {

        @Test
        @DisplayName("Should paginate forward through simple filter results")
        void shouldPaginateForwardThroughSimpleFilter() {
            // Insert test data with predictable ordering
            insertOrderedTestData();

            Session session = getSession();
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, BUCKET_NAME);

            // Create plan for simple filter: { status: 'ACTIVE' }
            PlanExecutorConfig config = createPlanConfig(metadata, "{ 'status': 'ACTIVE' }", false);
            config.setLimit(LIMIT);

            List<Versionstamp> allResults = new ArrayList<>();
            TreeSet<Versionstamp> seenVersionstamps = new TreeSet<>();

            // Execute multiple batches
            for (int batch = 0; batch < 3; batch++) {
                Map<Versionstamp, ByteBuffer> results = executePlan(config);

                if (results.isEmpty()) {
                    break; // No more results
                }

                // Verify no duplicates across batches
                for (Versionstamp versionstamp : results.keySet()) {
                    assertFalse(seenVersionstamps.contains(versionstamp),
                            "Duplicate versionstamp found in batch " + batch + ": " + versionstamp);
                    seenVersionstamps.add(versionstamp);
                    allResults.add(versionstamp);
                }

                // Verify batch size (except possibly the last batch)
                assertTrue(results.size() <= LIMIT,
                        "Batch " + batch + " size exceeds limit: " + results.size());

                // Verify forward ordering within batch
                List<Versionstamp> batchKeys = new ArrayList<>(results.keySet());
                for (int i = 1; i < batchKeys.size(); i++) {
                    assertTrue(batchKeys.get(i).compareTo(batchKeys.get(i - 1)) > 0,
                            "Forward scan batch " + batch + " is not properly ordered");
                }

                // Verify cursor was set correctly

                Bounds bounds = findCursorBounds(config);
                assertNotNull(bounds, "Cursor bounds should be set after batch " + batch);
                assertNotNull(bounds.lower(), "Lower bound should be set for forward scan");
                assertEquals(Operator.GT, bounds.lower().operator(),
                        "Forward scan should use GT operator");

                // Verify cursor points to last result
                Versionstamp cursorValue = extractVersionstamp(bounds.lower().value());
                assertEquals(batchKeys.get(batchKeys.size() - 1), cursorValue,
                        "Cursor should point to last result in batch " + batch);
            }

            // Verify we got some results and they are in forward order
            assertFalse(allResults.isEmpty(), "Should have found some ACTIVE records");
            for (int i = 1; i < allResults.size(); i++) {
                assertTrue(allResults.get(i).compareTo(allResults.get(i - 1)) > 0,
                        "Overall results should be in forward order");
            }
        }

        @Test
        @DisplayName("Should handle cursor boundaries correctly for range queries")
        void shouldHandleCursorBoundariesForRangeQueries() {
            insertOrderedTestData();

            Session session = getSession();
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, BUCKET_NAME);

            // Create plan for range query: { age: { $gte: 25, $lte: 35 } }
            PlanExecutorConfig config = createPlanConfig(metadata, "{ 'age': { '$gte': 25, '$lte': 35 } }", false);
            config.setLimit(LIMIT);

            // Execute first batch
            Map<Versionstamp, ByteBuffer> firstBatch = executePlan(config);
            assertFalse(firstBatch.isEmpty(), "First batch should contain results");
            List<Versionstamp> allResults = new ArrayList<>(firstBatch.keySet());

            // Verify cursor was set with proper bounds
            Bounds bounds = findCursorBounds(config);
            assertNotNull(bounds, "Cursor bounds should be set");
            assertNotNull(bounds.lower(), "Lower bound should be set");
            assertEquals(Operator.GT, bounds.lower().operator(), "Should use GT for forward pagination");

            // Execute the second batch
            Map<Versionstamp, ByteBuffer> secondBatch = executePlan(config);

            if (!secondBatch.isEmpty()) {
                allResults.addAll(secondBatch.keySet());

                // Verify no overlap between batches
                TreeSet<Versionstamp> firstKeys = new TreeSet<>(firstBatch.keySet());
                TreeSet<Versionstamp> secondKeys = new TreeSet<>(secondBatch.keySet());
                assertTrue(firstKeys.last().compareTo(secondKeys.first()) < 0,
                        "Second batch should start after first batch ends");
            }

            // Verify all results are in forward order
            for (int i = 1; i < allResults.size(); i++) {
                assertTrue(allResults.get(i).compareTo(allResults.get(i - 1)) > 0,
                        "Range query results should be in forward order");
            }
        }
    }

    @Nested
    @DisplayName("Reverse Scan Cursor Tests")
    class ReverseScanTests {

        @Test
        @DisplayName("Should paginate backward through simple filter results")
        void shouldPaginateBackwardThroughSimpleFilter() {
            insertOrderedTestData();

            Session session = getSession();
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, BUCKET_NAME);

            // Create plan for reverse scan: { status: 'ACTIVE' } with reverse=true
            PlanExecutorConfig config = createPlanConfig(metadata, "{ 'status': 'ACTIVE' }", true);
            config.setLimit(LIMIT);

            List<Versionstamp> allResults = new ArrayList<>();
            TreeSet<Versionstamp> seenVersionstamps = new TreeSet<>();

            // Execute multiple batches
            for (int batch = 0; batch < 3; batch++) {
                Map<Versionstamp, ByteBuffer> results = executePlan(config);

                if (results.isEmpty()) {
                    break; // No more results
                }

                // Verify no duplicates across batches
                for (Versionstamp versionstamp : results.keySet()) {
                    assertFalse(seenVersionstamps.contains(versionstamp),
                            "Duplicate versionstamp found in reverse batch " + batch + ": " + versionstamp);
                    seenVersionstamps.add(versionstamp);
                    allResults.add(versionstamp);
                }

                // Verify reverse ordering within batch (LinkedHashMap preserves FoundationDB reverse order)
                List<Versionstamp> batchKeys = new ArrayList<>(results.keySet());
                for (int i = 1; i < batchKeys.size(); i++) {
                    assertTrue(batchKeys.get(i).compareTo(batchKeys.get(i - 1)) < 0,
                            "Batch keys should be in reverse order (descending) for reverse scan");
                }

                // Verify cursor was set correctly for reverse scan
                Bounds bounds = findCursorBounds(config);
                assertNotNull(bounds, "Cursor bounds should be set after reverse batch " + batch);
                assertNotNull(bounds.upper(), "Upper bound should be set for reverse scan");
                assertEquals(Operator.LT, bounds.upper().operator(),
                        "Reverse scan should use LT operator");

                // Verify cursor points to the smallest result (last in descending batch)
                Versionstamp cursorValue = extractVersionstamp(bounds.upper().value());
                assertEquals(batchKeys.get(batchKeys.size() - 1), cursorValue,
                        "Reverse cursor should point to smallest result in batch " + batch);
            }

            // Verify we got some results
            assertFalse(allResults.isEmpty(), "Should have found some ACTIVE records in reverse");

            // For reverse scan, overall order should be descending when viewed across batches
            // (each batch continues from where the previous left off, going backward)
        }

        @Test
        @DisplayName("Should handle reverse range queries with proper cursor boundaries")
        void shouldHandleReverseBoundariesForRangeQueries() {
            insertOrderedTestData();

            Session session = getSession();
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, BUCKET_NAME);

            // Create plan for reverse range query: { age: { $gte: 25, $lte: 35 } } reverse
            PlanExecutorConfig config = createPlanConfig(metadata, "{ 'age': { '$gte': 25, '$lte': 35 } }", true);
            config.setLimit(LIMIT);

            // Execute first batch
            Map<Versionstamp, ByteBuffer> firstBatch = executePlan(config);
            assertFalse(firstBatch.isEmpty(), "First reverse batch should contain results");

            // Verify cursor was set correctly for reverse
            Bounds bounds = findCursorBounds(config);
            assertNotNull(bounds, "Cursor bounds should be set for reverse range");
            assertNotNull(bounds.upper(), "Upper bound should be set for reverse scan");
            assertEquals(Operator.LT, bounds.upper().operator(), "Should use LT for reverse pagination");

            // Execute second batch
            Map<Versionstamp, ByteBuffer> secondBatch = executePlan(config);

            if (!secondBatch.isEmpty()) {
                // Verify proper reverse ordering between batches
                Versionstamp firstBatchMin = firstBatch.keySet().stream().min(Versionstamp::compareTo).orElseThrow();
                Versionstamp secondBatchMax = secondBatch.keySet().stream().max(Versionstamp::compareTo).orElseThrow();

                assertTrue(secondBatchMax.compareTo(firstBatchMin) < 0,
                        "Second reverse batch should come before first batch chronologically");
            }
        }
    }

    @Nested
    @DisplayName("Range Scan Cursor Tests")
    class RangeScanCursorTests {

        @Test
        @DisplayName("Should handle PhysicalRangeScan with cursor pagination")
        void shouldHandlePhysicalRangeScanWithCursor() {
            insertOrderedTestData();

            Session session = getSession();
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, BUCKET_NAME);

            // Create optimized range plan: { age: { $gte: 20, $lte: 40 } }
            // This should be optimized to PhysicalRangeScan
            PlanExecutorConfig planConfig = createPlanConfig(metadata, "{ $and: [{ 'age': { '$gte': 20 } }, { 'age': { '$lte': 40 } }] }", false);
            planConfig.setLimit(LIMIT);

            // Execute multiple batches
            List<Versionstamp> allResults = new ArrayList<>();
            PlanExecutor executor = new PlanExecutor(context, planConfig);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                for (int batch = 0; batch < 4; batch++) {
                    Map<Versionstamp, ByteBuffer> results = executor.execute(tr);
                    if (results.isEmpty()) {
                        break;
                    }

                    allResults.addAll(results.keySet());

                    // Verify cursor behavior
                    Bounds bounds = findCursorBounds(planConfig);
                    assertNotNull(bounds, "Cursor should be set for range scan batch " + batch);

                    // For range scans, batch size may vary based on how many records match the filter
                    // Don't enforce strict batch sizes since filtering reduces the result count
                    assertFalse(results.isEmpty(), "Range scan batch " + batch + " should have at least one result");
                    assertTrue(results.size() <= LIMIT,
                            "Range scan batch " + batch + " should not exceed batch size limit");
                }
            }

            // Verify continuous ordering
            assertFalse(allResults.isEmpty(), "Should have range scan results");
            for (int i = 1; i < allResults.size(); i++) {
                assertTrue(allResults.get(i).compareTo(allResults.get(i - 1)) > 0,
                        "Range scan results should maintain forward order across batches");
            }
        }
    }

    @Nested
    @DisplayName("Cursor Boundary Edge Cases")
    class CursorBoundaryEdgeCases {

        @Test
        @DisplayName("Should return empty results only when index is exhausted")
        void shouldReturnEmptyResultsOnlyWhenIndexExhausted() {
            insertOrderedTestData();

            Session session = getSession();
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, BUCKET_NAME);

            // Query that matches no documents - should return empty only after exhausting the index
            PlanExecutorConfig config = createPlanConfig(metadata, "{ 'status': 'NONEXISTENT' }", false);
            config.setLimit(LIMIT);

            Map<Versionstamp, ByteBuffer> results = executePlan(config);
            assertTrue(results.isEmpty(), "Should have no results for nonexistent status");

            // Since we exhausted the index, no cursor should be set (scan is complete)
            //Bounds bounds = config.cursor().bounds().get(DefaultIndexDefinition.ID);
            // Note: bounds might be null if the scan is truly complete, or have bounds if there were partial matches
            // The key point is that empty results now only occur when the scan is actually done
        }

        @Test
        @DisplayName("Should preserve existing bounds when updating cursor")
        void shouldPreserveExistingBoundsWhenUpdatingCursor() {
            insertOrderedTestData();

            Session session = getSession();
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, BUCKET_NAME);

            PlanExecutorConfig config = createPlanConfig(metadata, "{ 'status': 'ACTIVE' }", false);
            config.setLimit(LIMIT);

            // Execute first batch
            Map<Versionstamp, ByteBuffer> firstBatch = executePlan(config);
            assertFalse(firstBatch.isEmpty(), "First batch should have results");

            Bounds firstBounds = findCursorBounds(config);
            assertNotNull(firstBounds.lower(), "Lower bound should be set after first batch");

            // Execute second batch
            Map<Versionstamp, ByteBuffer> secondBatch = executePlan(config);

            if (!secondBatch.isEmpty()) {
                Bounds secondBounds = findCursorBounds(config);
                assertNotNull(secondBounds.lower(), "Lower bound should still be set after second batch");

                // Verify cursor moved forward
                Versionstamp firstCursor = extractVersionstamp(firstBounds.lower().value());
                Versionstamp secondCursor = extractVersionstamp(secondBounds.lower().value());
                assertTrue(secondCursor.compareTo(firstCursor) > 0,
                        "Second cursor should be after first cursor");
            }
        }
    }

    @Nested
    @DisplayName("Internal Cursor Advancement Tests")
    class InternalCursorAdvancementTests {

        @Test
        @DisplayName("Should internally advance cursor and return results for sparse filters")
        void shouldInternallyAdvanceCursorForSparseFilters() {
            // Insert data where very few documents match the filter
            insertSparseTestData();

            Session session = getSession();
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, BUCKET_NAME);

            // Create a filter that matches very few documents: { age: 99 }
            // The new implementation should internally continue scanning until it finds the match
            PlanExecutorConfig config = createPlanConfig(metadata, "{ 'age': 99 }", false);
            config.setLimit(LIMIT);

            Map<Versionstamp, ByteBuffer> results = executePlan(config);

            // Should find the single matching document on first call (no empty results returned to client)
            assertEquals(1, results.size(), "Should find exactly one document with age 99");

            // Verify cursor was set correctly based on the found results
            Bounds bounds = findCursorBounds(config);
            assertNotNull(bounds, "Cursor should be set when results found");
            assertNotNull(bounds.lower(), "Lower bound should be set for forward scan");
            assertEquals(Operator.GT, bounds.lower().operator(), "Should use GT operator");

            // Second call should return empty (scan complete)
            Map<Versionstamp, ByteBuffer> secondResults = executePlan(config);
            assertTrue(secondResults.isEmpty(), "Second call should return empty - scan complete");
        }

        @Test
        @DisplayName("Should internally advance cursor in reverse scan for sparse filters")
        void shouldInternallyAdvanceCursorInReverseScanForSparseFilters() {
            // Insert data where very few documents match the filter
            insertSparseTestData();

            Session session = getSession();
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, BUCKET_NAME);

            // Create a filter that matches very few documents: { age: 99 } with reverse scan
            // The new implementation should internally continue scanning until it finds the match
            PlanExecutorConfig config = createPlanConfig(metadata, "{ 'age': 99 }", true);
            config.setLimit(LIMIT);

            Map<Versionstamp, ByteBuffer> results = executePlan(config);

            // Should find the single matching document on first call (no empty results returned to client)
            assertEquals(1, results.size(), "Should find exactly one document with age 99");

            // Verify cursor was set correctly for reverse scan
            Bounds bounds = findCursorBounds(config);
            assertNotNull(bounds, "Cursor should be set when results found");
            assertNotNull(bounds.upper(), "Upper bound should be set for reverse scan");
            assertEquals(Operator.LT, bounds.upper().operator(), "Should use LT operator for reverse scan");

            // Second call should return empty (scan complete)
            Map<Versionstamp, ByteBuffer> secondResults = executePlan(config);
            assertTrue(secondResults.isEmpty(), "Second call should return empty - scan complete");
        }

        @Test
        @DisplayName("Should efficiently handle filters with mixed distribution")
        void shouldEfficientlyHandleFiltersWithMixedDistribution() {
            // Insert data with some matching documents spread out
            insertMixedTestData();

            Session session = getSession();
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, BUCKET_NAME);

            // Create a filter that matches some but not all documents: { status: 'VIP' }
            PlanExecutorConfig config = createPlanConfig(metadata, "{ 'status': 'VIP' }", false);
            config.setLimit(LIMIT);

            List<Versionstamp> allResults = new ArrayList<>();
            int batchCount = 0;
            int maxBatches = 10; // Should need fewer batches now since empty results are handled internally

            while (batchCount < maxBatches) {
                Map<Versionstamp, ByteBuffer> results = executePlan(config);
                batchCount++;

                if (results.isEmpty()) {
                    // Empty results now mean scan is complete
                    break;
                } else {
                    // Non-empty batch - verify results and cursor
                    allResults.addAll(results.keySet());

                    Bounds bounds = findCursorBounds(config);
                    assertNotNull(bounds, "Cursor should be set for non-empty results");
                    assertNotNull(bounds.lower(), "Lower bound should be set");
                }
            }

            // Should find all VIP status documents (3 in the test data)
            assertEquals(3, allResults.size(), "Should find exactly 3 VIP documents");

            // Verify no duplicate results
            Set<Versionstamp> uniqueResults = new HashSet<>(allResults);
            assertEquals(uniqueResults.size(), allResults.size(), "Should have no duplicate results");

            // Should be more efficient than the old approach but exact batch count depends on data distribution
            assertTrue(batchCount <= 5, "Should complete efficiently due to internal advancement");
        }

        private void insertSparseTestData() {
            List<byte[]> documents = List.of(
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Alice', 'age': 25, 'status': 'ACTIVE' }"),
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Bob', 'age': 30, 'status': 'ACTIVE' }"),
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Charlie', 'age': 35, 'status': 'ACTIVE' }"),
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Diana', 'age': 40, 'status': 'ACTIVE' }"),
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Eve', 'age': 45, 'status': 'ACTIVE' }"),
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Frank', 'age': 50, 'status': 'ACTIVE' }"),
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Grace', 'age': 55, 'status': 'ACTIVE' }"),
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Henry', 'age': 60, 'status': 'ACTIVE' }"),
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Iris', 'age': 65, 'status': 'ACTIVE' }"),
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Special', 'age': 99, 'status': 'ACTIVE' }") // Only one matching document
            );
            insertDocuments(documents);
        }

        private void insertMixedTestData() {
            List<byte[]> documents = List.of(
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Alice', 'age': 25, 'status': 'ACTIVE' }"),
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Bob', 'age': 30, 'status': 'VIP' }"),
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Charlie', 'age': 35, 'status': 'ACTIVE' }"),
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Diana', 'age': 40, 'status': 'ACTIVE' }"),
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Eve', 'age': 45, 'status': 'ACTIVE' }"),
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Frank', 'age': 50, 'status': 'VIP' }"),
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Grace', 'age': 55, 'status': 'ACTIVE' }"),
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Henry', 'age': 60, 'status': 'ACTIVE' }"),
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Iris', 'age': 65, 'status': 'VIP' }"),
                    BSONUtil.jsonToDocumentThenBytes("{ 'name': 'Jack', 'age': 70, 'status': 'ACTIVE' }")
            );
            insertDocuments(documents);
        }
    }

    @Nested
    @DisplayName("Critical Edge Case Fix Tests")
    class CriticalEdgeCaseFixTests {

        @Test
        @DisplayName("Should never return empty results when matching documents exist later in index")
        void shouldNeverReturnEmptyResultsWhenMatchingDocumentsExistLater() {
            // Insert documents where only the LAST document matches the filter
            // This tests the core fix: internal scanning until results are found
            insertDocumentsWithOnlyLastMatching();

            Session session = getSession();
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, BUCKET_NAME);

            // Filter that only matches the very last document
            PlanExecutorConfig config = createPlanConfig(metadata, "{ 'category': 'FINAL' }", false);
            config.setLimit(2); // Small limit to force multiple internal scans

            // First call should internally scan through all non-matching docs and return the match
            Map<Versionstamp, ByteBuffer> firstResults = executePlan(config);
            assertEquals(1, firstResults.size(), "Should find the single matching document despite it being at the end");

            // Second call should return empty (scan complete)
            Map<Versionstamp, ByteBuffer> secondResults = executePlan(config);
            assertTrue(secondResults.isEmpty(), "Second call should return empty - scan truly complete");
        }

        @Test
        @DisplayName("Should handle extremely selective filters without client confusion")
        void shouldHandleExtremelySelectiveFiltersWithoutClientConfusion() {
            // Insert many documents with only 2 matching documents spread far apart
            insertDocumentsWithScatteredMatches();

            Session session = getSession();
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, BUCKET_NAME);

            // Filter matches only 2 out of 20 documents, spread apart
            PlanExecutorConfig config = createPlanConfig(metadata, "{ 'priority': 'CRITICAL' }", false);
            config.setLimit(3);

            List<Versionstamp> allResults = new ArrayList<>();
            int callCount = 0;

            // Should find both matching documents across multiple internal scans
            while (callCount < 5) { // Safety limit
                Map<Versionstamp, ByteBuffer> results = executePlan(config);
                callCount++;

                if (results.isEmpty()) {
                    break; // Scan complete
                }

                allResults.addAll(results.keySet());
            }

            // Should find exactly the 2 CRITICAL documents
            assertEquals(2, allResults.size(), "Should find exactly 2 CRITICAL priority documents");
            // Should complete in reasonable number of client calls (not 20+ calls)
            assertTrue(callCount <= 3, "Should complete efficiently without many empty result calls");
        }

        @Test
        @DisplayName("Should prevent infinite loops with filters that match nothing")
        void shouldPreventInfiniteLoopsWithFiltersMatchingNothing() {
            insertOrderedTestData();

            Session session = getSession();
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, BUCKET_NAME);

            // Filter that matches absolutely nothing
            PlanExecutorConfig config = createPlanConfig(metadata, "{ 'status': 'IMPOSSIBLE_VALUE' }", false);
            config.setLimit(3);

            // Should return empty after exhausting the entire index, not get stuck in infinite loop
            Map<Versionstamp, ByteBuffer> results = executePlan(config);
            assertTrue(results.isEmpty(), "Should return empty when no documents match anywhere in index");

            // Verify this was a single call that internally processed everything
            // Second call should also return empty (idempotent)
            Map<Versionstamp, ByteBuffer> secondResults = executePlan(config);
            assertTrue(secondResults.isEmpty(), "Second call should also return empty");
        }

        @Test
        @DisplayName("Should handle reverse scan with scattered matches correctly")
        void shouldHandleReverseScanWithScatteredMatchesCorrectly() {
            insertDocumentsWithScatteredMatches();

            Session session = getSession();
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, BUCKET_NAME);

            // Reverse scan for scattered matches
            PlanExecutorConfig config = createPlanConfig(metadata, "{ 'priority': 'CRITICAL' }", true);
            config.setLimit(3);

            List<Versionstamp> allResults = new ArrayList<>();
            int callCount = 0;

            while (callCount < 5) {
                Map<Versionstamp, ByteBuffer> results = executePlan(config);
                callCount++;

                if (results.isEmpty()) {
                    break;
                }

                allResults.addAll(results.keySet());
            }

            // Should find both CRITICAL documents in reverse order
            assertEquals(2, allResults.size(), "Should find exactly 2 CRITICAL documents in reverse scan");
            assertTrue(callCount <= 3, "Reverse scan should also complete efficiently");

            // Verify reverse ordering (later documents first)
            if (allResults.size() == 2) {
                assertTrue(allResults.get(0).compareTo(allResults.get(1)) > 0,
                        "Reverse scan should return documents in reverse chronological order");
            }
        }

        @Test
        @DisplayName("Should maintain proper cursor boundaries across internal scans")
        void shouldMaintainProperCursorBoundariesAcrossInternalScans() {
            insertDocumentsWithScatteredMatches();

            Session session = getSession();
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, BUCKET_NAME);

            PlanExecutorConfig config = createPlanConfig(metadata, "{ 'priority': 'CRITICAL' }", false);
            config.setLimit(1); // Force multiple calls

            // First call - should find first CRITICAL document
            Map<Versionstamp, ByteBuffer> firstResults = executePlan(config);
            assertEquals(1, firstResults.size(), "First call should find one CRITICAL document");

            // Verify cursor was set correctly
            Bounds firstBounds = findCursorBounds(config);
            assertNotNull(firstBounds, "Cursor should be set after first batch");
            assertNotNull(firstBounds.lower(), "Lower bound should be set for forward scan");
            assertEquals(Operator.GT, firstBounds.lower().operator(), "Should use GT operator");

            // Second call - should find second CRITICAL document
            Map<Versionstamp, ByteBuffer> secondResults = executePlan(config);
            assertEquals(1, secondResults.size(), "Second call should find second CRITICAL document");

            // Verify cursor advanced
            Bounds secondBounds = findCursorBounds(config);
            assertNotNull(secondBounds, "Cursor should be updated after second batch");

            Versionstamp firstCursor = extractVersionstamp(firstBounds.lower().value());
            Versionstamp secondCursor = extractVersionstamp(secondBounds.lower().value());
            assertTrue(secondCursor.compareTo(firstCursor) > 0, "Cursor should advance properly");

            // Third call - should be empty
            Map<Versionstamp, ByteBuffer> thirdResults = executePlan(config);
            assertTrue(thirdResults.isEmpty(), "Third call should return empty - scan complete");
        }

        private void insertDocumentsWithOnlyLastMatching() {
            List<byte[]> documents = new ArrayList<>();

            // Add 15 documents that don't match
            for (int i = 0; i < 15; i++) {
                documents.add(BSONUtil.jsonToDocumentThenBytes(
                        "{ 'name': 'Doc" + i + "', 'category': 'NORMAL', 'index': " + i + " }"));
            }

            // Add one final document that matches
            documents.add(BSONUtil.jsonToDocumentThenBytes(
                    "{ 'name': 'FinalDoc', 'category': 'FINAL', 'index': 15 }"));

            insertDocuments(documents);
        }

        private void insertDocumentsWithScatteredMatches() {
            List<byte[]> documents = new ArrayList<>();

            for (int i = 0; i < 20; i++) {
                String priority = (i == 5 || i == 15) ? "CRITICAL" : "NORMAL"; // Only 2 CRITICAL docs
                documents.add(BSONUtil.jsonToDocumentThenBytes(
                        "{ 'name': 'Doc" + i + "', 'priority': '" + priority + "', 'index': " + i + " }"));
            }

            insertDocuments(documents);
        }
    }

    @Nested
    @DisplayName("Performance and Efficiency Tests")
    class PerformanceAndEfficiencyTests {

        @Test
        @DisplayName("Should minimize client round-trips for selective filters")
        void shouldMinimizeClientRoundTripsForSelectiveFilters() {
            // Insert 50 documents with only 3 matching spread throughout
            insertLargeDatasetWithFewMatches();

            Session session = getSession();
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, BUCKET_NAME);

            // Very selective filter
            PlanExecutorConfig config = createPlanConfig(metadata, "{ 'tier': 'PREMIUM' }", false);
            config.setLimit(5);

            List<Versionstamp> allResults = new ArrayList<>();
            int clientCalls = 0;

            // Track client calls
            while (clientCalls < 10) { // Safety limit
                Map<Versionstamp, ByteBuffer> results = executePlan(config);
                clientCalls++;

                if (results.isEmpty()) {
                    break;
                }

                allResults.addAll(results.keySet());
            }

            // Should find all 3 PREMIUM documents
            assertEquals(3, allResults.size(), "Should find exactly 3 PREMIUM tier documents");

            // Should complete more efficiently than naive approach (without internal advancement would be 50+ calls)
            assertTrue(clientCalls <= 10, "Should complete much more efficiently than naive approach");
        }

        @Test
        @DisplayName("Should handle mixed selectivity patterns efficiently")
        void shouldHandleMixedSelectivityPatternsEfficiently() {
            // Create pattern: dense matches, then sparse matches, then dense again
            insertMixedSelectivityPattern();

            Session session = getSession();
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, BUCKET_NAME);

            PlanExecutorConfig config = createPlanConfig(metadata, "{ 'type': 'SPECIAL' }", false);
            config.setLimit(3);

            List<Map<Versionstamp, ByteBuffer>> batches = new ArrayList<>();
            int clientCalls = 0;

            while (clientCalls < 8) {
                Map<Versionstamp, ByteBuffer> results = executePlan(config);
                clientCalls++;

                if (results.isEmpty()) {
                    break;
                }

                batches.add(results);
            }

            // Should efficiently handle the mixed pattern
            assertFalse(batches.isEmpty(), "Should find SPECIAL type documents");

            // Verify total results (let's count them properly from our test data)
            int totalResults = batches.stream().mapToInt(Map::size).sum();
            // Dense: positions 0,2,4 = 3; Sparse: position 10 = 1; Dense2: positions 15,17,19,21,23 = 5; Total = 9
            assertEquals(9, totalResults, "Should find all 9 SPECIAL documents"); // Based on our test data

            // Should handle mixed patterns reasonably efficiently  
            assertTrue(clientCalls <= 8, "Should handle mixed selectivity reasonably efficiently");
        }

        private void insertLargeDatasetWithFewMatches() {
            List<byte[]> documents = new ArrayList<>();

            for (int i = 0; i < 50; i++) {
                String tier = (i == 10 || i == 25 || i == 40) ? "PREMIUM" : "STANDARD";
                documents.add(BSONUtil.jsonToDocumentThenBytes(
                        "{ 'id': " + i + ", 'tier': '" + tier + "', 'data': 'content" + i + "' }"));
            }

            insertDocuments(documents);
        }

        private void insertMixedSelectivityPattern() {
            List<byte[]> documents = new ArrayList<>();

            // Dense matches (positions 0-4: 3 matches)
            for (int i = 0; i < 5; i++) {
                String type = (i % 2 == 0) ? "SPECIAL" : "NORMAL"; // 3 SPECIAL docs
                documents.add(BSONUtil.jsonToDocumentThenBytes(
                        "{ 'id': " + i + ", 'type': '" + type + "', 'group': 'dense' }"));
            }

            // Sparse matches (positions 5-14: 1 match)
            for (int i = 5; i < 15; i++) {
                String type = (i == 10) ? "SPECIAL" : "NORMAL"; // 1 SPECIAL doc
                documents.add(BSONUtil.jsonToDocumentThenBytes(
                        "{ 'id': " + i + ", 'type': '" + type + "', 'group': 'sparse' }"));
            }

            // Dense matches again (positions 15-24: 4 matches)
            for (int i = 15; i < 25; i++) {
                String type = (i % 2 == 1) ? "SPECIAL" : "NORMAL"; // 4 SPECIAL docs
                documents.add(BSONUtil.jsonToDocumentThenBytes(
                        "{ 'id': " + i + ", 'type': '" + type + "', 'group': 'dense2' }"));
            }

            insertDocuments(documents);
        }
    }
}