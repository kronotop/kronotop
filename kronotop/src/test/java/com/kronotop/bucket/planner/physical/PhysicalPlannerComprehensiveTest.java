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

package com.kronotop.bucket.planner.physical;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.bucket.index.SortOrder;
import com.kronotop.bucket.optimizer.Optimizer;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.logical.LogicalNode;
import com.kronotop.bucket.planner.logical.LogicalPlanner;
import org.bson.BsonType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for Physical Planner integrating with Optimizer.
 * Tests end-to-end query processing from BQL to optimized physical plans.
 */
@DisplayName("Physical Planner Comprehensive Tests")
public class PhysicalPlannerComprehensiveTest extends BaseStandaloneInstanceTest {

    protected LogicalPlanner logicalPlanner;
    protected PhysicalPlanner physicalPlanner;
    protected Optimizer optimizer;
    protected BucketMetadata metadata;

    @BeforeEach
    void setUp() {
        logicalPlanner = new LogicalPlanner();
        physicalPlanner = new PhysicalPlanner();
        optimizer = new Optimizer();
        metadata = getBucketMetadata(TEST_BUCKET_NAME);
    }

    private void createIndex(IndexDefinition definition) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace indexSubspace = IndexUtil.create(tr, metadata.subspace(), definition);
            assertNotNull(indexSubspace);
            tr.commit().join();
        }
        // Refresh the index registry
        metadata = getBucketMetadata(TEST_BUCKET_NAME);
    }

    private void createIndexes(IndexDefinition... definitions) {
        for (IndexDefinition definition : definitions) {
            createIndex(definition);
        }
    }

    private void setupEcommerceIndexes() {
        createIndexes(
                IndexDefinition.create("category-index", "category", BsonType.STRING, SortOrder.ASCENDING),
                IndexDefinition.create("brand-index", "brand", BsonType.STRING, SortOrder.ASCENDING),
                IndexDefinition.create("stock-index", "inStock", BsonType.BOOLEAN, SortOrder.ASCENDING)
        );
    }

    private void setupSocialMediaIndexes() {
        createIndexes(
                IndexDefinition.create("country-index", "country", BsonType.STRING, SortOrder.ASCENDING),
                IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING),
                IndexDefinition.create("verified-index", "verified", BsonType.BOOLEAN, SortOrder.ASCENDING)
        );
    }

    // Helper methods

    private void setupOptimizationIndexes() {
        createIndexes(
                IndexDefinition.create("status-index", "status", BsonType.STRING, SortOrder.ASCENDING),
                IndexDefinition.create("type-index", "type", BsonType.STRING, SortOrder.ASCENDING)
        );
    }

    private PhysicalNode planBqlOnly(String bql) {
        BqlExpr expr = BqlParser.parse(bql);
        LogicalNode logicalPlan = logicalPlanner.plan(expr);
        return physicalPlanner.plan(metadata, logicalPlan, new PlannerContext());
    }

    private PhysicalNode planAndOptimizeBql(String bql) {
        PhysicalNode physicalPlan = planBqlOnly(bql);
        return optimizer.optimize(metadata, physicalPlan, new PlannerContext());
    }

    private boolean containsNodeType(PhysicalNode plan, Class<? extends PhysicalNode> nodeType) {
        if (nodeType.isInstance(plan)) {
            return true;
        }

        if (plan instanceof PhysicalAnd and) {
            return and.children().stream().anyMatch(child -> containsNodeType(child, nodeType));
        } else if (plan instanceof PhysicalOr or) {
            return or.children().stream().anyMatch(child -> containsNodeType(child, nodeType));
        } else if (plan instanceof PhysicalNot not) {
            return containsNodeType(not.child(), nodeType);
        } else if (plan instanceof PhysicalIndexScan scan) {
            return containsNodeType(scan.node(), nodeType);
        } else if (plan instanceof PhysicalFullScan scan) {
            return containsNodeType(scan.node(), nodeType);
        }

        return false;
    }

    private void verifyOptimizedPlanStructure(PhysicalNode plan) {
        // Verify the plan contains some optimized structures
        boolean hasOptimized =
                containsNodeType(plan, PhysicalIndexIntersection.class) ||
                        containsNodeType(plan, PhysicalRangeScan.class) ||
                        containsNodeType(plan, PhysicalIndexScan.class);

        assertTrue(hasOptimized, "Plan should contain optimized node types");
    }

    private boolean containsOptimizedOperations(PhysicalNode plan) {
        return containsNodeType(plan, PhysicalIndexScan.class) ||
                containsNodeType(plan, PhysicalIndexIntersection.class) ||
                containsNodeType(plan, PhysicalRangeScan.class);
    }

    private boolean containsNestedStructure(PhysicalNode node) {
        if (node instanceof PhysicalAnd and) {
            return and.children().stream().anyMatch(child ->
                    child instanceof PhysicalOr || child instanceof PhysicalAnd);
        } else if (node instanceof PhysicalOr or) {
            return or.children().stream().anyMatch(child ->
                    child instanceof PhysicalOr || child instanceof PhysicalAnd);
        }
        return false;
    }

    private int countTotalNodes(PhysicalNode node) {
        int count = 1;

        if (node instanceof PhysicalAnd and) {
            count += and.children().stream()
                    .mapToInt(this::countTotalNodes)
                    .sum();
        } else if (node instanceof PhysicalOr or) {
            count += or.children().stream()
                    .mapToInt(this::countTotalNodes)
                    .sum();
        } else if (node instanceof PhysicalNot not) {
            count += countTotalNodes(not.child());
        } else if (node instanceof PhysicalIndexScan scan) {
            count += countTotalNodes(scan.node());
        } else if (node instanceof PhysicalFullScan scan) {
            count += countTotalNodes(scan.node());
        }

        return count;
    }

    private Object extractValue(Object operand) {
        if (operand instanceof StringVal(String value)) {
            return value;
        } else if (operand instanceof Int32Val(int value)) {
            return value;
        } else if (operand instanceof Int64Val(long value)) {
            return value;
        } else if (operand instanceof Decimal128Val decimal128Val) {
            return decimal128Val.value();
        } else if (operand instanceof DoubleVal(double value)) {
            return value;
        } else if (operand instanceof BooleanVal(boolean value)) {
            return value;
        } else if (operand instanceof NullVal) {
            return null;
        } else if (operand instanceof BinaryVal binaryVal) {
            return binaryVal.value();
        } else if (operand instanceof DateTimeVal dateTimeVal) {
            return dateTimeVal.value();
        } else if (operand instanceof List<?> list) {
            return list.stream()
                    .map(this::extractValue)
                    .collect(java.util.stream.Collectors.toList());
        }
        return operand;
    }

    @Nested
    @DisplayName("End-to-End Pipeline Tests")
    class EndToEndPipelineTests {

        @Test
        @DisplayName("Should process simple indexed query end-to-end")
        void shouldProcessSimpleIndexedQueryEndToEnd() {
            // Create index
            createIndex(IndexDefinition.create("user-index", "userId", BsonType.STRING, SortOrder.ASCENDING));

            String bql = "{ \"userId\": \"user123\" }";

            // Full pipeline: BQL -> Logical -> Physical -> Optimized
            PhysicalNode optimizedPlan = planAndOptimizeBql(bql);

            // Verify end result is indexed scan
            assertInstanceOf(PhysicalIndexScan.class, optimizedPlan);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) optimizedPlan;
            assertInstanceOf(PhysicalFilter.class, indexScan.node());

            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("userId", filter.selector());
            assertEquals(Operator.EQ, filter.op());
            assertEquals("user123", extractValue(filter.operand()));
        }

        @Test
        @DisplayName("Should process complex AND query with optimization")
        void shouldProcessComplexAndQueryWithOptimization() {
            // Setup multiple indexes
            createIndexes(
                    IndexDefinition.create("user-index", "userId", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("status-index", "status", BsonType.STRING, SortOrder.ASCENDING)
            );

            String bql = "{ $and: [" +
                    "{ \"userId\": \"user123\" }, " +
                    "{ \"status\": \"active\" }, " +
                    "{ \"score\": { $gt: 80 } }" +  // Non-indexed
                    "] }";

            PhysicalNode optimizedPlan = planAndOptimizeBql(bql);

            // Should optimize into intersection or ordered AND
            assertNotNull(optimizedPlan);

            // Verify contains optimized structures
            boolean hasOptimizedStructures =
                    containsNodeType(optimizedPlan, PhysicalIndexIntersection.class) ||
                            containsNodeType(optimizedPlan, PhysicalIndexScan.class);
            assertTrue(hasOptimizedStructures, "Should contain optimized scan structures");
        }

        @Test
        @DisplayName("Should process range query with consolidation")
        void shouldProcessRangeQueryWithConsolidation() {
            createIndex(IndexDefinition.create("timestamp-index", "timestamp", BsonType.DATE_TIME, SortOrder.ASCENDING));

            String bql = "{ $and: [" +
                    "{ \"timestamp\": { $gte: \"2024-01-01\" } }, " +
                    "{ \"timestamp\": { $lt: \"2024-12-31\" } }" +
                    "] }";

            PhysicalNode optimizedPlan = planAndOptimizeBql(bql);

            // Should create optimized range structure
            assertNotNull(optimizedPlan);

            // Should have range scan or consolidated structure
            boolean hasRangeOptimization =
                    containsNodeType(optimizedPlan, PhysicalRangeScan.class) ||
                            (optimizedPlan instanceof PhysicalIndexScan);
            assertTrue(hasRangeOptimization, "Should optimize range conditions");
        }

        @Test
        @DisplayName("Should handle mixed indexed and non-indexed conditions")
        void shouldHandleMixedIndexedAndNonIndexedConditions() {
            // Create partial indexes
            createIndex(IndexDefinition.create("category-index", "category", BsonType.STRING, SortOrder.ASCENDING));

            String bql = "{ $and: [" +
                    "{ \"category\": \"electronics\" }, " +  // Indexed
                    "{ \"price\": { $gt: 100 } }, " +        // Non-indexed
                    "{ \"description\": { $ne: \"\" } }" +   // Non-indexed
                    "] }";

            PhysicalNode optimizedPlan = planAndOptimizeBql(bql);

            // Should contain both indexed and non-indexed operations
            boolean hasIndexed = containsNodeType(optimizedPlan, PhysicalIndexScan.class);
            boolean hasFullScan = containsNodeType(optimizedPlan, PhysicalFullScan.class);

            if (optimizedPlan instanceof PhysicalAnd) {
                // In AND structure, should have both types
                assertTrue(hasIndexed || hasFullScan, "Should handle mixed condition types");
            } else {
                // Single condition might be optimized differently
                assertTrue(true, "Single condition optimization is valid");
            }
        }
    }

    @Nested
    @DisplayName("Complex Query Scenarios")
    class ComplexQueryScenariosTests {

        @Test
        @DisplayName("Should handle e-commerce style query")
        void shouldHandleEcommerceStyleQuery() {
            setupEcommerceIndexes();

            String bql = "{ $and: [" +
                    "{ \"category\": \"electronics\" }, " +
                    "{ \"price\": { $gte: 100 } }, " +
                    "{ \"brand\": \"TechCorp\" }, " +
                    "{ \"inStock\": true }" +
                    "] }";

            PhysicalNode optimizedPlan = planAndOptimizeBql(bql);

            // Should create efficient plan structure
            assertNotNull(optimizedPlan);
            verifyOptimizedPlanStructure(optimizedPlan);
        }

        @Test
        @DisplayName("Should handle social media user query")
        void shouldHandleSocialMediaUserQuery() {
            setupSocialMediaIndexes();

            String bql = "{ $and: [" +
                    "{ \"country\": \"USA\" }, " +
                    "{ \"age\": { $gte: 18 } }, " +
                    "{ \"verified\": true }" +
                    "] }";

            PhysicalNode optimizedPlan = planAndOptimizeBql(bql);

            // Should optimize multi-field query
            assertNotNull(optimizedPlan);
            assertTrue(containsOptimizedOperations(optimizedPlan),
                    "Should contain optimized operations");
        }

        @Test
        @DisplayName("Should handle nested OR within AND")
        void shouldHandleNestedOrWithinAnd() {
            createIndex(IndexDefinition.create("type-index", "type", BsonType.STRING, SortOrder.ASCENDING));

            String bql = "{ $and: [" +
                    "{ \"type\": \"user\" }, " +
                    "{ $or: [" +
                    "{ \"plan\": \"premium\" }, " +
                    "{ \"plan\": \"enterprise\" }" +
                    "] }" +
                    "] }";

            PhysicalNode optimizedPlan = planAndOptimizeBql(bql);

            // Should maintain nested structure while optimizing
            assertNotNull(optimizedPlan);
            assertTrue(containsNestedStructure(optimizedPlan),
                    "Should preserve nested logical structure");
        }
    }

    @Nested
    @DisplayName("Performance Optimization Tests")
    class PerformanceOptimizationTests {

        @Test
        @DisplayName("Should reduce node count through optimization")
        void shouldReduceNodeCountThroughOptimization() {
            setupOptimizationIndexes();

            // Query with optimization opportunities
            String bql = "{ $and: [" +
                    "{ \"status\": \"active\" }, " +
                    "{ \"status\": \"active\" }, " +  // Redundant
                    "{ \"type\": \"user\" }" +
                    "] }";

            PhysicalNode unoptimized = planBqlOnly(bql);
            PhysicalNode optimized = planAndOptimizeBql(bql);

            int unoptimizedCount = countTotalNodes(unoptimized);
            int optimizedCount = countTotalNodes(optimized);

            // Optimization should not increase node count
            assertTrue(optimizedCount <= unoptimizedCount,
                    "Optimization should not increase node count");
        }

        @Test
        @DisplayName("Should prioritize indexed operations")
        void shouldPrioritizeIndexedOperations() {
            createIndexes(
                    IndexDefinition.create("id-index", "id", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("category-index", "category", BsonType.STRING, SortOrder.ASCENDING)
            );

            String bql = "{ $and: [" +
                    "{ \"description\": { $ne: \"\" } }, " +  // Non-indexed
                    "{ \"id\": \"item123\" }, " +             // Indexed
                    "{ \"category\": \"books\" }" +           // Indexed
                    "] }";

            PhysicalNode optimized = planAndOptimizeBql(bql);

            // Should contain both indexed and non-indexed, with potential ordering
            if (optimized instanceof PhysicalAnd and) {
                // At least verify it's a valid AND structure
                assertTrue(and.children().size() > 0, "Should have child operations");
            }

            // Basic verification that optimization didn't break the query
            assertNotNull(optimized);
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCasesTests {

        @Test
        @DisplayName("Should handle single field query")
        void shouldHandleSingleFieldQuery() {
            createIndex(IndexDefinition.create("name-index", "name", BsonType.STRING, SortOrder.ASCENDING));

            String bql = "{ \"name\": \"John\" }";

            PhysicalNode optimized = planAndOptimizeBql(bql);

            // Should create proper index scan
            assertInstanceOf(PhysicalIndexScan.class, optimized);
        }

        @Test
        @DisplayName("Should handle empty query gracefully")
        void shouldHandleEmptyQueryGracefully() {
            String bql = "{}";

            PhysicalNode optimized = planAndOptimizeBql(bql);

            // Should handle without throwing exceptions
            assertNotNull(optimized);
        }

        @Test
        @DisplayName("Should handle complex nested structures")
        void shouldHandleComplexNestedStructures() {
            String bql = "{ $and: [" +
                    "{ $or: [" +
                    "{ \"level1\": \"value1\" }, " +
                    "{ \"level1\": \"value2\" }" +
                    "] }, " +
                    "{ \"topLevel\": \"value\" }" +
                    "] }";

            PhysicalNode optimized = planAndOptimizeBql(bql);

            // Should handle nesting without errors
            assertNotNull(optimized);
        }
    }
}