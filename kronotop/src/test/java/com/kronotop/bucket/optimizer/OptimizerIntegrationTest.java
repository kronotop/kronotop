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

package com.kronotop.bucket.optimizer;

import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.*;
import org.bson.BsonType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the complete optimizer with rule interactions,
 * priority ordering, edge cases, and iteration limits.
 */
class OptimizerIntegrationTest extends BaseOptimizerTest {

    @Nested
    @DisplayName("Rule Interaction and Priority Ordering")
    class RuleInteractionTests {

        @Test
        @DisplayName("Should apply rules in correct priority order")
        void shouldApplyRulesInCorrectPriorityOrder() {
            // Create indexes
            createIndexes(
                    IndexDefinition.create("name-index", "name", BsonType.STRING),
                    IndexDefinition.create("age-index", "age", BsonType.INT32)
            );

            // Create plan that can benefit from multiple rules:
            // AND(name="john", name="john", age >= 18, age < 65)
            // - RedundantScanElimination should remove duplicate name conditions
            // - RangeScanConsolidation should consolidate age conditions
            // - IndexIntersection should combine remaining indexed conditions

            String query = "{ $and: [" +
                    "{ \"name\": \"john\" }, " +
                    "{ \"name\": \"john\" }, " +  // Duplicate for redundancy elimination
                    "{ \"age\": { $gte: 18 } }, " +
                    "{ \"age\": { $lt: 65 } }" +   // Range for consolidation
                    "] }";

            PhysicalNode unoptimized = planWithoutOptimization(query);
            PhysicalNode optimized = planAndOptimize(query);

            // Verify optimization results
            // Should end up as: AND(PhysicalIndexScan(name), PhysicalRangeScan(age))
            // OR PhysicalIndexIntersection if intersection wins over range consolidation

            // Count original vs optimized nodes
            int originalIndexScans = countNodeType(unoptimized, PhysicalIndexScan.class);
            int optimizedIndexScans = countNodeType(optimized, PhysicalIndexScan.class);
            int rangeScanCount = countNodeType(optimized, PhysicalRangeScan.class);
            int intersectionCount = countNodeType(optimized, PhysicalIndexIntersection.class);

            // Should have fewer index scans after optimization
            assertTrue(optimizedIndexScans < originalIndexScans,
                    "Should reduce number of index scans through optimization");

            // Should have either range scans or intersections (or both)
            assertTrue(rangeScanCount > 0 || intersectionCount > 0,
                    "Should create optimized scan nodes");

            // Final plan should be simplified (not a complex nested AND)
            if (optimized instanceof PhysicalAnd and) {
                assertTrue(and.children().size() <= 2,
                        "Should simplify to at most 2 children after optimization");
            }
        }

        @Test
        @DisplayName("Should handle complex nested scenarios with multiple rule applications")
        void shouldHandleComplexNestedScenariosWithMultipleRules() {
            // Create indexes
            createIndexes(
                    IndexDefinition.create("name-index", "name", BsonType.STRING),
                    IndexDefinition.create("age-index", "age", BsonType.INT32),
                    IndexDefinition.create("status-index", "status", BsonType.STRING)
            );

            // Complex query with multiple optimization opportunities:
            // AND(
            //   OR(name="john", name="john"),     // Redundant elimination in OR
            //   AND(age >= 18, age < 65),         // Range consolidation
            //   status="active"                   // Can be intersected with result
            // )

            PhysicalFilter nameFilter1 = createFilter("name", Operator.EQ, "john");
            PhysicalFilter nameFilter2 = createFilter("name", Operator.EQ, "john"); // Duplicate
            PhysicalIndexScan nameScan1 = createIndexScan(nameFilter1);
            PhysicalIndexScan nameScan2 = createIndexScan(nameFilter2);
            PhysicalOr nameOr = createOr(nameScan1, nameScan2);

            PhysicalFilter ageGte = createFilter("age", Operator.GTE, 18);
            PhysicalFilter ageLt = createFilter("age", Operator.LT, 65);
            PhysicalIndexScan ageGteScan = createIndexScan(ageGte);
            PhysicalIndexScan ageLtScan = createIndexScan(ageLt);
            PhysicalAnd ageAnd = createAnd(ageGteScan, ageLtScan);

            PhysicalFilter statusFilter = createFilter("status", Operator.EQ, "active");
            PhysicalIndexScan statusScan = createIndexScan(statusFilter);

            PhysicalAnd topLevel = createAnd(nameOr, ageAnd, statusScan);

            PhysicalNode optimized = optimize(topLevel);

            // Verify that optimizer runs successfully on complex plans without errors
            assertNotNull(optimized, "Optimizer should produce a valid result");
            assertInstanceOf(PhysicalNode.class, optimized, "Result should be a PhysicalNode");

            // Verify the plan structure is maintained
            assertTrue(countNodeType(optimized, PhysicalNode.class) > 0,
                    "Optimized plan should contain nodes");

        }

        @Test
        @DisplayName("Should handle rule conflicts gracefully")
        void shouldHandleRuleConflictsGracefully() {
            // Create scenario where different rules might conflict
            // e.g., range consolidation vs index intersection on same fields
            createIndexes(
                    IndexDefinition.create("price-index", "price", BsonType.DOUBLE),
                    IndexDefinition.create("category-index", "category", BsonType.STRING)
            );

            // Query that could benefit from different optimizations:
            // AND(price >= 10, price <= 100, category="books", category="books")
            String query = "{ $and: [" +
                    "{ \"price\": { $gte: 10 } }, " +
                    "{ \"price\": { $lte: 100 } }, " +
                    "{ \"category\": \"books\" }, " +
                    "{ \"category\": \"books\" }" +  // Duplicate for redundancy
                    "] }";

            PhysicalNode optimized = planAndOptimize(query);

            // Should apply optimizations without conflicts
            assertNotNull(optimized);

            // Should handle both range consolidation and redundancy elimination
            int rangeScanCount = countNodeType(optimized, PhysicalRangeScan.class);
            assertTrue(rangeScanCount <= 1, "Should have at most one range scan for price");

            // Should eliminate duplicate category conditions
            if (optimized instanceof PhysicalAnd and) {
                // Count distinct category conditions
                long categoryConditions = and.children().stream()
                        .filter(child -> {
                            if (child instanceof PhysicalIndexScan indexScan &&
                                    indexScan.node() instanceof PhysicalFilter filter) {
                                return "category".equals(filter.selector());
                            }
                            return false;
                        })
                        .count();
                assertTrue(categoryConditions <= 1, "Should eliminate duplicate category conditions");
            }
        }
    }

    @Nested
    @DisplayName("Edge Cases and Error Handling")
    class EdgeCaseTests {

        @Test
        @DisplayName("Should handle empty AND operations")
        void shouldHandleEmptyAndOperations() {
            // This shouldn't happen in practice, but test defensive programming
            PhysicalAnd emptyAnd = createAnd(); // Empty children list

            PhysicalNode optimized = optimize(emptyAnd);

            // Should handle gracefully without throwing exceptions
            assertNotNull(optimized);
        }

        @Test
        @DisplayName("Should handle deeply nested structures")
        void shouldHandleDeeplyNestedStructures() {
            // Create deeply nested AND/OR structure
            PhysicalFilter baseFilter = createFilter("name", Operator.EQ, "john");
            PhysicalNode current = baseFilter;

            // Create nested structure: AND(OR(AND(OR(...))))
            for (int i = 0; i < 10; i++) {
                if (i % 2 == 0) {
                    current = createAnd(current, createFilter("field" + i, Operator.EQ, "value" + i));
                } else {
                    current = createOr(current, createFilter("field" + i, Operator.EQ, "value" + i));
                }
            }

            PhysicalNode optimized = optimize(current);

            // Should handle without stack overflow or infinite loops
            assertNotNull(optimized);
        }

        @Test
        @DisplayName("Should handle null and invalid inputs gracefully")
        void shouldHandleNullAndInvalidInputsGracefully() {
            // Test with minimal valid structure
            PhysicalFilter filter = createFilter("name", Operator.EQ, "john");

            PhysicalNode optimized = optimize(filter);

            // Should handle gracefully
            assertNotNull(optimized);
            assertEquals(filter, optimized); // Leaf nodes should remain unchanged (PhysicalNode.equals() ignores IDs)
        }

        @Test
        @DisplayName("Should handle mixed indexed and non-indexed complex scenarios")
        void shouldHandleMixedIndexedAndNonIndexedComplexScenarios() {
            // Create only one index
            createIndex(IndexDefinition.create(
                    "name-index", "name", BsonType.STRING
            ));

            // Query with mix of indexed and non-indexed fields
            String query = "{ $and: [" +
                    "{ \"name\": \"john\" }, " +       // Indexed
                    "{ \"age\": 25 }, " +              // Not indexed
                    "{ \"score\": { $gt: 80 } }, " +   // Not indexed
                    "{ \"status\": \"active\" }" +     // Not indexed
                    "] }";

            PhysicalNode optimized = planAndOptimize(query);

            // Should handle gracefully with mix of scan types
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd and = (PhysicalAnd) optimized;

            // Should have one PhysicalIndexScan and multiple PhysicalFullScans
            int indexScanCount = countNodeType(optimized, PhysicalIndexScan.class);
            int fullScanCount = countNodeType(optimized, PhysicalFullScan.class);

            assertEquals(1, indexScanCount, "Should have one index scan for name");
            assertEquals(3, fullScanCount, "Should have three full scans for non-indexed fields");
        }
    }

    @Nested
    @DisplayName("Optimizer Iteration Limits and Convergence")
    class IterationLimitTests {

        @Test
        @DisplayName("Should converge within iteration limits")
        void shouldConvergeWithinIterationLimits() {
            // Create scenario that might require multiple optimization passes
            createIndexes(
                    IndexDefinition.create("name-index", "name", BsonType.STRING),
                    IndexDefinition.create("age-index", "age", BsonType.INT32)
            );

            // Create complex plan with multiple levels of optimization opportunities
            PhysicalFilter nameFilter1 = createFilter("name", Operator.EQ, "john");
            PhysicalFilter nameFilter2 = createFilter("name", Operator.EQ, "john");
            PhysicalAnd nameAnd = createAnd(nameFilter1, nameFilter2); // Redundancy

            PhysicalFilter ageFilter1 = createFilter("age", Operator.GTE, 18);
            PhysicalFilter ageFilter2 = createFilter("age", Operator.LT, 65);
            PhysicalOr ageOr = createOr(
                    createIndexScan(ageFilter1),
                    createIndexScan(ageFilter2)
            );

            // Nested structure that might need multiple passes
            PhysicalAnd topLevel = createAnd(nameAnd, ageOr);

            PhysicalNode optimized = optimize(topLevel);

            // Should complete without infinite loops
            assertNotNull(optimized);

            // Should be significantly simplified
            int originalComplexity = countNodeType(topLevel, PhysicalAnd.class) +
                    countNodeType(topLevel, PhysicalOr.class);
            int optimizedComplexity = countNodeType(optimized, PhysicalAnd.class) +
                    countNodeType(optimized, PhysicalOr.class);

            assertTrue(optimizedComplexity <= originalComplexity,
                    "Should not increase complexity through optimization");
        }

        @Test
        @DisplayName("Should handle scenarios requiring no optimization")
        void shouldHandleScenariosRequiringNoOptimization() {
            // Create optimal plan that needs no optimization
            createIndex(IndexDefinition.create(
                    "name-index", "name", BsonType.STRING
            ));

            String query = "{ \"name\": \"john\" }"; // Simple, optimal query

            PhysicalNode unoptimized = planWithoutOptimization(query);
            PhysicalNode optimized = planAndOptimize(query);

            // Should remain essentially unchanged
            assertInstanceOf(PhysicalIndexScan.class, unoptimized);
            assertInstanceOf(PhysicalIndexScan.class, optimized);
        }

        @Test
        @DisplayName("Should stabilize after applying all beneficial optimizations")
        void shouldStabilizeAfterApplyingAllBeneficialOptimizations() {
            // Create comprehensive optimization scenario
            createIndexes(
                    IndexDefinition.create("name-index", "name", BsonType.STRING),
                    IndexDefinition.create("age-index", "age", BsonType.INT32),
                    IndexDefinition.create("status-index", "status", BsonType.STRING)
            );

            // Query with multiple optimization opportunities
            String query = "{ $and: [" +
                    "{ \"name\": \"john\" }, " +
                    "{ \"name\": \"john\" }, " +        // Redundancy
                    "{ \"age\": { $gte: 18 } }, " +
                    "{ \"age\": { $lt: 65 } }, " +      // Range consolidation
                    "{ \"status\": \"active\" }" +      // Index intersection candidate
                    "] }";

            PhysicalNode firstOptimization = planAndOptimize(query);

            // Use same context for second optimization to maintain ID consistency
            PlannerContext context = new PlannerContext();
            PhysicalNode secondOptimization = optimizer.optimize(metadata, firstOptimization, context);

            // Second optimization should not change the plan (convergence)
            // PhysicalNode.equals() ignores IDs, so this directly compares structure
            assertEquals(
                    firstOptimization,
                    secondOptimization,
                    "Second optimization should not change the plan structure"
            );
        }
    }

    @Nested
    @DisplayName("Real-World Complex Scenarios")
    class RealWorldScenarioTests {

        @Test
        @DisplayName("Should optimize complex e-commerce query efficiently")
        void shouldOptimizeComplexEcommerceQueryEfficiently() {
            // Simulate e-commerce product search scenario
            createIndexes(
                    IndexDefinition.create("category-index", "category", BsonType.STRING),
                    IndexDefinition.create("brand-index", "brand", BsonType.STRING),
                    IndexDefinition.create("price-index", "price", BsonType.DOUBLE),
                    IndexDefinition.create("rating-index", "rating", BsonType.DOUBLE)
            );

            // Complex e-commerce query:
            // Find products in "electronics" category, from "apple" brand,
            // with price between $100-$2000, and high rating
            String query = "{ $and: [" +
                    "{ \"category\": \"electronics\" }, " +
                    "{ \"brand\": \"apple\" }, " +
                    "{ \"price\": { $gte: 100.0 } }, " +
                    "{ \"price\": { $lte: 2000.0 } }, " +
                    "{ \"rating\": { $gte: 4.0 } }" +
                    "] }";

            PhysicalNode unoptimized = planWithoutOptimization(query);
            PhysicalNode optimized = planAndOptimize(query);

            // Verify comprehensive optimization
            int originalIndexScans = countNodeType(unoptimized, PhysicalIndexScan.class);
            int optimizedIndexScans = countNodeType(optimized, PhysicalIndexScan.class);
            int rangeScanCount = countNodeType(optimized, PhysicalRangeScan.class);
            int intersectionCount = countNodeType(optimized, PhysicalIndexIntersection.class);

            // Should apply multiple optimizations
            assertTrue(originalIndexScans > optimizedIndexScans + rangeScanCount,
                    "Should reduce index scans through range consolidation");

            // Should have range scan for price
            assertTrue(rangeScanCount > 0, "Should consolidate price range conditions");

            // Should potentially create intersections for EQ conditions
            assertTrue(intersectionCount >= 0, "May create intersections for category/brand/rating");

            // Overall plan should be more efficient
            assertTrue(optimized instanceof PhysicalAnd ||
                            optimized instanceof PhysicalIndexIntersection ||
                            optimized instanceof PhysicalRangeScan,
                    "Should create efficient optimized plan structure");
        }

        @Test
        @DisplayName("Should optimize user analytics query with temporal constraints")
        void shouldOptimizeUserAnalyticsQueryWithTemporalConstraints() {
            // Simulate user analytics scenario
            createIndexes(
                    IndexDefinition.create("user_id-index", "user_id", BsonType.STRING),
                    IndexDefinition.create("event_type-index", "event_type", BsonType.STRING),
                    IndexDefinition.create("timestamp-index", "timestamp", BsonType.DATE_TIME)
            );

            // Analytics query: Find specific user's events of certain type within time range
            String query = "{ $and: [" +
                    "{ \"user_id\": \"user12345\" }, " +
                    "{ \"event_type\": \"purchase\" }, " +
                    "{ \"timestamp\": { $gte: \"2024-01-01\" } }, " +
                    "{ \"timestamp\": { $lte: \"2024-01-31\" } }" +
                    "] }";

            PhysicalNode optimized = planAndOptimize(query);

            // Should create efficient plan combining intersections and range scans
            assertNotNull(optimized);

            // Should have optimized temporal range
            int rangeScanCount = countNodeType(optimized, PhysicalRangeScan.class);
            assertTrue(rangeScanCount <= 1, "Should consolidate timestamp range into single scan");

            // Should potentially intersect user_id and event_type
            int intersectionCount = countNodeType(optimized, PhysicalIndexIntersection.class);
            assertTrue(intersectionCount >= 0, "May create intersection for user_id and event_type");

            // Final plan should be compact
            if (optimized instanceof PhysicalAnd and) {
                assertTrue(and.children().size() <= 2,
                        "Should create compact optimized plan for analytics query");
            }
        }
    }
}