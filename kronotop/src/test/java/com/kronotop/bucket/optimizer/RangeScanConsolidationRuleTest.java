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
import com.kronotop.bucket.index.SortOrder;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.*;
import org.bson.BsonType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for RangeScanConsolidationRule.
 * Tests all scenarios where range conditions should be consolidated.
 */
class RangeScanConsolidationRuleTest extends BaseOptimizerTest {

    @Nested
    @DisplayName("Basic Range Consolidation")
    class BasicRangeConsolidationTests {

        @Test
        @DisplayName("Should consolidate GTE and LT into range scan")
        void shouldConsolidateGteAndLtIntoRangeScan() {
            // Create index for age field
            createIndex(IndexDefinition.create(
                    "age-index", "age", BsonType.INT32, SortOrder.ASCENDING
            ));

            // Test: AND(age >= 18, age < 65)
            String query = "{ $and: [{ \"age\": { $gte: 18 } }, { \"age\": { $lt: 65 } }] }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should be optimized to PhysicalRangeScan
            assertInstanceOf(PhysicalRangeScan.class, optimized);
            PhysicalRangeScan rangeScan = (PhysicalRangeScan) optimized;

            assertEquals("age", rangeScan.selector());
            assertEquals(18, extractValue(rangeScan.lowerBound()));
            assertEquals(65, extractValue(rangeScan.upperBound()));
            assertTrue(rangeScan.includeLower());
            assertFalse(rangeScan.includeUpper());
            assertNotNull(rangeScan.index());
            assertEquals("age", rangeScan.index().selector());
        }

        @Test
        @DisplayName("Should consolidate GT and LTE into range scan")
        void shouldConsolidateGtAndLteIntoRangeScan() {
            // Create index for score field
            createIndex(IndexDefinition.create(
                    "score-index", "score", BsonType.DOUBLE, SortOrder.ASCENDING
            ));

            // Test: AND(score > 50.0, score <= 100.0)
            String query = "{ $and: [{ \"score\": { $gt: 50.0 } }, { \"score\": { $lte: 100.0 } }] }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should be optimized to PhysicalRangeScan
            assertInstanceOf(PhysicalRangeScan.class, optimized);
            PhysicalRangeScan rangeScan = (PhysicalRangeScan) optimized;

            assertEquals("score", rangeScan.selector());
            assertEquals(50.0, extractValue(rangeScan.lowerBound()));
            assertEquals(100.0, extractValue(rangeScan.upperBound()));
            assertFalse(rangeScan.includeLower());
            assertTrue(rangeScan.includeUpper());
        }

        @Test
        @DisplayName("Should consolidate multiple range conditions")
        void shouldConsolidateMultipleRangeConditions() {
            // Create index for price field
            createIndex(IndexDefinition.create(
                    "price-index", "price", BsonType.DOUBLE, SortOrder.ASCENDING
            ));

            // Test: AND(price >= 10.0, price < 100.0, price > 5.0, price <= 99.0)
            String query = "{ $and: [" +
                    "{ \"price\": { $gte: 10.0 } }, " +
                    "{ \"price\": { $lt: 100.0 } }, " +
                    "{ \"price\": { $gt: 5.0 } }, " +
                    "{ \"price\": { $lte: 99.0 } }" +
                    "] }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should consolidate to most restrictive bounds: price >= 10.0 AND price <= 99.0
            assertInstanceOf(PhysicalRangeScan.class, optimized);
            PhysicalRangeScan rangeScan = (PhysicalRangeScan) optimized;

            assertEquals("price", rangeScan.selector());
            assertEquals(10.0, extractValue(rangeScan.lowerBound())); // Most restrictive lower
            assertEquals(99.0, extractValue(rangeScan.upperBound())); // Most restrictive upper
            assertTrue(rangeScan.includeLower()); // GTE
            assertTrue(rangeScan.includeUpper()); // LTE
        }
    }

    @Nested
    @DisplayName("Range Consolidation with Mixed Conditions")
    class MixedConditionTests {

        @Test
        @DisplayName("Should consolidate range conditions and preserve non-range conditions")
        void shouldConsolidateRangeAndPreserveNonRange() {
            // Create indexes
            createIndexes(
                    IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING),
                    IndexDefinition.create("name-index", "name", BsonType.STRING, SortOrder.ASCENDING)
            );

            // Test: AND(age >= 18, age < 65, name = "john")
            String query = "{ $and: [" +
                    "{ \"age\": { $gte: 18 } }, " +
                    "{ \"age\": { $lt: 65 } }, " +
                    "{ \"name\": \"john\" }" +
                    "] }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should be AND(PhysicalRangeScan(age), PhysicalIndexScan(name))
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd and = (PhysicalAnd) optimized;
            assertEquals(2, and.children().size());

            // One child should be PhysicalRangeScan for age
            boolean hasRangeScan = and.children().stream()
                    .anyMatch(child -> child instanceof PhysicalRangeScan rangeScan &&
                            "age".equals(rangeScan.selector()));
            assertTrue(hasRangeScan, "Should contain PhysicalRangeScan for age");

            // One child should be PhysicalIndexScan for name
            boolean hasNameScan = and.children().stream()
                    .anyMatch(child -> child instanceof PhysicalIndexScan indexScan &&
                            indexScan.node() instanceof PhysicalFilter filter &&
                            "name".equals(filter.selector()));
            assertTrue(hasNameScan, "Should contain PhysicalIndexScan for name");
        }

        @Test
        @DisplayName("Should handle range conditions on different fields separately")
        void shouldHandleRangeConditionsOnDifferentFieldsSeparately() {
            // Create indexes
            createIndexes(
                    IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING),
                    IndexDefinition.create("score-index", "score", BsonType.DOUBLE, SortOrder.ASCENDING)
            );

            // Test: AND(age >= 18, age < 65, score > 50, score <= 100)
            String query = "{ $and: [" +
                    "{ \"age\": { $gte: 18 } }, " +
                    "{ \"age\": { $lt: 65 } }, " +
                    "{ \"score\": { $gt: 50 } }, " +
                    "{ \"score\": { $lte: 100 } }" +
                    "] }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should create two separate range scans
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd and = (PhysicalAnd) optimized;
            assertEquals(2, and.children().size());

            // Should have PhysicalRangeScan for both age and score
            long rangeScanCount = and.children().stream()
                    .filter(child -> child instanceof PhysicalRangeScan)
                    .count();
            assertEquals(2, rangeScanCount, "Should have two range scans");
        }
    }

    @Nested
    @DisplayName("Edge Cases and Limitations")
    class EdgeCaseTests {

        @Test
        @DisplayName("Should consolidate range conditions even when no index exists")
        void shouldConsolidateEvenWhenNoIndexExists() {
            // No index created for age field

            // Test: AND(age >= 18, age < 65)
            String query = "{ $and: [{ \"age\": { $gte: 18 } }, { \"age\": { $lt: 65 } }] }";
            PhysicalNode optimized = planAndOptimize(query);

            // After our changes: RangeScanConsolidationRule creates PhysicalRangeScan with null index,
            // then RangeScanFallbackRule converts it to PhysicalFullScan with composite filters
            assertInstanceOf(PhysicalFullScan.class, optimized);
            PhysicalFullScan fullScan = (PhysicalFullScan) optimized;

            // Should have PhysicalAnd with two PhysicalFilter children
            assertInstanceOf(PhysicalAnd.class, fullScan.node());
            PhysicalAnd and = (PhysicalAnd) fullScan.node();
            assertEquals(2, and.children().size());

            // Check first condition: age >= 18
            PhysicalFilter filter1 = (PhysicalFilter) and.children().get(0);
            assertEquals("age", filter1.selector());
            assertEquals(Operator.GTE, filter1.op());
            assertEquals(18, extractValue(filter1.operand()));

            // Check second condition: age < 65
            PhysicalFilter filter2 = (PhysicalFilter) and.children().get(1);
            assertEquals("age", filter2.selector());
            assertEquals(Operator.LT, filter2.op());
            assertEquals(65, extractValue(filter2.operand()));
        }

        @Test
        @DisplayName("Should not consolidate single range condition")
        void shouldNotConsolidateSingleRangeCondition() {
            // Create index
            createIndex(IndexDefinition.create(
                    "age-index", "age", BsonType.INT32, SortOrder.ASCENDING
            ));

            // Test: age >= 18 (single condition)
            String query = "{ \"age\": { $gte: 18 } }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should remain as PhysicalIndexScan (not converted to range scan)
            assertInstanceOf(PhysicalIndexScan.class, optimized);
        }

        @Test
        @DisplayName("Should consolidate overlapping lower bound conditions to most restrictive")
        void shouldConsolidateOverlappingLowerBoundConditions() {
            // Create index
            createIndex(IndexDefinition.create(
                    "age-index", "age", BsonType.INT32, SortOrder.ASCENDING
            ));

            // Test: AND(age >= 18, age > 21) - should consolidate to age > 21 (most restrictive)
            String query = "{ $and: [{ \"age\": { $gte: 18 } }, { \"age\": { $gt: 21 } }] }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should consolidate to single index scan with most restrictive condition (age > 21)
            // Since both are lower bounds only, consolidation should occur but remain as index scan
            assertInstanceOf(PhysicalIndexScan.class, optimized);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) optimized;

            // Verify the condition is the most restrictive one (age > 21)
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("age", filter.selector());
            assertEquals(Operator.GT, filter.op());
            assertEquals(21, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("Should consolidate overlapping upper bound conditions to most restrictive")
        void shouldConsolidateOverlappingUpperBoundConditions() {
            // Create index
            createIndex(IndexDefinition.create(
                    "age-index", "age", BsonType.INT32, SortOrder.ASCENDING
            ));

            // Test: AND(age < 65, age <= 50) - should consolidate to age <= 50 (most restrictive)
            String query = "{ $and: [{ \"age\": { $lt: 65 } }, { \"age\": { $lte: 50 } }] }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should consolidate to single index scan with most restrictive condition (age <= 50)
            // Since both are upper bounds only, consolidation should occur but remain as index scan
            assertInstanceOf(PhysicalIndexScan.class, optimized);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) optimized;

            // Verify the condition is the most restrictive one (age <= 50)
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("age", filter.selector());
            assertEquals(Operator.LTE, filter.op());
            assertEquals(50, extractValue(filter.operand()));
        }
    }

    @Nested
    @DisplayName("Nested Structure Range Consolidation")
    class NestedStructureTests {

        @Test
        @DisplayName("Should consolidate range conditions in nested AND operations")
        void shouldConsolidateRangeConditionsInNestedAnd() {
            // Create index
            createIndex(IndexDefinition.create(
                    "age-index", "age", BsonType.INT32, SortOrder.ASCENDING
            ));

            // Create simple nested structure: AND(age >= 18, age < 65) - should consolidate to PhysicalRangeScan
            PhysicalFilter ageGte = createFilter("age", Operator.GTE, 18);
            PhysicalFilter ageLt = createFilter("age", Operator.LT, 65);
            PhysicalIndexScan ageGteScan = createIndexScan(ageGte);
            PhysicalIndexScan ageLtScan = createIndexScan(ageLt);
            PhysicalAnd ageAnd = createAnd(ageGteScan, ageLtScan);

            PhysicalNode optimized = optimize(ageAnd);

            // Should consolidate to a single PhysicalRangeScan (not maintain AND structure)
            assertInstanceOf(PhysicalRangeScan.class, optimized);
            PhysicalRangeScan rangeScan = (PhysicalRangeScan) optimized;

            // Verify the range scan properties
            assertEquals("age", rangeScan.selector());
            assertEquals(18, extractValue(rangeScan.lowerBound()));
            assertEquals(65, extractValue(rangeScan.upperBound()));
            assertTrue(rangeScan.includeLower()); // GTE
            assertFalse(rangeScan.includeUpper()); // LT
        }

        @Test
        @DisplayName("Should handle range consolidation with NOT operations")
        void shouldHandleRangeConsolidationWithNotOperations() {
            // Create index
            createIndex(IndexDefinition.create(
                    "age-index", "age", BsonType.INT32, SortOrder.ASCENDING
            ));

            // Create: NOT(AND(age >= 18, age < 65))
            PhysicalFilter ageGte = createFilter("age", Operator.GTE, 18);
            PhysicalFilter ageLt = createFilter("age", Operator.LT, 65);
            PhysicalIndexScan ageGteScan = createIndexScan(ageGte);
            PhysicalIndexScan ageLtScan = createIndexScan(ageLt);
            PhysicalAnd ageAnd = createAnd(ageGteScan, ageLtScan);
            PhysicalNot notOperation = new PhysicalNot(1, ageAnd);

            PhysicalNode optimized = optimize(notOperation);

            // Should consolidate inner AND to range scan: NOT(PhysicalRangeScan)
            assertInstanceOf(PhysicalNot.class, optimized);
            PhysicalNot result = (PhysicalNot) optimized;
            assertInstanceOf(PhysicalRangeScan.class, result.child());

            PhysicalRangeScan rangeScan = (PhysicalRangeScan) result.child();
            assertEquals("age", rangeScan.selector());
        }
    }

    @Nested
    @DisplayName("Performance and Optimization Verification")
    class PerformanceVerificationTests {

        @Test
        @DisplayName("Should reduce number of index scans through consolidation")
        void shouldReduceNumberOfIndexScansThoughConsolidation() {
            // Create index
            createIndex(IndexDefinition.create(
                    "price-index", "price", BsonType.DOUBLE, SortOrder.ASCENDING
            ));

            // Test: AND(price >= 10, price < 100, price > 5, price <= 95)
            String query = "{ $and: [" +
                    "{ \"price\": { $gte: 10 } }, " +
                    "{ \"price\": { $lt: 100 } }, " +
                    "{ \"price\": { $gt: 5 } }, " +
                    "{ \"price\": { $lte: 95 } }" +
                    "] }";

            PhysicalNode unoptimized = planWithoutOptimization(query);
            PhysicalNode optimized = planAndOptimize(query);

            // Count PhysicalIndexScan nodes
            int unoptimizedIndexScans = countNodeType(unoptimized, PhysicalIndexScan.class);
            int optimizedIndexScans = countNodeType(optimized, PhysicalIndexScan.class);

            // Should reduce from 4 index scans to 0 (replaced by 1 range scan)
            assertEquals(2, unoptimizedIndexScans);
            assertEquals(0, optimizedIndexScans);

            // Should have exactly 1 PhysicalRangeScan
            int rangeScanCount = countNodeType(optimized, PhysicalRangeScan.class);
            assertEquals(1, rangeScanCount);
        }

        @Test
        @DisplayName("Should preserve plan structure when no optimization possible")
        void shouldPreservePlanStructureWhenNoOptimizationPossible() {
            // Create index for different field
            createIndex(IndexDefinition.create(
                    "status-index", "status", BsonType.STRING, SortOrder.ASCENDING
            ));

            // Test: AND(status = "active", name = "john") - no range conditions
            String query = "{ $and: [{ \"status\": \"active\" }, { \"name\": \"john\" }] }";

            PhysicalNode unoptimized = planWithoutOptimization(query);
            PhysicalNode optimized = planAndOptimize(query);

            // Structure should be preserved (only redundant scan elimination might apply)
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd optimizedAnd = (PhysicalAnd) optimized;
            assertEquals(2, optimizedAnd.children().size());

            // Should not have any PhysicalRangeScan nodes
            int rangeScanCount = countNodeType(optimized, PhysicalRangeScan.class);
            assertEquals(0, rangeScanCount);
        }
    }

    @Nested
    @DisplayName("Full Scan Range Consolidation (No Index)")
    class FullScanRangeConsolidationTests {

        @Test
        @DisplayName("Should consolidate range conditions on full scan when no index exists")
        void shouldConsolidateRangeConditionsOnFullScanWithoutIndex() {
            // No index created for age field - should use PhysicalFullScan

            // Test: AND(age >= 18, age <= 65) without index
            String query = "{ $and: [{ \"age\": { $gte: 18 } }, { \"age\": { $lte: 65 } }] }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should consolidate into PhysicalFullScan with composite filters
            assertInstanceOf(PhysicalFullScan.class, optimized);
            PhysicalFullScan fullScan = (PhysicalFullScan) optimized;

            // Should have PhysicalAnd with two PhysicalFilter children
            assertInstanceOf(PhysicalAnd.class, fullScan.node());
            PhysicalAnd and = (PhysicalAnd) fullScan.node();
            assertEquals(2, and.children().size());

            // Check first condition: age >= 18
            PhysicalFilter filter1 = (PhysicalFilter) and.children().get(0);
            assertEquals("age", filter1.selector());
            assertEquals(Operator.GTE, filter1.op());
            assertEquals(18, extractValue(filter1.operand()));

            // Check second condition: age <= 65
            PhysicalFilter filter2 = (PhysicalFilter) and.children().get(1);
            assertEquals("age", filter2.selector());
            assertEquals(Operator.LTE, filter2.op());
            assertEquals(65, extractValue(filter2.operand()));
        }

        @Test
        @DisplayName("Should handle mixed bounds without index")
        void shouldHandleMixedBoundsWithoutIndex() {
            // No index created - should use PhysicalFullScan

            // Test: AND(age > 18, age < 65) without index  
            String query = "{ $and: [{ \"age\": { $gt: 18 } }, { \"age\": { $lt: 65 } }] }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should consolidate into PhysicalFullScan with composite filters
            assertInstanceOf(PhysicalFullScan.class, optimized);
            PhysicalFullScan fullScan = (PhysicalFullScan) optimized;

            // Should have PhysicalAnd with two PhysicalFilter children
            assertInstanceOf(PhysicalAnd.class, fullScan.node());
            PhysicalAnd and = (PhysicalAnd) fullScan.node();
            assertEquals(2, and.children().size());

            // Check first condition: age > 18
            PhysicalFilter filter1 = (PhysicalFilter) and.children().get(0);
            assertEquals("age", filter1.selector());
            assertEquals(Operator.GT, filter1.op());
            assertEquals(18, extractValue(filter1.operand()));

            // Check second condition: age < 65
            PhysicalFilter filter2 = (PhysicalFilter) and.children().get(1);
            assertEquals("age", filter2.selector());
            assertEquals(Operator.LT, filter2.op());
            assertEquals(65, extractValue(filter2.operand()));
        }

        @Test
        @DisplayName("Should preserve original plan when no consolidation possible without index")
        void shouldPreserveOriginalPlanWhenNoConsolidationPossibleWithoutIndex() {
            // No index created - should use PhysicalFullScan

            // Test: AND(age >= 18, name = "john") - can't consolidate different fields
            String query = "{ $and: [{ \"age\": { $gte: 18 } }, { \"name\": \"john\" }] }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should remain as PhysicalAnd with 2 PhysicalFullScan children
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd and = (PhysicalAnd) optimized;
            assertEquals(2, and.children().size());

            // Both children should be PhysicalFullScan since no indexes
            long fullScanCount = and.children().stream()
                    .filter(child -> child instanceof PhysicalFullScan)
                    .count();
            assertEquals(2, fullScanCount);

            // Should not have any PhysicalRangeScan
            int rangeScanCount = countNodeType(optimized, PhysicalRangeScan.class);
            assertEquals(0, rangeScanCount);
        }
    }
}