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
 * Integration tests for IndexIntersectionRule.
 * Tests all scenarios where multiple indexed conditions should be intersected.
 */
class IndexIntersectionRuleTest extends BaseOptimizerTest {

    @Nested
    @DisplayName("Basic Index Intersection")
    class BasicIndexIntersectionTests {

        @Test
        @DisplayName("Should create intersection for two indexed EQ conditions")
        void shouldCreateIntersectionForTwoIndexedEqConditions() {
            // Create indexes for name and age fields
            createIndexes(
                    IndexDefinition.create("name-index", "name", BsonType.STRING),
                    IndexDefinition.create("age-index", "age", BsonType.INT32)
            );

            // Test: AND(name="john", age=25)
            String query = "{ $and: [{ \"name\": \"john\" }, { \"age\": 25 }] }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should be optimized to PhysicalIndexIntersection
            assertInstanceOf(PhysicalIndexIntersection.class, optimized);
            PhysicalIndexIntersection intersection = (PhysicalIndexIntersection) optimized;

            assertEquals(2, intersection.indexes().size());
            assertEquals(2, intersection.filters().size());

            // Verify indexes
            assertTrue(intersection.indexes().stream()
                    .anyMatch(idx -> "name".equals(idx.selector())));
            assertTrue(intersection.indexes().stream()
                    .anyMatch(idx -> "age".equals(idx.selector())));

            // Verify filters
            assertTrue(intersection.filters().stream()
                    .anyMatch(filter -> "name".equals(filter.selector()) &&
                            "john".equals(extractValue(filter.operand()))));
            assertTrue(intersection.filters().stream()
                    .anyMatch(filter -> "age".equals(filter.selector()) &&
                            Integer.valueOf(25).equals(extractValue(filter.operand()))));
        }

        @Test
        @DisplayName("Should create intersection for three indexed EQ conditions")
        void shouldCreateIntersectionForThreeIndexedEqConditions() {
            // Create indexes for multiple fields
            createIndexes(
                    IndexDefinition.create("name-index", "name", BsonType.STRING),
                    IndexDefinition.create("age-index", "age", BsonType.INT32),
                    IndexDefinition.create("status-index", "status", BsonType.STRING)
            );

            // Test: AND(name="john", age=25, status="active")
            String query = "{ $and: [" +
                    "{ \"name\": \"john\" }, " +
                    "{ \"age\": 25 }, " +
                    "{ \"status\": \"active\" }" +
                    "] }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should be optimized to PhysicalIndexIntersection
            assertInstanceOf(PhysicalIndexIntersection.class, optimized);
            PhysicalIndexIntersection intersection = (PhysicalIndexIntersection) optimized;

            assertEquals(3, intersection.indexes().size());
            assertEquals(3, intersection.filters().size());

            // Verify all three fields are included
            assertTrue(intersection.indexes().stream()
                    .anyMatch(idx -> "name".equals(idx.selector())));
            assertTrue(intersection.indexes().stream()
                    .anyMatch(idx -> "age".equals(idx.selector())));
            assertTrue(intersection.indexes().stream()
                    .anyMatch(idx -> "status".equals(idx.selector())));
        }

        @Test
        @DisplayName("Should not create intersection for single indexed condition")
        void shouldNotCreateIntersectionForSingleIndexedCondition() {
            // Create index for name field only
            createIndex(IndexDefinition.create(
                    "name-index", "name", BsonType.STRING
            ));

            // Test: name="john" (single condition)
            String query = "{ \"name\": \"john\" }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should remain as PhysicalIndexScan
            assertInstanceOf(PhysicalIndexScan.class, optimized);
        }
    }

    @Nested
    @DisplayName("Mixed Index and Non-Index Conditions")
    class MixedConditionTests {

        @Test
        @DisplayName("Should create intersection for indexed conditions and preserve non-indexed")
        void shouldCreateIntersectionForIndexedAndPreserveNonIndexed() {
            // Create indexes for name and age, but not for score
            createIndexes(
                    IndexDefinition.create("name-index", "name", BsonType.STRING),
                    IndexDefinition.create("age-index", "age", BsonType.INT32)
            );

            // Test: AND(name="john", age=25, score > 80) - score has no index
            String query = "{ $and: [" +
                    "{ \"name\": \"john\" }, " +
                    "{ \"age\": 25 }, " +
                    "{ \"score\": { $gt: 80 } }" +
                    "] }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should be AND(PhysicalIndexIntersection, PhysicalFullScan)
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd and = (PhysicalAnd) optimized;
            assertEquals(2, and.children().size());

            // One child should be PhysicalIndexIntersection for name and age
            boolean hasIntersection = and.children().stream()
                    .anyMatch(child -> child instanceof PhysicalIndexIntersection intersection &&
                            intersection.indexes().size() == 2);
            assertTrue(hasIntersection, "Should contain PhysicalIndexIntersection for name and age");

            // One child should be PhysicalFullScan for score
            boolean hasFullScan = and.children().stream()
                    .anyMatch(child -> child instanceof PhysicalFullScan fullScan &&
                            fullScan.node() instanceof PhysicalFilter filter &&
                            "score".equals(filter.selector()));
            assertTrue(hasFullScan, "Should contain PhysicalFullScan for score");
        }

        @Test
        @DisplayName("Should create intersection for indexed EQ and preserve range conditions")
        void shouldCreateIntersectionForIndexedEqAndPreserveRangeConditions() {
            // Create indexes for all fields
            createIndexes(
                    IndexDefinition.create("name-index", "name", BsonType.STRING),
                    IndexDefinition.create("age-index", "age", BsonType.INT32),
                    IndexDefinition.create("score-index", "score", BsonType.DOUBLE)
            );

            // Test: AND(name="john", age=25, score > 80) - only EQ should be intersected
            String query = "{ $and: [" +
                    "{ \"name\": \"john\" }, " +
                    "{ \"age\": 25 }, " +
                    "{ \"score\": { $gt: 80 } }" +
                    "] }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should be AND(PhysicalIndexIntersection, PhysicalIndexScan)
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd and = (PhysicalAnd) optimized;
            assertEquals(2, and.children().size());

            // One child should be PhysicalIndexIntersection for name and age (EQ conditions)
            boolean hasIntersection = and.children().stream()
                    .anyMatch(child -> child instanceof PhysicalIndexIntersection intersection &&
                            intersection.indexes().size() == 2);
            assertTrue(hasIntersection, "Should contain PhysicalIndexIntersection for EQ conditions");

            // One child should be PhysicalIndexScan for score (GT condition)
            boolean hasScoreScan = and.children().stream()
                    .anyMatch(child -> child instanceof PhysicalIndexScan indexScan &&
                            indexScan.node() instanceof PhysicalFilter filter &&
                            "score".equals(filter.selector()) &&
                            Operator.GT == filter.op());
            assertTrue(hasScoreScan, "Should contain PhysicalIndexScan for GT condition");
        }
    }

    @Nested
    @DisplayName("Non-EQ Operator Exclusions")
    class NonEqOperatorTests {

        @Test
        @DisplayName("Should not create intersection for non-EQ operators")
        void shouldNotCreateIntersectionForNonEqOperators() {
            // Create indexes for both fields
            createIndexes(
                    IndexDefinition.create("age-index", "age", BsonType.INT32),
                    IndexDefinition.create("score-index", "score", BsonType.DOUBLE)
            );

            // Test: AND(age > 18, score < 100) - no EQ operators
            String query = "{ $and: [{ \"age\": { $gt: 18 } }, { \"score\": { $lt: 100 } }] }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should remain as AND with two PhysicalIndexScans (no intersection)
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd and = (PhysicalAnd) optimized;
            assertEquals(2, and.children().size());

            // Both children should be PhysicalIndexScan
            assertTrue(and.children().stream()
                    .allMatch(child -> child instanceof PhysicalIndexScan));

            // Should not have any PhysicalIndexIntersection
            int intersectionCount = countNodeType(optimized, PhysicalIndexIntersection.class);
            assertEquals(0, intersectionCount);
        }

        @Test
        @DisplayName("Should handle mixed EQ and non-EQ operators correctly")
        void shouldHandleMixedEqAndNonEqOperatorsCorrectly() {
            // Create indexes for all fields
            createIndexes(
                    IndexDefinition.create("name-index", "name", BsonType.STRING),
                    IndexDefinition.create("age-index", "age", BsonType.INT32),
                    IndexDefinition.create("score-index", "score", BsonType.DOUBLE)
            );

            // Test: AND(name="john", age > 18, score="high") - only name and score are EQ
            String query = "{ $and: [" +
                    "{ \"name\": \"john\" }, " +
                    "{ \"age\": { $gt: 18 } }, " +
                    "{ \"score\": \"high\" }" +
                    "] }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should be AND(PhysicalIndexIntersection, PhysicalIndexScan)
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd and = (PhysicalAnd) optimized;
            assertEquals(2, and.children().size());

            // Should have PhysicalIndexIntersection for name and score (EQ conditions)
            boolean hasIntersection = and.children().stream()
                    .anyMatch(child -> child instanceof PhysicalIndexIntersection intersection &&
                            intersection.indexes().size() == 2 &&
                            intersection.indexes().stream().anyMatch(idx -> "name".equals(idx.selector())) &&
                            intersection.indexes().stream().anyMatch(idx -> "score".equals(idx.selector())));
            assertTrue(hasIntersection, "Should contain PhysicalIndexIntersection for EQ conditions");

            // Should have PhysicalIndexScan for age (GT condition)
            boolean hasAgeScan = and.children().stream()
                    .anyMatch(child -> child instanceof PhysicalIndexScan indexScan &&
                            indexScan.node() instanceof PhysicalFilter filter &&
                            "age".equals(filter.selector()) &&
                            Operator.GT == filter.op());
            assertTrue(hasAgeScan, "Should contain PhysicalIndexScan for GT condition");
        }
    }

    @Nested
    @DisplayName("Index Availability Requirements")
    class IndexAvailabilityTests {

        @Test
        @DisplayName("Should not create intersection when indexes are missing")
        void shouldNotCreateIntersectionWhenIndexesAreMissing() {
            // Create index only for name field
            createIndex(IndexDefinition.create(
                    "name-index", "name", BsonType.STRING
            ));

            // Test: AND(name="john", age=25) - age has no index
            String query = "{ $and: [{ \"name\": \"john\" }, { \"age\": 25 }] }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should be AND(PhysicalIndexScan, PhysicalFullScan) - no intersection
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd and = (PhysicalAnd) optimized;
            assertEquals(2, and.children().size());

            // Should not have any PhysicalIndexIntersection
            int intersectionCount = countNodeType(optimized, PhysicalIndexIntersection.class);
            assertEquals(0, intersectionCount);

            // Should have one PhysicalIndexScan and one PhysicalFullScan
            int indexScanCount = countNodeType(optimized, PhysicalIndexScan.class);
            int fullScanCount = countNodeType(optimized, PhysicalFullScan.class);
            assertEquals(1, indexScanCount);
            assertEquals(1, fullScanCount);
        }

        @Test
        @DisplayName("Should create intersection only for indexed fields")
        void shouldCreateIntersectionOnlyForIndexedFields() {
            // Create indexes for name and status, but not age
            createIndexes(
                    IndexDefinition.create("name-index", "name", BsonType.STRING),
                    IndexDefinition.create("status-index", "status", BsonType.STRING)
            );

            // Test: AND(name="john", age=25, status="active") - age has no index
            String query = "{ $and: [" +
                    "{ \"name\": \"john\" }, " +
                    "{ \"age\": 25 }, " +
                    "{ \"status\": \"active\" }" +
                    "] }";
            PhysicalNode optimized = planAndOptimize(query);

            // Should be AND(PhysicalIndexIntersection, PhysicalFullScan)
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd and = (PhysicalAnd) optimized;
            assertEquals(2, and.children().size());

            // Should have PhysicalIndexIntersection for name and status only
            boolean hasIntersection = and.children().stream()
                    .anyMatch(child -> child instanceof PhysicalIndexIntersection intersection &&
                            intersection.indexes().size() == 2 &&
                            intersection.indexes().stream().anyMatch(idx -> "name".equals(idx.selector())) &&
                            intersection.indexes().stream().anyMatch(idx -> "status".equals(idx.selector())));
            assertTrue(hasIntersection, "Should contain PhysicalIndexIntersection for indexed fields only");

            // Should have PhysicalFullScan for age (no index)
            boolean hasAgeFullScan = and.children().stream()
                    .anyMatch(child -> child instanceof PhysicalFullScan fullScan &&
                            fullScan.node() instanceof PhysicalFilter filter &&
                            "age".equals(filter.selector()));
            assertTrue(hasAgeFullScan, "Should contain PhysicalFullScan for non-indexed field");
        }
    }

    @Nested
    @DisplayName("Nested Structure Index Intersection")
    class NestedStructureTests {

        @Test
        @DisplayName("Should create intersection in nested AND operations")
        void shouldCreateIntersectionInNestedAndOperations() {
            // Create indexes
            createIndexes(
                    IndexDefinition.create("name-index", "name", BsonType.STRING),
                    IndexDefinition.create("age-index", "age", BsonType.INT32)
            );

            // Create manual nested structure: AND(OR(status="active"), AND(name="john", age=25))
            PhysicalFilter statusFilter = createFilter("status", Operator.EQ, "active");
            PhysicalOr statusOr = createOr(statusFilter);

            PhysicalFilter nameFilter = createFilter("name", Operator.EQ, "john");
            PhysicalFilter ageFilter = createFilter("age", Operator.EQ, 25);
            PhysicalIndexScan nameScan = createIndexScan(nameFilter);
            PhysicalIndexScan ageScan = createIndexScan(ageFilter);
            PhysicalAnd innerAnd = createAnd(nameScan, ageScan);

            PhysicalAnd topLevel = createAnd(statusOr, innerAnd);

            PhysicalNode optimized = optimize(topLevel);

            // Should create intersection for the nested indexed conditions
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd result = (PhysicalAnd) optimized;

            // Should contain a PhysicalIndexIntersection
            boolean hasIntersection = result.children().stream()
                    .anyMatch(child -> child instanceof PhysicalIndexIntersection intersection &&
                            intersection.indexes().size() == 2);
            assertTrue(hasIntersection, "Should contain PhysicalIndexIntersection for nested indexed conditions");
        }

        @Test
        @DisplayName("Should handle intersection with NOT operations")
        void shouldHandleIntersectionWithNotOperations() {
            // Create indexes
            createIndexes(
                    IndexDefinition.create("name-index", "name", BsonType.STRING),
                    IndexDefinition.create("age-index", "age", BsonType.INT32)
            );

            // Create: NOT(AND(name="john", age=25))
            PhysicalFilter nameFilter = createFilter("name", Operator.EQ, "john");
            PhysicalFilter ageFilter = createFilter("age", Operator.EQ, 25);
            PhysicalIndexScan nameScan = createIndexScan(nameFilter);
            PhysicalIndexScan ageScan = createIndexScan(ageFilter);
            PhysicalAnd innerAnd = createAnd(nameScan, ageScan);
            PhysicalNot notOperation = new PhysicalNot(1, innerAnd);

            PhysicalNode optimized = optimize(notOperation);

            // Should create intersection inside NOT: NOT(PhysicalIndexIntersection)
            assertInstanceOf(PhysicalNot.class, optimized);
            PhysicalNot result = (PhysicalNot) optimized;
            assertInstanceOf(PhysicalIndexIntersection.class, result.child());

            PhysicalIndexIntersection intersection = (PhysicalIndexIntersection) result.child();
            assertEquals(2, intersection.indexes().size());
        }
    }

    @Nested
    @DisplayName("Performance and Optimization Verification")
    class PerformanceVerificationTests {

        @Test
        @DisplayName("Should reduce number of index scans through intersection")
        void shouldReduceNumberOfIndexScansThoughIntersection() {
            // Create indexes for multiple fields
            createIndexes(
                    IndexDefinition.create("name-index", "name", BsonType.STRING),
                    IndexDefinition.create("age-index", "age", BsonType.INT32),
                    IndexDefinition.create("status-index", "status", BsonType.STRING)
            );

            // Test: AND(name="john", age=25, status="active")
            String query = "{ $and: [" +
                    "{ \"name\": \"john\" }, " +
                    "{ \"age\": 25 }, " +
                    "{ \"status\": \"active\" }" +
                    "] }";

            PhysicalNode unoptimized = planWithoutOptimization(query);
            PhysicalNode optimized = planAndOptimize(query);

            // Count PhysicalIndexScan nodes
            int unoptimizedIndexScans = countNodeType(unoptimized, PhysicalIndexScan.class);
            int optimizedIndexScans = countNodeType(optimized, PhysicalIndexScan.class);

            // Should reduce from 3 index scans to 0 (replaced by 1 intersection)
            assertEquals(3, unoptimizedIndexScans);
            assertEquals(0, optimizedIndexScans);

            // Should have exactly 1 PhysicalIndexIntersection
            int intersectionCount = countNodeType(optimized, PhysicalIndexIntersection.class);
            assertEquals(1, intersectionCount);
        }

        @Test
        @DisplayName("Should preserve plan structure when intersection not beneficial")
        void shouldPreservePlanStructureWhenIntersectionNotBeneficial() {
            // Create indexes
            createIndexes(
                    IndexDefinition.create("age-index", "age", BsonType.INT32),
                    IndexDefinition.create("score-index", "score", BsonType.DOUBLE)
            );

            // Test: AND(age > 18, score < 100) - no EQ conditions for intersection
            String query = "{ $and: [{ \"age\": { $gt: 18 } }, { \"score\": { $lt: 100 } }] }";

            PhysicalNode unoptimized = planWithoutOptimization(query);
            PhysicalNode optimized = planAndOptimize(query);

            // Structure should be preserved (intersection not applicable)
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd optimizedAnd = (PhysicalAnd) optimized;
            assertEquals(2, optimizedAnd.children().size());

            // Should not have any PhysicalIndexIntersection nodes
            int intersectionCount = countNodeType(optimized, PhysicalIndexIntersection.class);
            assertEquals(0, intersectionCount);

            // Should still have PhysicalIndexScan nodes
            int indexScanCount = countNodeType(optimized, PhysicalIndexScan.class);
            assertEquals(2, indexScanCount);
        }
    }
}