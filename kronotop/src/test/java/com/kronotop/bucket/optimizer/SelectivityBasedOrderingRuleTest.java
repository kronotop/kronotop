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
 * Test suite for SelectivityBasedOrderingRule.
 * Tests the rule's ability to reorder conditions based on estimated selectivity.
 */
@DisplayName("SelectivityBasedOrderingRule Tests")
public class SelectivityBasedOrderingRuleTest extends BaseOptimizerTest {

    private final SelectivityBasedOrderingRule rule = new SelectivityBasedOrderingRule();

    @Nested
    @DisplayName("Basic Functionality Tests")
    class BasicFunctionalityTests {

        @Test
        @DisplayName("Should have correct rule name and priority")
        void shouldHaveCorrectRuleNameAndPriority() {
            assertEquals("SelectivityBasedOrdering", rule.getName());
            assertEquals(60, rule.getPriority()); // Medium priority
        }

        @Test
        @DisplayName("Should apply to AND nodes with multiple children")
        void shouldApplyToAndNodesWithMultipleChildren() {
            PhysicalFilter filter1 = createFilter("name", Operator.EQ, "john");
            PhysicalFilter filter2 = createFilter("age", Operator.GT, 25);
            PhysicalAnd and = createAnd(filter1, filter2);

            assertTrue(rule.canApply(and));
        }

        @Test
        @DisplayName("Should apply to OR nodes with multiple children")
        void shouldApplyToOrNodesWithMultipleChildren() {
            PhysicalFilter filter1 = createFilter("name", Operator.EQ, "john");
            PhysicalFilter filter2 = createFilter("age", Operator.GT, 25);
            PhysicalOr or = createOr(filter1, filter2);

            assertTrue(rule.canApply(or));
        }

        @Test
        @DisplayName("Should not apply to single child nodes")
        void shouldNotApplyToSingleChildNodes() {
            PhysicalFilter filter = createFilter("name", Operator.EQ, "john");
            PhysicalAnd singleAnd = createAnd(filter);
            PhysicalOr singleOr = createOr(filter);

            assertFalse(rule.canApply(singleAnd));
            assertFalse(rule.canApply(singleOr));
        }

        @Test
        @DisplayName("Should apply to NOT operations")
        void shouldApplyToNotOperations() {
            PhysicalFilter filter = createFilter("name", Operator.EQ, "john");
            PhysicalNot not = new PhysicalNot(1, filter);

            assertTrue(rule.canApply(not));
        }
    }

    @Nested
    @DisplayName("AND Operation Ordering Tests")
    class AndOperationOrderingTests {

        @Test
        @DisplayName("Should order AND conditions from most to least selective")
        void shouldOrderAndConditionsFromMostToLeastSelective() {
            // Create indexes to make some conditions more selective
            createIndexes(
                    IndexDefinition.create("name-index", "name", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("email-index", "email", BsonType.STRING, SortOrder.ASCENDING)
            );

            // Create conditions with different selectivity:
            // 1. Basic filter (no index scan - least selective)
            PhysicalFilter basicFilter = createFilter("description", Operator.NE, "test");

            // 2. Equality filter (moderately selective)
            PhysicalFilter eqFilter = createFilter("status", Operator.EQ, "active");

            // 3. Index scan with equality (most selective)
            PhysicalIndexScan indexScan = createIndexScan(createFilter("name", Operator.EQ, "john"));

            // Create AND in suboptimal order (least to most selective)
            PhysicalAnd originalAnd = createAnd(basicFilter, eqFilter, indexScan);

            PhysicalNode optimized = rule.apply(originalAnd, metadata, new PlannerContext());

            // Should reorder to: indexScan, eqFilter, basicFilter (most to least selective)
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd result = (PhysicalAnd) optimized;
            assertEquals(3, result.children().size());

            // Most selective should be first (IndexScan with EQ)
            assertInstanceOf(PhysicalIndexScan.class, result.children().get(0));
        }

        @Test
        @DisplayName("Should handle mixed node types in AND operations")
        void shouldHandleMixedNodeTypesInAndOperations() {
            // Create different types of nodes with varying selectivity
            createIndex(IndexDefinition.create("name-index", "name", BsonType.STRING, SortOrder.ASCENDING));

            PhysicalFilter simpleFilter = createFilter("category", Operator.EQ, "books");
            PhysicalIndexScan indexScan = createIndexScan(createFilter("name", Operator.EQ, "john"));
            PhysicalFilter rangeFilter = createFilter("score", Operator.GT, 80);

            PhysicalAnd originalAnd = createAnd(simpleFilter, rangeFilter, indexScan);
            PhysicalNode optimized = rule.apply(originalAnd, metadata, new PlannerContext());

            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd result = (PhysicalAnd) optimized;

            // IndexScan should be first (most selective)
            assertInstanceOf(PhysicalIndexScan.class, result.children().get(0));
        }

        @Test
        @DisplayName("Should handle AND with nested structures")
        void shouldHandleAndWithNestedStructures() {
            createIndex(IndexDefinition.create("name-index", "name", BsonType.STRING, SortOrder.ASCENDING));

            // Create nested OR (less selective than individual conditions)
            PhysicalFilter filter1 = createFilter("status", Operator.EQ, "active");
            PhysicalFilter filter2 = createFilter("status", Operator.EQ, "pending");
            PhysicalOr nestedOr = createOr(filter1, filter2);

            // Create highly selective index scan
            PhysicalIndexScan indexScan = createIndexScan(createFilter("name", Operator.EQ, "john"));

            PhysicalAnd originalAnd = createAnd(nestedOr, indexScan);
            PhysicalNode optimized = rule.apply(originalAnd, metadata, new PlannerContext());

            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd result = (PhysicalAnd) optimized;

            // IndexScan should come first (more selective than OR)
            assertInstanceOf(PhysicalIndexScan.class, result.children().get(0));
            assertInstanceOf(PhysicalOr.class, result.children().get(1));
        }
    }

    @Nested
    @DisplayName("OR Operation Ordering Tests")
    class OrOperationOrderingTests {

        @Test
        @DisplayName("Should order OR conditions from least to most selective")
        void shouldOrderOrConditionsFromLeastToMostSelective() {
            createIndex(IndexDefinition.create("name-index", "name", BsonType.STRING, SortOrder.ASCENDING));

            // Create conditions with different selectivity
            PhysicalIndexScan indexScan = createIndexScan(createFilter("name", Operator.EQ, "john")); // Most selective
            PhysicalFilter eqFilter = createFilter("status", Operator.EQ, "active"); // Moderate
            PhysicalFilter neFilter = createFilter("desc", Operator.NE, "test"); // Least selective

            // Create OR in suboptimal order (most to least selective)
            PhysicalOr originalOr = createOr(indexScan, eqFilter, neFilter);

            PhysicalNode optimized = rule.apply(originalOr, metadata, new PlannerContext());

            // Should reorder to: neFilter, eqFilter, indexScan (least to most selective)
            assertInstanceOf(PhysicalOr.class, optimized);
            PhysicalOr result = (PhysicalOr) optimized;
            assertEquals(3, result.children().size());

            // Least selective should be first for OR (better short-circuiting)
            PhysicalFilter firstChild = (PhysicalFilter) result.children().get(0);
            assertEquals(Operator.NE, firstChild.op());

            // Most selective should be last
            assertInstanceOf(PhysicalIndexScan.class, result.children().get(2));
        }

        @Test
        @DisplayName("Should handle OR with equality vs range operators")
        void shouldHandleOrWithEqualityVsRangeOperators() {
            createIndex(IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING));

            // Equality is more selective than range
            PhysicalIndexScan eqScan = createIndexScan(createFilter("age", Operator.EQ, 25));
            PhysicalIndexScan rangeScan = createIndexScan(createFilter("age", Operator.GT, 18));

            PhysicalOr originalOr = createOr(eqScan, rangeScan);
            PhysicalNode optimized = rule.apply(originalOr, metadata, new PlannerContext());

            assertInstanceOf(PhysicalOr.class, optimized);
            PhysicalOr result = (PhysicalOr) optimized;

            // For OR: less selective (range) should come first
            PhysicalIndexScan firstChild = (PhysicalIndexScan) result.children().get(0);
            PhysicalFilter firstFilter = (PhysicalFilter) firstChild.node();
            assertEquals(Operator.GT, firstFilter.op()); // Range operator first

            PhysicalIndexScan secondChild = (PhysicalIndexScan) result.children().get(1);
            PhysicalFilter secondFilter = (PhysicalFilter) secondChild.node();
            assertEquals(Operator.EQ, secondFilter.op()); // Equality operator last
        }
    }

    @Nested
    @DisplayName("Complex Structure Tests")
    class ComplexStructureTests {

        @Test
        @DisplayName("Should handle nested AND/OR combinations")
        void shouldHandleNestedAndOrCombinations() {
            createIndexes(
                    IndexDefinition.create("name-index", "name", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING)
            );

            // Create complex nested structure
            PhysicalIndexScan nameEq = createIndexScan(createFilter("name", Operator.EQ, "john"));
            PhysicalFilter statusCheck = createFilter("status", Operator.EQ, "active");

            PhysicalOr innerOr = createOr(statusCheck, nameEq); // OR should order: statusCheck, nameEq
            PhysicalIndexScan ageRange = createIndexScan(createFilter("age", Operator.GT, 18));

            PhysicalAnd outerAnd = createAnd(innerOr, ageRange); // AND should order: ageRange, innerOr

            PhysicalNode optimized = rule.apply(outerAnd, metadata, new PlannerContext());

            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd result = (PhysicalAnd) optimized;
            assertEquals(2, result.children().size());

            // For AND: more selective IndexScan should come first
            assertInstanceOf(PhysicalIndexScan.class, result.children().get(0));
            assertInstanceOf(PhysicalOr.class, result.children().get(1));
        }

        @Test
        @DisplayName("Should recursively optimize nested structures")
        void shouldRecursivelyOptimizeNestedStructures() {
            createIndex(IndexDefinition.create("name-index", "name", BsonType.STRING, SortOrder.ASCENDING));

            // Create deeply nested structure that needs recursive optimization
            PhysicalFilter rangeFilter = createFilter("desc", Operator.GT, 100);
            PhysicalIndexScan indexScan1 = createIndexScan(createFilter("name", Operator.EQ, "john"));
            PhysicalAnd innerAnd = createAnd(rangeFilter, indexScan1); // Should reorder to: indexScan1, rangeFilter

            PhysicalFilter eqFilter = createFilter("status", Operator.EQ, "active");
            PhysicalAnd outerAnd = createAnd(eqFilter, innerAnd); // Should consider selectivity of inner AND

            PhysicalNode optimized = rule.apply(outerAnd, metadata, new PlannerContext());

            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd result = (PhysicalAnd) optimized;
            assertEquals(2, result.children().size());

            // Inner AND should be more selective (contains IndexScan) and come first
            assertInstanceOf(PhysicalAnd.class, result.children().get(0));
            PhysicalAnd innerResult = (PhysicalAnd) result.children().get(0);

            // Within inner AND, IndexScan should come first
            assertInstanceOf(PhysicalIndexScan.class, innerResult.children().get(0));
            assertInstanceOf(PhysicalFilter.class, innerResult.children().get(1));
        }
    }

    @Nested
    @DisplayName("Edge Case Tests")
    class EdgeCaseTests {

        @Test
        @DisplayName("Should handle single child AND/OR gracefully")
        void shouldHandleSingleChildAndOrGracefully() {
            PhysicalFilter filter = createFilter("name", Operator.EQ, "john");
            PhysicalAnd singleAnd = createAnd(filter);
            PhysicalOr singleOr = createOr(filter);

            PhysicalNode optimizedAnd = rule.apply(singleAnd, metadata, new PlannerContext());
            PhysicalNode optimizedOr = rule.apply(singleOr, metadata, new PlannerContext());

            // Should return the single child directly
            assertEquals(filter, optimizedAnd);
            assertEquals(filter, optimizedOr);
        }

        @Test
        @DisplayName("Should handle nodes with no selectivity difference")
        void shouldHandleNodesWithNoSelectivityDifference() {
            // Create multiple filters with same selectivity
            PhysicalFilter filter1 = createFilter("name", Operator.EQ, "john");
            PhysicalFilter filter2 = createFilter("email", Operator.EQ, "john@example.com");
            PhysicalFilter filter3 = createFilter("status", Operator.EQ, "active");

            PhysicalAnd and = createAnd(filter1, filter2, filter3);
            PhysicalNode optimized = rule.apply(and, metadata, new PlannerContext());

            // Should still create valid AND node (order may vary but structure should be preserved)
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd result = (PhysicalAnd) optimized;
            assertEquals(3, result.children().size());
        }

        @Test
        @DisplayName("Should handle NOT operations correctly")
        void shouldHandleNotOperationsCorrectly() {
            PhysicalFilter filter = createFilter("name", Operator.EQ, "john");
            PhysicalNot not = new PhysicalNot(1, filter);

            PhysicalNode optimized = rule.apply(not, metadata, new PlannerContext());

            // Should recursively optimize the child
            assertInstanceOf(PhysicalNot.class, optimized);
            PhysicalNot result = (PhysicalNot) optimized;
            assertEquals(filter, result.child());
        }
    }

    @Nested
    @DisplayName("Selectivity Estimation Tests")
    class SelectivityEstimationTests {

        @Test
        @DisplayName("Should prefer indexed conditions over regular filters")
        void shouldPreferIndexedConditionsOverRegularFilters() {
            createIndex(IndexDefinition.create("name-index", "name", BsonType.STRING, SortOrder.ASCENDING));

            PhysicalIndexScan indexScan = createIndexScan(createFilter("name", Operator.EQ, "john"));
            PhysicalFilter regularFilter = createFilter("description", Operator.EQ, "test");

            PhysicalAnd and = createAnd(regularFilter, indexScan);
            PhysicalNode optimized = rule.apply(and, metadata, new PlannerContext());

            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd result = (PhysicalAnd) optimized;

            // IndexScan should come first
            assertInstanceOf(PhysicalIndexScan.class, result.children().get(0));
            assertInstanceOf(PhysicalFilter.class, result.children().get(1));
        }

        @Test
        @DisplayName("Should prefer equality over range operations")
        void shouldPreferEqualityOverRangeOperations() {
            createIndex(IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING));

            PhysicalIndexScan rangeScan = createIndexScan(createFilter("age", Operator.GT, 18));
            PhysicalIndexScan eqScan = createIndexScan(createFilter("age", Operator.EQ, 25));

            PhysicalAnd and = createAnd(rangeScan, eqScan);
            PhysicalNode optimized = rule.apply(and, metadata, new PlannerContext());

            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd result = (PhysicalAnd) optimized;

            // Equality scan should come first (more selective)
            PhysicalIndexScan firstScan = (PhysicalIndexScan) result.children().get(0);
            PhysicalFilter firstFilter = (PhysicalFilter) firstScan.node();
            assertEquals(Operator.EQ, firstFilter.op());
        }
    }
}