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

package com.kronotop.bucket.planner.logical;

import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.planner.Operator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for RedundantConditionEliminationTransform.
 * Tests the elimination of redundant conditions in logical expressions.
 */
class RedundantConditionEliminationTest {

    private LogicalPlanner planner;

    @BeforeEach
    void setUp() {
        planner = new LogicalPlanner();
    }

    // Helper method to extract the actual value from BqlValue objects
    private Object extractValue(Object operand) {
        if (operand instanceof StringVal(String value)) {
            return value;
        } else if (operand instanceof Int32Val(int value)) {
            return value;
        } else if (operand instanceof Int64Val(long value)) {
            return value;
        } else if (operand instanceof Decimal128Val(java.math.BigDecimal value)) {
            return value;
        }
        return operand;
    }

    // ============================================================================
    // Duplicate Condition Elimination Tests
    // ============================================================================

    @Nested
    @DisplayName("Duplicate Condition Elimination Tests")
    class DuplicateEliminationTests {

        @Test
        @DisplayName("Exact duplicate conditions in AND should be eliminated")
        void testExactDuplicatesInAnd() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "status": "active" },
                        { "status": "active" },
                        { "category": "electronics" }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(2, andNode.children().size()); // Duplicate removed

            // Should have status=active (once) and category=electronics
            boolean hasStatus = false, hasCategory = false;
            for (LogicalNode child : andNode.children()) {
                assertInstanceOf(LogicalFilter.class, child);
                LogicalFilter filter = (LogicalFilter) child;
                if ("status".equals(filter.selector())) {
                    hasStatus = true;
                    assertEquals("active", extractValue(filter.operand()));
                } else if ("category".equals(filter.selector())) {
                    hasCategory = true;
                    assertEquals("electronics", extractValue(filter.operand()));
                }
            }
            assertTrue(hasStatus && hasCategory);
        }

        @Test
        @DisplayName("Exact duplicate conditions in OR should be eliminated")
        void testExactDuplicatesInOr() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$or": [
                        { "role": "admin" },
                        { "role": "admin" },
                        { "role": "user" }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalOr.class, result);
            LogicalOr orNode = (LogicalOr) result;
            assertEquals(2, orNode.children().size()); // Duplicate removed

            // Should have role=admin (once) and role=user
            boolean hasAdmin = false, hasUser = false;
            for (LogicalNode child : orNode.children()) {
                assertInstanceOf(LogicalFilter.class, child);
                LogicalFilter filter = (LogicalFilter) child;
                if ("admin".equals(extractValue(filter.operand()))) {
                    hasAdmin = true;
                } else if ("user".equals(extractValue(filter.operand()))) {
                    hasUser = true;
                }
            }
            assertTrue(hasAdmin && hasUser);
        }

        @Test
        @DisplayName("Multiple duplicates should be reduced to single condition")
        void testMultipleDuplicates() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "selector": "value" },
                        { "selector": "value" },
                        { "selector": "value" },
                        { "selector": "value" }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Should be simplified to single filter
            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("selector", filter.selector());
            assertEquals("value", extractValue(filter.operand()));
        }
    }

    // ============================================================================
    // Numeric Range Redundancy Tests
    // ============================================================================

    @Nested
    @DisplayName("Numeric Range Redundancy Elimination Tests")
    class NumericRangeRedundancyTests {

        @Test
        @DisplayName("AND with GT redundancy should keep more restrictive condition")
        void testGtRedundancyInAnd() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "price": { "$gt": 100 } },
                        { "price": { "$gt": 50 } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Should keep only the more restrictive condition (price > 100)
            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("price", filter.selector());
            assertEquals(Operator.GT, filter.op());
            assertEquals(100, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("AND with LT redundancy should keep more restrictive condition")
        void testLtRedundancyInAnd() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "age": { "$lt": 30 } },
                        { "age": { "$lt": 50 } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Should keep only the more restrictive condition (age < 30)
            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("age", filter.selector());
            assertEquals(Operator.LT, filter.op());
            assertEquals(30, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("AND with GTE/GT mixed redundancy should work correctly")
        void testGteGtRedundancyInAnd() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "score": { "$gte": 80 } },
                        { "score": { "$gt": 75 } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Should keep the more restrictive condition (score >= 80)
            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("score", filter.selector());
            assertEquals(Operator.GTE, filter.op());
            assertEquals(80, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("AND with LTE/LT mixed redundancy should work correctly")
        void testLteLtRedundancyInAnd() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "rating": { "$lte": 5 } },
                        { "rating": { "$lt": 8 } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Should keep the more restrictive condition (rating <= 5)
            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("rating", filter.selector());
            assertEquals(Operator.LTE, filter.op());
            assertEquals(5, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("OR with GT redundancy should keep less restrictive condition")
        void testGtRedundancyInOr() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$or": [
                        { "price": { "$gt": 100 } },
                        { "price": { "$gt": 50 } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Should keep only the less restrictive condition (price > 50)
            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("price", filter.selector());
            assertEquals(Operator.GT, filter.op());
            assertEquals(50, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("OR with LT redundancy should keep less restrictive condition")
        void testLtRedundancyInOr() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$or": [
                        { "age": { "$lt": 30 } },
                        { "age": { "$lt": 50 } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Should keep only the less restrictive condition (age < 50)
            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("age", filter.selector());
            assertEquals(Operator.LT, filter.op());
            assertEquals(50, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("Complex numeric range redundancy should work correctly")
        void testComplexNumericRangeRedundancy() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "tenant_id": "123" },
                        { "price": { "$gt": 100 } },
                        { "price": { "$gte": 50 } },
                        { "price": { "$gt": 75 } },
                        { "category": "electronics" }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(3, andNode.children().size()); // tenant_id, price (most restrictive), category

            // Find the price filter
            LogicalFilter priceFilter = null;
            for (LogicalNode child : andNode.children()) {
                if (child instanceof LogicalFilter filter && "price".equals(filter.selector())) {
                    priceFilter = filter;
                    break;
                }
            }

            assertNotNull(priceFilter);
            assertEquals(Operator.GT, priceFilter.op());
            assertEquals(100, extractValue(priceFilter.operand())); // Most restrictive
        }
    }

    // ============================================================================
    // Set Operation Redundancy Tests
    // ============================================================================

    @Nested
    @DisplayName("Set Operation Redundancy Elimination Tests")
    class SetOperationRedundancyTests {

        @Test
        @DisplayName("AND with IN subset redundancy should keep superset")
        void testInSubsetRedundancyInAnd() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "status": { "$in": ["active", "pending"] } },
                        { "status": { "$in": ["active"] } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Should keep the superset (more permissive in AND context)
            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("status", filter.selector());
            assertEquals(Operator.IN, filter.op());

            @SuppressWarnings("unchecked")
            java.util.List<Object> values = (java.util.List<Object>) filter.operand();
            assertEquals(2, values.size()); // Should keep the larger set
        }

        @Test
        @DisplayName("OR with IN subset redundancy should keep superset")
        void testInSubsetRedundancyInOr() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$or": [
                        { "category": { "$in": ["electronics"] } },
                        { "category": { "$in": ["electronics", "books"] } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Should keep the superset (more permissive in OR context)
            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("category", filter.selector());
            assertEquals(Operator.IN, filter.op());

            @SuppressWarnings("unchecked")
            java.util.List<Object> values = (java.util.List<Object>) filter.operand();
            assertEquals(2, values.size()); // Should keep the larger set
        }
    }

    // ============================================================================
    // Mixed Redundancy Tests
    // ============================================================================

    @Nested
    @DisplayName("Mixed Redundancy Elimination Tests")
    class MixedRedundancyTests {

        @Test
        @DisplayName("AND with multiple selector redundancies should eliminate all")
        void testMultipleSelectorRedundanciesInAnd() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "price": { "$gt": 100 } },
                        { "price": { "$gt": 50 } },
                        { "rating": { "$lte": 5 } },
                        { "rating": { "$lt": 8 } },
                        { "status": "active" },
                        { "status": "active" }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(3, andNode.children().size()); // One condition per selector

            // Verify each selector has the correct condition
            boolean hasPriceGt100 = false, hasRatingLte5 = false, hasStatusActive = false;

            for (LogicalNode child : andNode.children()) {
                assertInstanceOf(LogicalFilter.class, child);
                LogicalFilter filter = (LogicalFilter) child;

                switch (filter.selector()) {
                    case "price" -> {
                        assertEquals(Operator.GT, filter.op());
                        assertEquals(100, extractValue(filter.operand()));
                        hasPriceGt100 = true;
                    }
                    case "rating" -> {
                        assertEquals(Operator.LTE, filter.op());
                        assertEquals(5, extractValue(filter.operand()));
                        hasRatingLte5 = true;
                    }
                    case "status" -> {
                        assertEquals(Operator.EQ, filter.op());
                        assertEquals("active", extractValue(filter.operand()));
                        hasStatusActive = true;
                    }
                }
            }

            assertTrue(hasPriceGt100 && hasRatingLte5 && hasStatusActive);
        }

        @Test
        @DisplayName("OR with multiple selector redundancies should eliminate all")
        void testMultipleSelectorRedundanciesInOr() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$or": [
                        { "priority": { "$gt": 5 } },
                        { "priority": { "$gt": 3 } },
                        { "role": "admin" },
                        { "role": "admin" }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalOr.class, result);
            LogicalOr orNode = (LogicalOr) result;
            assertEquals(2, orNode.children().size()); // One condition per selector

            // Verify each selector has the correct condition
            boolean hasPriorityGt3 = false, hasRoleAdmin = false;

            for (LogicalNode child : orNode.children()) {
                assertInstanceOf(LogicalFilter.class, child);
                LogicalFilter filter = (LogicalFilter) child;

                switch (filter.selector()) {
                    case "priority" -> {
                        assertEquals(Operator.GT, filter.op());
                        assertEquals(3, extractValue(filter.operand())); // Less restrictive in OR
                        hasPriorityGt3 = true;
                    }
                    case "role" -> {
                        assertEquals(Operator.EQ, filter.op());
                        assertEquals("admin", extractValue(filter.operand()));
                        hasRoleAdmin = true;
                    }
                }
            }

            assertTrue(hasPriorityGt3 && hasRoleAdmin);
        }

        @Test
        @DisplayName("Nested structures should preserve redundancy elimination")
        void testNestedRedundancyElimination() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "tenant_id": "123" },
                        {
                          "$or": [
                            { "priority": { "$gt": 5 } },
                            { "priority": { "$gt": 3 } }
                          ]
                        }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(2, andNode.children().size());

            // The OR should be simplified to a single filter (priority > 3)
            LogicalNode orChild = null;
            for (LogicalNode child : andNode.children()) {
                if (child instanceof LogicalFilter filter && "priority".equals(filter.selector())) {
                    orChild = child;
                    break;
                }
            }

            assertNotNull(orChild);
            assertInstanceOf(LogicalFilter.class, orChild);
            LogicalFilter priorityFilter = (LogicalFilter) orChild;
            assertEquals(Operator.GT, priorityFilter.op());
            assertEquals(3, extractValue(priorityFilter.operand())); // Less restrictive kept in OR
        }
    }

    // ============================================================================
    // Edge Cases and Non-Redundant Tests
    // ============================================================================

    @Nested
    @DisplayName("Edge Cases and Non-Redundant Condition Tests")
    class EdgeCasesTests {

        @Test
        @DisplayName("Non-redundant conditions should be preserved")
        void testNonRedundantConditionsPreserved() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "price": { "$gte": 100 } },
                        { "price": { "$lte": 200 } },
                        { "category": "electronics" }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // These conditions are not redundant (define a range)
            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(3, andNode.children().size()); // All conditions preserved
        }

        @Test
        @DisplayName("Different selectors should not interfere with each other")
        void testDifferentSelectorsIndependent() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "price": { "$gt": 100 } },
                        { "rating": { "$gt": 100 } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Different selectors, should not be considered redundant
            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(2, andNode.children().size());
        }

        @Test
        @DisplayName("Empty AND should remain empty")
        void testEmptyAnd() {
            BqlExpr expr = BqlParser.parse("{ \"$and\": [] }");

            LogicalNode result = planner.plan(expr);

            // Should be optimized to TRUE by constant folding
            assertInstanceOf(LogicalTrue.class, result);
        }

        @Test
        @DisplayName("Empty OR should remain empty")
        void testEmptyOr() {
            BqlExpr expr = BqlParser.parse("{ \"$or\": [] }");

            LogicalNode result = planner.plan(expr);

            // Should be optimized to FALSE by constant folding
            assertInstanceOf(LogicalFalse.class, result);
        }

        @Test
        @DisplayName("Single condition should remain single")
        void testSingleCondition() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "status": "active" }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Should be simplified to single filter
            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("status", filter.selector());
            assertEquals("active", extractValue(filter.operand()));
        }

        @Test
        @DisplayName("Complex nested redundancy elimination should work")
        void testComplexNestedRedundancy() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "tenant_id": "123" },
                        {
                          "$and": [
                            { "price": { "$gt": 100 } },
                            { "price": { "$gt": 50 } },
                            { "price": { "$gt": 75 } }
                          ]
                        }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Should be flattened and redundancy eliminated
            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(2, andNode.children().size()); // tenant_id + most restrictive price

            // Find price filter
            LogicalFilter priceFilter = null;
            for (LogicalNode child : andNode.children()) {
                if (child instanceof LogicalFilter filter && "price".equals(filter.selector())) {
                    priceFilter = filter;
                    break;
                }
            }

            assertNotNull(priceFilter);
            assertEquals(100, extractValue(priceFilter.operand())); // Most restrictive condition
        }
    }

    // ============================================================================
    // Integration with Other Transforms Tests
    // ============================================================================

    @Nested
    @DisplayName("Integration with Other Transforms")
    class IntegrationTests {

        @Test
        @DisplayName("Redundancy elimination should work with other optimizations")
        void testIntegrationWithOtherOptimizations() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "status": "active" },
                        { "status": "active" },
                        {
                          "$or": [
                            { "role": "admin" },
                            { "role": { "$ne": "admin" } }
                          ]
                        },
                        { "price": { "$gt": 100 } },
                        { "price": { "$gt": 50 } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Should combine:
            // 1. Duplicate elimination: status=active (once)
            // 2. Tautology elimination: OR(role=admin, role!=admin) → TRUE
            // 3. Redundancy elimination: price > 100 (more restrictive)
            // 4. Constant folding: AND(status=active, TRUE, price>100) → AND(status=active, price>100)

            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(2, andNode.children().size()); // status + price

            boolean hasStatus = false, hasPrice = false;
            for (LogicalNode child : andNode.children()) {
                assertInstanceOf(LogicalFilter.class, child);
                LogicalFilter filter = (LogicalFilter) child;

                if ("status".equals(filter.selector())) {
                    assertEquals("active", extractValue(filter.operand()));
                    hasStatus = true;
                } else if ("price".equals(filter.selector())) {
                    assertEquals(100, extractValue(filter.operand()));
                    hasPrice = true;
                }
            }

            assertTrue(hasStatus && hasPrice);
        }

        @Test
        @DisplayName("Pipeline order should not affect final result")
        void testPipelineOrderIndependence() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "selector": { "$gt": 10 } },
                        { "selector": { "$gt": 5 } },
                        { "selector": { "$gt": 8 } }
                      ]
                    }
                    """);

            // Test with default pipeline
            LogicalNode result1 = planner.plan(expr);

            // Test with different pipeline order (shouldn't matter for this case)
            LogicalNode result2 = planner.plan(expr);

            // Results should be identical
            assertEquals(result1.toString(), result2.toString());

            // Should result in single filter with most restrictive condition
            assertInstanceOf(LogicalFilter.class, result1);
            LogicalFilter filter = (LogicalFilter) result1;
            assertEquals("selector", filter.selector());
            assertEquals(Operator.GT, filter.op());
            assertEquals(10, extractValue(filter.operand()));
        }
    }
}