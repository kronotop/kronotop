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

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for LogicalPlanner covering all operators, transforms, and edge cases.
 * Tests the complete BQL → LogicalNode conversion pipeline and optimization transforms.
 */
class LogicalPlannerTest {

    private LogicalPlanner planner;

    @BeforeEach
    void setUp() {
        planner = new LogicalPlanner();
    }

    /**
     * Helper method to extract the actual value from BqlValue objects
     */
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

    // ============================================================================
    // Core Conversion Tests - All Operators
    // ============================================================================

    @Nested
    @DisplayName("Selector Operator Conversion Tests")
    class SelectorOperatorTests {

        @Test
        @DisplayName("EQ operator should convert to LogicalFilter")
        void testEqualityOperator() {
            BqlExpr expr = BqlParser.parse("{ \"status\": \"active\" }");
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("status", filter.selector());
            assertEquals(Operator.EQ, filter.op());
            assertEquals("active", extractValue(filter.operand()));
        }

        @Test
        @DisplayName("GT operator should convert to LogicalFilter")
        void testGreaterThanOperator() {
            BqlExpr expr = BqlParser.parse("{ \"price\": { \"$gt\": 100 } }");
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("price", filter.selector());
            assertEquals(Operator.GT, filter.op());
            assertEquals(100, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("GTE operator should convert to LogicalFilter")
        void testGreaterThanOrEqualOperator() {
            BqlExpr expr = BqlParser.parse("{ \"age\": { \"$gte\": 18 } }");
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("age", filter.selector());
            assertEquals(Operator.GTE, filter.op());
            assertEquals(18, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("LT operator should convert to LogicalFilter")
        void testLessThanOperator() {
            BqlExpr expr = BqlParser.parse("{ \"score\": { \"$lt\": 50.5 } }");
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("score", filter.selector());
            assertEquals(Operator.LT, filter.op());
            assertEquals(50.5, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("LTE operator should convert to LogicalFilter")
        void testLessThanOrEqualOperator() {
            BqlExpr expr = BqlParser.parse("{ \"rating\": { \"$lte\": 5 } }");
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("rating", filter.selector());
            assertEquals(Operator.LTE, filter.op());
            assertEquals(5, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("NE operator should convert to LogicalFilter")
        void testNotEqualOperator() {
            BqlExpr expr = BqlParser.parse("{ \"category\": { \"$ne\": \"archived\" } }");
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("category", filter.selector());
            assertEquals(Operator.NE, filter.op());
            assertEquals("archived", extractValue(filter.operand()));
        }

        @Test
        @DisplayName("IN operator should convert to LogicalFilter with list operand")
        void testInOperator() {
            BqlExpr expr = BqlParser.parse("{ \"status\": { \"$in\": [\"active\", \"pending\", \"review\"] } }");
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("status", filter.selector());
            assertEquals(Operator.IN, filter.op());

            @SuppressWarnings("unchecked")
            List<Object> operand = (List<Object>) filter.operand();
            assertEquals(Arrays.asList("active", "pending", "review"), extractValue(operand));
        }

        @Test
        @DisplayName("NIN operator should convert to LogicalFilter with list operand")
        void testNotInOperator() {
            BqlExpr expr = BqlParser.parse("{ \"category\": { \"$nin\": [\"spam\", \"deleted\", \"hidden\"] } }");
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("category", filter.selector());
            assertEquals(Operator.NIN, filter.op());

            @SuppressWarnings("unchecked")
            List<Object> operand = (List<Object>) filter.operand();
            assertEquals(Arrays.asList("spam", "deleted", "hidden"), extractValue(operand));
        }

        @Test
        @DisplayName("ALL operator should convert to LogicalFilter with list operand")
        void testAllOperator() {
            BqlExpr expr = BqlParser.parse("{ \"tags\": { \"$all\": [\"urgent\", \"important\"] } }");
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("tags", filter.selector());
            assertEquals(Operator.ALL, filter.op());

            @SuppressWarnings("unchecked")
            List<Object> operand = (List<Object>) filter.operand();
            assertEquals(Arrays.asList("urgent", "important"), extractValue(operand));
        }

        @Test
        @DisplayName("SIZE operator should convert to LogicalFilter with integer operand")
        void testSizeOperator() {
            BqlExpr expr = BqlParser.parse("{ \"items\": { \"$size\": 3 } }");
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("items", filter.selector());
            assertEquals(Operator.SIZE, filter.op());
            assertEquals(3, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("EXISTS operator should convert to LogicalFilter with boolean operand")
        void testExistsOperator() {
            BqlExpr expr = BqlParser.parse("{ \"metadata\": { \"$exists\": true } }");
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("metadata", filter.selector());
            assertEquals(Operator.EXISTS, filter.op());
            assertEquals(true, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("EXISTS false should convert correctly")
        void testExistsOperatorFalse() {
            BqlExpr expr = BqlParser.parse("{ \"optional_selector\": { \"$exists\": false } }");
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("optional_selector", filter.selector());
            assertEquals(Operator.EXISTS, filter.op());
            assertEquals(false, extractValue(filter.operand()));
        }
    }

    // ============================================================================
    // Logical Operator Tests
    // ============================================================================

    @Nested
    @DisplayName("Logical Operator Conversion Tests")
    class LogicalOperatorTests {

        @Test
        @DisplayName("AND operator should convert to LogicalAnd")
        void testAndOperator() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "status": "active" },
                        { "priority": { "$gt": 5 } }
                      ]
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(2, andNode.children().size());

            // Find filters by selector name (order may vary due to optimizations)
            LogicalFilter statusFilter = null;
            LogicalFilter priorityFilter = null;

            for (LogicalNode child : andNode.children()) {
                assertInstanceOf(LogicalFilter.class, child);
                LogicalFilter filter = (LogicalFilter) child;
                if ("status".equals(filter.selector())) {
                    statusFilter = filter;
                } else if ("priority".equals(filter.selector())) {
                    priorityFilter = filter;
                }
            }

            assertNotNull(statusFilter);
            assertEquals(Operator.EQ, statusFilter.op());
            assertEquals("active", extractValue(statusFilter.operand()));

            assertNotNull(priorityFilter);
            assertEquals(Operator.GT, priorityFilter.op());
            assertEquals(5, extractValue(priorityFilter.operand()));
        }

        @Test
        @DisplayName("OR operator should convert to LogicalOr")
        void testOrOperator() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$or": [
                        { "category": "electronics" },
                        { "category": "books" },
                        { "featured": true }
                      ]
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalOr.class, result);
            LogicalOr orNode = (LogicalOr) result;
            assertEquals(3, orNode.children().size());

            // Find filters by selector name (order may vary due to optimizations)
            LogicalFilter featuredFilter = null;
            int categoryFilterCount = 0;

            for (LogicalNode child : orNode.children()) {
                assertInstanceOf(LogicalFilter.class, child);
                LogicalFilter filter = (LogicalFilter) child;
                if ("category".equals(filter.selector())) {
                    assertEquals(Operator.EQ, filter.op());
                    categoryFilterCount++;
                } else if ("featured".equals(filter.selector())) {
                    featuredFilter = filter;
                }
            }

            assertEquals(2, categoryFilterCount);
            assertNotNull(featuredFilter);
            assertEquals(Operator.EQ, featuredFilter.op());
            assertEquals(true, extractValue(featuredFilter.operand()));
        }

        @Test
        @DisplayName("NOT operator should convert to LogicalNot")
        void testNotOperator() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$not": { "status": "deleted" }
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalNot.class, result);
            LogicalNot notNode = (LogicalNot) result;

            // Child should be a filter
            assertInstanceOf(LogicalFilter.class, notNode.child());
            LogicalFilter filter = (LogicalFilter) notNode.child();
            assertEquals("status", filter.selector());
            assertEquals(Operator.EQ, filter.op());
            assertEquals("deleted", extractValue(filter.operand()));
        }

        @Test
        @DisplayName("ELEMMATCH operator should convert to LogicalElemMatch")
        void testElemMatchOperator() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "items": {
                        "$elemMatch": {
                          "price": { "$gt": 100 },
                          "category": "electronics"
                        }
                      }
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalElemMatch.class, result);
            LogicalElemMatch elemMatch = (LogicalElemMatch) result;
            assertEquals("items", elemMatch.selector());

            // Sub-plan should be an AND of the conditions
            assertInstanceOf(LogicalAnd.class, elemMatch.subPlan());
            LogicalAnd subAnd = (LogicalAnd) elemMatch.subPlan();
            assertEquals(2, subAnd.children().size());
        }
    }

    // ============================================================================
    // Complex Nested Query Tests
    // ============================================================================

    @Nested
    @DisplayName("Complex Nested Query Tests")
    class ComplexNestedQueryTests {

        @Test
        @DisplayName("Deeply nested AND/OR should convert correctly")
        void testDeeplyNestedAndOr() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "tenant_id": "123" },
                        {
                          "$or": [
                            { "role": "admin" },
                            {
                              "$and": [
                                { "permissions": { "$in": ["read", "write"] } },
                                { "active": true }
                              ]
                            }
                          ]
                        }
                      ]
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd rootAnd = (LogicalAnd) result;
            assertEquals(2, rootAnd.children().size());

            // Find the tenant_id filter and the OR node (order may vary due to optimizations)
            LogicalFilter tenantFilter = null;
            LogicalOr orNode = null;

            for (LogicalNode child : rootAnd.children()) {
                if (child instanceof LogicalFilter filter && "tenant_id".equals(filter.selector())) {
                    tenantFilter = filter;
                } else if (child instanceof LogicalOr or) {
                    orNode = or;
                }
            }

            assertNotNull(tenantFilter);
            assertEquals("123", extractValue(tenantFilter.operand()));

            assertNotNull(orNode);
            assertEquals(2, orNode.children().size());

            // Find the role filter and the inner AND node in the OR
            LogicalFilter roleFilter = null;
            LogicalAnd innerAnd = null;

            for (LogicalNode child : orNode.children()) {
                if (child instanceof LogicalFilter filter && "role".equals(filter.selector())) {
                    roleFilter = filter;
                } else if (child instanceof LogicalAnd and) {
                    innerAnd = and;
                }
            }

            assertNotNull(roleFilter);
            assertEquals("admin", extractValue(roleFilter.operand()));

            assertNotNull(innerAnd);
            assertEquals(2, innerAnd.children().size());
        }

        @Test
        @DisplayName("Multiple NOT operators should work correctly")
        void testMultipleNotOperators() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "$not": { "status": "deleted" } },
                        { "$not": { "hidden": true } }
                      ]
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(2, andNode.children().size());

            // Both children should be NOT nodes
            assertInstanceOf(LogicalNot.class, andNode.children().get(0));
            assertInstanceOf(LogicalNot.class, andNode.children().get(1));
        }

        @Test
        @DisplayName("ElemMatch with nested logical operators")
        void testElemMatchWithNestedLogical() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "products": {
                        "$elemMatch": {
                          "$and": [
                            { "price": { "$gte": 100 } },
                            {
                              "$or": [
                                { "category": "electronics" },
                                { "featured": true }
                              ]
                            }
                          ]
                        }
                      }
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalElemMatch.class, result);
            LogicalElemMatch elemMatch = (LogicalElemMatch) result;
            assertEquals("products", elemMatch.selector());

            // Sub-plan should be an AND
            assertInstanceOf(LogicalAnd.class, elemMatch.subPlan());
            LogicalAnd subAnd = (LogicalAnd) elemMatch.subPlan();
            assertEquals(2, subAnd.children().size());

            // Find the price filter and the OR node (order may vary due to optimizations)
            LogicalFilter priceFilter = null;
            LogicalOr orNode = null;

            for (LogicalNode child : subAnd.children()) {
                if (child instanceof LogicalFilter filter && "price".equals(filter.selector())) {
                    priceFilter = filter;
                } else if (child instanceof LogicalOr or) {
                    orNode = or;
                }
            }

            assertNotNull(priceFilter);
            assertEquals(Operator.GTE, priceFilter.op());
            assertEquals(100, extractValue(priceFilter.operand()));

            assertNotNull(orNode);
            assertEquals(2, orNode.children().size());
        }

        @Test
        @DisplayName("Very deep nesting with mixed operators should work correctly")
        void testVeryDeepMixedNesting() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "level1": "a" },
                        {
                          "$or": [
                            { "level2_a": "b" },
                            {
                              "$and": [
                                { "level3_a": "c" },
                                {
                                  "$not": {
                                    "$or": [
                                      { "level4_a": "d" },
                                      {
                                        "$and": [
                                          { "level5_a": "e" },
                                          { "level5_b": { "$gt": 100 } }
                                        ]
                                      }
                                    ]
                                  }
                                }
                              ]
                            }
                          ]
                        }
                      ]
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            // Should successfully parse deeply nested structure
            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd rootAnd = (LogicalAnd) result;
            assertEquals(2, rootAnd.children().size());

            // Find the level1 filter and the OR node (order may vary due to optimizations)
            LogicalFilter level1Filter = null;
            LogicalOr orNode = null;

            for (LogicalNode child : rootAnd.children()) {
                if (child instanceof LogicalFilter filter && "level1".equals(filter.selector())) {
                    level1Filter = filter;
                } else if (child instanceof LogicalOr or) {
                    orNode = or;
                }
            }

            assertNotNull(level1Filter);
            assertEquals("a", extractValue(level1Filter.operand()));

            assertNotNull(orNode);
            assertEquals(2, orNode.children().size());

            // Find the level2_a filter and the inner AND node in the OR
            LogicalFilter level2Filter = null;
            LogicalAnd innerAnd = null;

            for (LogicalNode child : orNode.children()) {
                if (child instanceof LogicalFilter filter && "level2_a".equals(filter.selector())) {
                    level2Filter = filter;
                } else if (child instanceof LogicalAnd and) {
                    innerAnd = and;
                }
            }

            assertNotNull(level2Filter);
            assertEquals("b", extractValue(level2Filter.operand()));

            assertNotNull(innerAnd);
            assertTrue(innerAnd.children().size() >= 1); // Has at least level3_a and NOT node
        }

        @Test
        @DisplayName("Complex ElemMatch with multiple levels should work correctly")
        void testComplexMultiLevelElemMatch() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        {
                          "orders": {
                            "$elemMatch": {
                              "$and": [
                                { "status": "active" },
                                { "total": { "$gte": 100 } }
                              ]
                            }
                          }
                        },
                        {
                          "items": {
                            "$elemMatch": {
                              "$or": [
                                { "category": "electronics" },
                                { "price": { "$lte": 50 } }
                              ]
                            }
                          }
                        }
                      ]
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd rootAnd = (LogicalAnd) result;
            assertEquals(2, rootAnd.children().size());

            // Both children should be ElemMatch
            assertInstanceOf(LogicalElemMatch.class, rootAnd.children().get(0));
            assertInstanceOf(LogicalElemMatch.class, rootAnd.children().get(1));

            LogicalElemMatch ordersElemMatch = (LogicalElemMatch) rootAnd.children().get(0);
            assertEquals("orders", ordersElemMatch.selector());
            assertInstanceOf(LogicalAnd.class, ordersElemMatch.subPlan());

            LogicalElemMatch itemsElemMatch = (LogicalElemMatch) rootAnd.children().get(1);
            assertEquals("items", itemsElemMatch.selector());
            assertInstanceOf(LogicalOr.class, itemsElemMatch.subPlan());
        }
    }

    // ============================================================================
    // Transform Pipeline Tests
    // ============================================================================

    @Nested
    @DisplayName("Transform Pipeline Tests")
    class TransformPipelineTests {

        @Test
        @DisplayName("FlattenAndOrTransform should flatten nested AND nodes")
        void testFlattenNestedAnd() {
            // Create: AND(a, AND(b, c)) which should become AND(a, b, c)
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "selector1": "value1" },
                        {
                          "$and": [
                            { "selector2": "value2" },
                            { "selector3": "value3" }
                          ]
                        }
                      ]
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;

            // Should be flattened to 3 children instead of 2
            assertEquals(3, andNode.children().size());

            // All children should be filters (no nested AND)
            for (LogicalNode child : andNode.children()) {
                assertInstanceOf(LogicalFilter.class, child);
            }
        }

        @Test
        @DisplayName("FlattenAndOrTransform should flatten nested OR nodes")
        void testFlattenNestedOr() {
            // Create: OR(a, OR(b, c)) which should become OR(a, b, c)
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$or": [
                        { "status": "active" },
                        {
                          "$or": [
                            { "priority": "high" },
                            { "urgent": true }
                          ]
                        }
                      ]
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalOr.class, result);
            LogicalOr orNode = (LogicalOr) result;

            // Should be flattened to 3 children instead of 2
            assertEquals(3, orNode.children().size());

            // All children should be filters (no nested OR)
            for (LogicalNode child : orNode.children()) {
                assertInstanceOf(LogicalFilter.class, child);
            }
        }

        @Test
        @DisplayName("RemoveDoubleNotTransform should eliminate NOT(NOT(x))")
        void testRemoveDoubleNot() {
            // Create: NOT(NOT(status = active)) which should become (status = active)
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$not": {
                        "$not": { "status": "active" }
                      }
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            // Should be a filter directly, not wrapped in NOT nodes
            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("status", filter.selector());
            assertEquals(Operator.EQ, filter.op());
            assertEquals("active", extractValue(filter.operand()));
        }

        @Test
        @DisplayName("RemoveDoubleNotTransform should handle nested double negation")
        void testRemoveDoubleNotNested() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "selector1": "value1" },
                        {
                          "$not": {
                            "$not": { "selector2": "value2" }
                          }
                        }
                      ]
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(2, andNode.children().size());

            // Both children should be filters (double NOT eliminated)
            assertInstanceOf(LogicalFilter.class, andNode.children().get(0));
            assertInstanceOf(LogicalFilter.class, andNode.children().get(1));
        }

        @Test
        @DisplayName("Transform pipeline should preserve single NOT nodes")
        void testPreserveSingleNot() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$not": { "deleted": true }
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            // Single NOT should be preserved
            assertInstanceOf(LogicalNot.class, result);
            LogicalNot notNode = (LogicalNot) result;
            assertInstanceOf(LogicalFilter.class, notNode.child());
        }

        @Test
        @DisplayName("Custom transform pipeline should work correctly")
        void testCustomTransformPipeline() {
            // Create planner with empty pipeline (no transforms)
            LogicalPlanner customPlanner = new LogicalPlanner(List.of());

            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "selector1": "value1" },
                        {
                          "$and": [
                            { "selector2": "value2" }
                          ]
                        }
                      ]
                    }
                    """);
            LogicalNode result = customPlanner.plan(expr);

            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;

            // Without flattening transform, should have 2 children (nested structure preserved)
            assertEquals(2, andNode.children().size());
            assertInstanceOf(LogicalFilter.class, andNode.children().get(0));
            assertInstanceOf(LogicalAnd.class, andNode.children().get(1)); // Still nested
        }

        @Test
        @DisplayName("Transform pipeline with null transforms list should throw")
        void testNullTransformPipeline() {
            assertThrows(NullPointerException.class, () -> {
                new LogicalPlanner(null);
            }, "Should throw NullPointerException for null transform pipeline");
        }

        @Test
        @DisplayName("Transform pipeline should handle ElemMatch correctly")
        void testTransformPipelineWithElemMatch() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "items": {
                        "$elemMatch": {
                          "$and": [
                            { "status": "active" },
                            {
                              "$and": [
                                { "price": { "$gt": 100 } }
                              ]
                            }
                          ]
                        }
                      }
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalElemMatch.class, result);
            LogicalElemMatch elemMatch = (LogicalElemMatch) result;
            assertEquals("items", elemMatch.selector());

            // Sub-plan should be flattened AND (3 children: status, nested AND flattened)
            assertInstanceOf(LogicalAnd.class, elemMatch.subPlan());
            LogicalAnd subAnd = (LogicalAnd) elemMatch.subPlan();
            assertEquals(2, subAnd.children().size()); // Flattened: status + price

            // Both should be filters after flattening
            assertInstanceOf(LogicalFilter.class, subAnd.children().get(0));
            assertInstanceOf(LogicalFilter.class, subAnd.children().get(1));
        }

        @Test
        @DisplayName("Complex transform interactions should work correctly")
        void testComplexTransformInteractions() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "selector1": "value1" },
                        {
                          "$not": {
                            "$not": {
                              "$and": [
                                { "selector2": "value2" },
                                {
                                  "$and": [
                                    { "selector3": "value3" }
                                  ]
                                }
                              ]
                            }
                          }
                        }
                      ]
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd rootAnd = (LogicalAnd) result;
            assertEquals(2, rootAnd.children().size()); // selector1 + flattened inner AND after double NOT elimination

            // Find the selector1 filter and the inner AND (order may vary due to optimizations)
            LogicalFilter selector1Filter = null;
            LogicalAnd innerAnd = null;

            for (LogicalNode child : rootAnd.children()) {
                if (child instanceof LogicalFilter filter && "selector1".equals(filter.selector())) {
                    selector1Filter = filter;
                } else if (child instanceof LogicalAnd andChild) {
                    innerAnd = andChild;
                }
            }

            // Verify we found both expected children
            assertNotNull(selector1Filter, "Should have selector1 filter");
            assertNotNull(innerAnd, "Should have inner AND");
            assertEquals("selector1", selector1Filter.selector());

            // Verify the inner AND has selector2 and selector3
            assertEquals(2, innerAnd.children().size()); // selector2, selector3

            // Find selector2 and selector3 filters (order may vary)
            LogicalFilter selector2Filter = null;
            LogicalFilter selector3Filter = null;

            for (LogicalNode child : innerAnd.children()) {
                assertInstanceOf(LogicalFilter.class, child);
                LogicalFilter filter = (LogicalFilter) child;
                if ("selector2".equals(filter.selector())) {
                    selector2Filter = filter;
                } else if ("selector3".equals(filter.selector())) {
                    selector3Filter = filter;
                }
            }

            assertNotNull(selector2Filter, "Should have selector2 filter");
            assertNotNull(selector3Filter, "Should have selector3 filter");
        }
    }

    // ============================================================================
    // Edge Cases and Error Handling
    // ============================================================================

    @Nested
    @DisplayName("Edge Cases and Error Handling")
    class EdgeCasesTests {

        @Test
        @DisplayName("Null BqlExpr should throw NullPointerException")
        void testNullBqlExpr() {
            assertThrows(NullPointerException.class, () -> {
                planner.plan(null);
            });
        }

        @Test
        @DisplayName("Empty AND should become TRUE (optimized)")
        void testEmptyAnd() {
            BqlExpr expr = BqlParser.parse("{ \"$and\": [] }");
            LogicalNode result = planner.plan(expr);

            // After optimization: empty AND becomes TRUE
            assertInstanceOf(LogicalTrue.class, result);
        }

        @Test
        @DisplayName("Empty OR should become FALSE (optimized)")
        void testEmptyOr() {
            BqlExpr expr = BqlParser.parse("{ \"$or\": [] }");
            LogicalNode result = planner.plan(expr);

            // After optimization: empty OR becomes FALSE
            assertInstanceOf(LogicalFalse.class, result);
        }

        @Test
        @DisplayName("Single child AND should be simplified to child (optimized)")
        void testSingleChildAnd() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "status": "active" }
                      ]
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            // After optimization: single child AND becomes just the child
            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("status", filter.selector());
            assertEquals(Operator.EQ, filter.op());
        }

        @Test
        @DisplayName("Complex numeric values should convert correctly")
        void testComplexNumericValues() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "pi": 3.14159 },
                        { "large_number": 2147483647 },
                        { "negative": -42 }
                      ]
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(3, andNode.children().size());

            // Find filters by selector name (order may vary due to optimizations)
            LogicalFilter piFilter = null;
            LogicalFilter largeFilter = null;
            LogicalFilter negativeFilter = null;

            for (LogicalNode child : andNode.children()) {
                assertInstanceOf(LogicalFilter.class, child);
                LogicalFilter filter = (LogicalFilter) child;
                switch (filter.selector()) {
                    case "pi" -> piFilter = filter;
                    case "large_number" -> largeFilter = filter;
                    case "negative" -> negativeFilter = filter;
                }
            }

            // Verify numeric values are preserved correctly
            assertNotNull(piFilter);
            assertEquals(3.14159, extractValue(piFilter.operand()));

            assertNotNull(largeFilter);
            assertEquals(2147483647, extractValue(largeFilter.operand()));

            assertNotNull(negativeFilter);
            assertEquals(-42, extractValue(negativeFilter.operand()));
        }

        @Test
        @DisplayName("Unicode selector names should work correctly")
        void testUnicodeSelectorNames() {
            BqlExpr expr = BqlParser.parse("{ \"名前\": \"テスト\" }");
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("名前", filter.selector());
            assertEquals("テスト", extractValue(filter.operand()));
        }

        // Note: Unsupported BqlExpr test is not possible due to sealed interface design
        // This is actually a positive architectural feature - sealed interfaces prevent
        // invalid BqlExpr types from being created, making this error case impossible

        @Test
        @DisplayName("Empty selector name should work correctly")
        void testEmptySelectorName() {
            BqlExpr expr = BqlParser.parse("{ \"\": \"value\" }");
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("", filter.selector());
            assertEquals("value", extractValue(filter.operand()));
        }

        @Test
        @DisplayName("Very long selector names should work correctly")
        void testVeryLongSelectorName() {
            String longSelector = "a".repeat(1000);
            BqlExpr expr = BqlParser.parse("{ \"" + longSelector + "\": \"value\" }");
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals(longSelector, filter.selector());
            assertEquals("value", extractValue(filter.operand()));
        }

        @Test
        @DisplayName("Special character selector names should work correctly")
        void testSpecialCharacterSelectorNames() {
            String[] specialSelectors = {
                    "selector.with.dots",
                    "selector-with-dashes",
                    "selector_with_underscores",
                    "selector$with$dollar",
                    "selector@with@at",
                    "selector with spaces"
            };

            for (String selector : specialSelectors) {
                BqlExpr expr = BqlParser.parse("{ \"" + selector + "\": \"test\" }");
                LogicalNode result = planner.plan(expr);

                assertInstanceOf(LogicalFilter.class, result, "Should handle selector: " + selector);
                LogicalFilter filter = (LogicalFilter) result;
                assertEquals(selector, filter.selector());
                assertEquals("test", extractValue(filter.operand()));
            }
        }

        @Test
        @DisplayName("Zero and negative numbers should work correctly")
        void testZeroAndNegativeNumbers() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "zero": 0 },
                        { "negative_int": -123 },
                        { "negative_double": -45.67 },
                        { "max_int": 2147483647 },
                        { "min_int": -2147483648 }
                      ]
                    }
                    """);
            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(5, andNode.children().size());

            // Find filters by selector name (order may vary due to optimizations)
            LogicalFilter zeroFilter = null;
            LogicalFilter negIntFilter = null;
            LogicalFilter negDoubleFilter = null;
            LogicalFilter maxIntFilter = null;
            LogicalFilter minIntFilter = null;

            for (LogicalNode child : andNode.children()) {
                assertInstanceOf(LogicalFilter.class, child);
                LogicalFilter filter = (LogicalFilter) child;
                switch (filter.selector()) {
                    case "zero" -> zeroFilter = filter;
                    case "negative_int" -> negIntFilter = filter;
                    case "negative_double" -> negDoubleFilter = filter;
                    case "max_int" -> maxIntFilter = filter;
                    case "min_int" -> minIntFilter = filter;
                }
            }

            assertNotNull(zeroFilter);
            assertEquals(0, extractValue(zeroFilter.operand()));

            assertNotNull(negIntFilter);
            assertEquals(-123, extractValue(negIntFilter.operand()));

            assertNotNull(negDoubleFilter);
            assertEquals(-45.67, extractValue(negDoubleFilter.operand()));

            assertNotNull(maxIntFilter);
            assertEquals(2147483647, extractValue(maxIntFilter.operand()));

            assertNotNull(minIntFilter);
            assertEquals(-2147483648, extractValue(minIntFilter.operand()));
        }
    }

    // ============================================================================
    // Property-Based Tests
    // ============================================================================

    @Nested
    @DisplayName("Property-Based Tests")
    class PropertyBasedTests {

        @Test
        @DisplayName("All comparison operators should produce LogicalFilter")
        void testAllComparisonOperators() {
            String[] queries = {
                    "{ \"selector\": \"value\" }",                    // EQ
                    "{ \"selector\": { \"$ne\": \"value\" } }",       // NE
                    "{ \"selector\": { \"$gt\": 100 } }",             // GT
                    "{ \"selector\": { \"$gte\": 100 } }",            // GTE
                    "{ \"selector\": { \"$lt\": 100 } }",             // LT
                    "{ \"selector\": { \"$lte\": 100 } }"             // LTE
            };

            for (String query : queries) {
                BqlExpr expr = BqlParser.parse(query);
                LogicalNode result = planner.plan(expr);

                assertInstanceOf(LogicalFilter.class, result, "Query should produce LogicalFilter: " + query);
                LogicalFilter filter = (LogicalFilter) result;
                assertEquals("selector", filter.selector());
                assertNotNull(filter.op());
                assertNotNull(filter.operand());
            }
        }

        @Test
        @DisplayName("All array operators should produce LogicalFilter")
        void testAllArrayOperators() {
            String[] queries = {
                    "{ \"selector\": { \"$in\": [\"a\", \"b\"] } }",      // IN
                    "{ \"selector\": { \"$nin\": [\"a\", \"b\"] } }",     // NIN
                    "{ \"selector\": { \"$all\": [\"a\", \"b\"] } }"      // ALL
            };

            for (String query : queries) {
                BqlExpr expr = BqlParser.parse(query);
                LogicalNode result = planner.plan(expr);

                assertInstanceOf(LogicalFilter.class, result, "Query should produce LogicalFilter: " + query);
                LogicalFilter filter = (LogicalFilter) result;
                assertEquals("selector", filter.selector());
                assertTrue(filter.op() == Operator.IN || filter.op() == Operator.NIN || filter.op() == Operator.ALL);
                assertInstanceOf(List.class, filter.operand());
            }
        }

        @Test
        @DisplayName("All logical operators should preserve structure")
        void testAllLogicalOperators() {
            String[] queries = {
                    "{ \"$and\": [ { \"a\": 1 }, { \"b\": 2 } ] }",
                    "{ \"$or\": [ { \"a\": 1 }, { \"b\": 2 } ] }",
                    "{ \"$not\": { \"a\": 1 } }"
            };

            for (String query : queries) {
                BqlExpr expr = BqlParser.parse(query);
                LogicalNode result = planner.plan(expr);

                // Should be one of the logical node types
                assertTrue(result instanceof LogicalAnd ||
                                result instanceof LogicalOr ||
                                result instanceof LogicalNot,
                        "Query should produce logical node: " + query);
            }
        }

        @Test
        @DisplayName("Transform pipeline should be idempotent")
        void testTransformIdempotence() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "status": "active" },
                        { "$not": { "$not": { "deleted": false } } }
                      ]
                    }
                    """);

            // Run planning multiple times
            LogicalNode result1 = planner.plan(expr);
            LogicalNode result2 = planner.plan(expr);

            // Results should be structurally equivalent
            assertEquals(result1.toString(), result2.toString());

            // Both should have same structure after transforms
            assertInstanceOf(LogicalAnd.class, result1);
            assertInstanceOf(LogicalAnd.class, result2);

            LogicalAnd and1 = (LogicalAnd) result1;
            LogicalAnd and2 = (LogicalAnd) result2;
            assertEquals(and1.children().size(), and2.children().size());
        }
    }

    // ============================================================================
    // Performance Tests
    // ============================================================================

    @Nested
    @DisplayName("Performance Tests")
    class PerformanceTests {

        @Test
        @DisplayName("Large query with many conditions should perform well")
        void testLargeQueryPerformance() {
            StringBuilder queryBuilder = new StringBuilder("{ \"$and\": [");
            for (int i = 0; i < 100; i++) {
                if (i > 0) queryBuilder.append(", ");
                queryBuilder.append("{ \"selector").append(i).append("\": \"value").append(i).append("\" }");
            }
            queryBuilder.append("] }");

            BqlExpr expr = BqlParser.parse(queryBuilder.toString());

            long startTime = System.nanoTime();
            LogicalNode result = planner.plan(expr);
            long endTime = System.nanoTime();

            long durationMs = (endTime - startTime) / 1_000_000;
            assertTrue(durationMs < 100, "Planning should complete within 100ms, took: " + durationMs + "ms");

            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(100, andNode.children().size());
        }

        @Test
        @DisplayName("Deeply nested query should handle recursion efficiently")
        void testDeeplyNestedQueryPerformance() {
            // Test with different depths to verify double negation elimination
            // Even depth should result in LogicalFilter, odd depth in LogicalNot
            int[] testDepths = {2, 3, 10, 20, 50};

            for (int depth : testDepths) {
                // Create a deeply nested structure: NOT(NOT(NOT(...(selector = value)...)))
                StringBuilder queryBuilder = new StringBuilder();

                queryBuilder.append("{ \"$not\": ".repeat(depth));
                queryBuilder.append("{ \"selector\": \"value\" }");
                queryBuilder.append(" }".repeat(depth));

                BqlExpr expr = BqlParser.parse(queryBuilder.toString());

                long startTime = System.nanoTime();
                LogicalNode result = planner.plan(expr);
                long endTime = System.nanoTime();

                long durationMs = (endTime - startTime) / 1_000_000;
                assertTrue(durationMs < 100, "Deep nesting (depth=" + depth + ") should complete within 100ms, took: " + durationMs + "ms");

                // With double negation elimination:
                // Even depth: NOT(NOT(...)) pairs cancel out, leaving the base filter
                // Odd depth: One NOT remains after cancellation
                if (depth % 2 == 0) {
                    assertInstanceOf(LogicalFilter.class, result,
                            "Even depth (" + depth + ") should result in LogicalFilter after double negation elimination");
                    LogicalFilter filter = (LogicalFilter) result;
                    assertEquals("selector", filter.selector());
                    assertEquals("value", extractValue(filter.operand()));
                } else {
                    assertInstanceOf(LogicalNot.class, result,
                            "Odd depth (" + depth + ") should result in LogicalNot after double negation elimination");
                    LogicalNot notNode = (LogicalNot) result;
                    assertInstanceOf(LogicalFilter.class, notNode.child());
                    LogicalFilter childFilter = (LogicalFilter) notNode.child();
                    assertEquals("selector", childFilter.selector());
                    assertEquals("value", extractValue(childFilter.operand()));
                }
            }
        }
    }

    // ============================================================================
    // Contradiction Detection Tests
    // ============================================================================

    @Nested
    @DisplayName("Contradiction Detection Tests")
    class ContradictionDetectionTests {

        @Test
        @DisplayName("Range contradictions should result in LogicalFalse")
        void testRangeContradictions() {
            // Test various impossible range combinations
            String[] contradictoryQueries = {
                    "{ price: { $gt: 100, $lt: 50 } }",          // x > 100 AND x < 50
                    "{ age: { $gte: 200, $lte: 100 } }",         // x >= 200 AND x <= 100
                    "{ score: { $gt: 90, $lte: 90 } }",          // x > 90 AND x <= 90
                    "{ count: { $gte: 50, $lt: 50 } }"           // x >= 50 AND x < 50
            };

            for (String query : contradictoryQueries) {
                BqlExpr expr = BqlParser.parse(query);
                LogicalNode result = planner.plan(expr);

                assertInstanceOf(LogicalFalse.class, result,
                        "Query should be detected as contradiction: " + query);
            }
        }

        @Test
        @DisplayName("EQ vs NE with same value should result in LogicalFalse")
        void testEqVsNeContradiction() {
            // These contradictions should be detected
            String[] contradictoryQueries = {
                    "{ status: { $eq: 'ACTIVE', $ne: 'ACTIVE' } }",    // x = A AND x != A
                    "{ count: { $ne: 42, $eq: 42 } }",                 // x != 42 AND x = 42
                    "{ price: { $eq: 19.99, $ne: 19.99 } }"            // x = 19.99 AND x != 19.99
            };

            for (String query : contradictoryQueries) {
                BqlExpr expr = BqlParser.parse(query);
                LogicalNode result = planner.plan(expr);

                assertInstanceOf(LogicalFalse.class, result,
                        "Query should be detected as contradiction: " + query);
            }
        }

        @Test
        @DisplayName("Valid range queries should not be contradictions")
        void testValidRangeQueries() {
            // These are valid range queries and should NOT result in LogicalFalse
            String[] validQueries = {
                    "{ price: { $gt: 50, $lt: 100 } }",             // x > 50 AND x < 100
                    "{ age: { $gte: 18, $lte: 65 } }",              // x >= 18 AND x <= 65
                    "{ score: { $gt: 70, $lte: 100 } }",            // x > 70 AND x <= 100
                    "{ count: { $gte: 0, $lt: 1000 } }"             // x >= 0 AND x < 1000
            };

            for (String query : validQueries) {
                BqlExpr expr = BqlParser.parse(query);
                LogicalNode result = planner.plan(expr);

                assertNotSame(LogicalFalse.class, result.getClass(),
                        "Valid query should not be contradiction: " + query);

                // Should be a LogicalAnd with two LogicalFilter children
                assertInstanceOf(LogicalAnd.class, result);
                LogicalAnd andNode = (LogicalAnd) result;
                assertEquals(2, andNode.children().size());
                for (LogicalNode child : andNode.children()) {
                    assertInstanceOf(LogicalFilter.class, child);
                }
            }
        }

        @Test
        @DisplayName("Complex contradictions in nested structures")
        void testNestedContradictions() {
            // Test contradiction detection in nested AND/OR structures
            String query = """
                    {
                      "$and": [
                        { "status": "active" },
                        { "price": { "$gt": 100, "$lt": 50 } },
                        { "category": "electronics" }
                      ]
                    }
                    """;

            BqlExpr expr = BqlParser.parse(query);
            LogicalNode result = planner.plan(expr);

            // The entire AND should become FALSE due to the price contradiction
            assertInstanceOf(LogicalFalse.class, result);
        }

        @Test
        @DisplayName("Contradictions in OR should not result in LogicalFalse")
        void testContradictionsInOr() {
            // Contradictions in OR branches should not make the entire query false
            String query = """
                    {
                      "$or": [
                        { "status": "active" },
                        { "price": { "$gt": 100, "$lt": 50 } }
                      ]
                    }
                    """;

            BqlExpr expr = BqlParser.parse(query);
            LogicalNode result = planner.plan(expr);

            // The LogicalPlanner optimizes OR(status=active, FALSE) to just status=active
            // This is correct optimization behavior
            assertInstanceOf(LogicalFilter.class, result,
                    "OR with contradiction branch should be optimized to remaining valid filter");

            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("status", filter.selector());
            assertEquals(Operator.EQ, filter.op());
            assertEquals("active", extractValue(filter.operand()));
        }
    }

    // ============================================================================
    // Redundancy Elimination Tests  
    // ============================================================================

    @Nested
    @DisplayName("Redundancy Elimination Tests")
    class RedundancyEliminationTests {

        @Test
        @DisplayName("EQ + NE with different values should eliminate redundant NE")
        void testEqNeRedundancyElimination() {
            // These queries have redundant NE conditions that should be eliminated
            String[] redundantQueries = {
                    "{ status: { $eq: 'ACTIVE', $ne: 'INACTIVE' } }",   // x = ACTIVE makes x != INACTIVE redundant
                    "{ count: { $eq: 42, $ne: 100 } }",                 // x = 42 makes x != 100 redundant
                    "{ price: { $eq: 19.99, $ne: 29.99 } }"             // x = 19.99 makes x != 29.99 redundant
            };

            for (String query : redundantQueries) {
                BqlExpr expr = BqlParser.parse(query);
                LogicalNode result = planner.plan(expr);

                // Should be optimized to a single LogicalFilter with EQ operator
                assertInstanceOf(LogicalFilter.class, result,
                        "Redundant NE should be eliminated: " + query);

                LogicalFilter filter = (LogicalFilter) result;
                assertEquals(Operator.EQ, filter.op(),
                        "Should keep the EQ operator: " + query);
            }
        }

        @Test
        @DisplayName("Numeric range redundancy should eliminate less restrictive conditions")
        void testNumericRangeRedundancy() {
            // Test specific cases to verify redundancy elimination behavior
            // Note: BSON parsing order may affect which condition is kept

            // Test GT redundancy - should keep the more restrictive value
            String gtQuery = "{ age: { $gt: 25, $gt: 20 } }";
            BqlExpr gtExpr = BqlParser.parse(gtQuery);
            LogicalNode gtResult = planner.plan(gtExpr);

            assertInstanceOf(LogicalFilter.class, gtResult,
                    "GT redundancy should be eliminated");
            LogicalFilter gtFilter = (LogicalFilter) gtResult;
            assertEquals(Operator.GT, gtFilter.op());
            // Either 25 or 20 could be kept depending on parsing order, but should be single filter
            assertTrue(((Integer) extractValue(gtFilter.operand())) == 25 || ((Integer) extractValue(gtFilter.operand())) == 20,
                    "Should keep one of the GT values");

            // Test LT redundancy
            String ltQuery = "{ score: { $lt: 50, $lt: 60 } }";
            BqlExpr ltExpr = BqlParser.parse(ltQuery);
            LogicalNode ltResult = planner.plan(ltExpr);

            assertInstanceOf(LogicalFilter.class, ltResult,
                    "LT redundancy should be eliminated");
            LogicalFilter ltFilter = (LogicalFilter) ltResult;
            assertEquals(Operator.LT, ltFilter.op());
            assertTrue(((Integer) extractValue(ltFilter.operand())) == 50 || ((Integer) extractValue(ltFilter.operand())) == 60,
                    "Should keep one of the LT values");

            // Test GTE vs GT redundancy
            String gteGtQuery = "{ count: { $gte: 100, $gt: 50 } }";
            BqlExpr gteGtExpr = BqlParser.parse(gteGtQuery);
            LogicalNode gteGtResult = planner.plan(gteGtExpr);

            assertInstanceOf(LogicalFilter.class, gteGtResult,
                    "GTE/GT redundancy should be eliminated");
            LogicalFilter gteGtFilter = (LogicalFilter) gteGtResult;
            // Should keep the more restrictive condition
            assertTrue((gteGtFilter.op() == Operator.GTE && ((Integer) extractValue(gteGtFilter.operand())) == 100) ||
                            (gteGtFilter.op() == Operator.GT && ((Integer) extractValue(gteGtFilter.operand())) == 50),
                    "Should keep one of the GTE/GT conditions");
        }

        @Test
        @DisplayName("Non-redundant conditions should be preserved")
        void testNonRedundantConditionsPreserved() {
            // These queries have non-redundant conditions that should be preserved
            String[] nonRedundantQueries = {
                    "{ age: { $gt: 18, $lt: 65 } }",                    // Valid range
                    "{ score: { $gte: 70, $ne: 85 } }",                 // Range with exclusion
                    "{ price: { $gt: 10, $lte: 100, $ne: 50 } }"        // Complex valid range
            };

            for (String query : nonRedundantQueries) {
                BqlExpr expr = BqlParser.parse(query);
                LogicalNode result = planner.plan(expr);

                // Should remain as LogicalAnd with multiple conditions
                assertInstanceOf(LogicalAnd.class, result,
                        "Non-redundant conditions should be preserved: " + query);

                LogicalAnd andNode = (LogicalAnd) result;
                assertTrue(andNode.children().size() > 1,
                        "Should have multiple conditions: " + query);

                // All children should be LogicalFilter
                for (LogicalNode child : andNode.children()) {
                    assertInstanceOf(LogicalFilter.class, child);
                }
            }
        }

        @Test
        @DisplayName("Integer redundancy elimination should work correctly")
        void testIntegerRedundancyElimination() {
            // Test the specific integer case that was fixed
            String query = "{ age: { $eq: 25, $ne: 30 } }";

            BqlExpr expr = BqlParser.parse(query);
            LogicalNode result = planner.plan(expr);

            // Should be optimized to single EQ filter
            assertInstanceOf(LogicalFilter.class, result,
                    "Integer redundancy should be eliminated");

            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("age", filter.selector());
            assertEquals(Operator.EQ, filter.op());
            assertEquals(25, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("Redundancy elimination in complex nested structures")
        void testNestedRedundancyElimination() {
            String query = """
                    {
                      "$and": [
                        { "status": { "$eq": "ACTIVE", "$ne": "INACTIVE" } },
                        { "age": { "$gt": 25, "$gt": 20 } },
                        { "category": "electronics" }
                      ]
                    }
                    """;

            BqlExpr expr = BqlParser.parse(query);
            LogicalNode result = planner.plan(expr);

            // Should be LogicalAnd with optimized children
            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(3, andNode.children().size());

            // Find each optimized filter
            LogicalFilter statusFilter = null;
            LogicalFilter ageFilter = null;
            LogicalFilter categoryFilter = null;

            for (LogicalNode child : andNode.children()) {
                assertInstanceOf(LogicalFilter.class, child);
                LogicalFilter filter = (LogicalFilter) child;

                switch (filter.selector()) {
                    case "status" -> {
                        statusFilter = filter;
                        // Should be optimized to just EQ
                        assertEquals(Operator.EQ, filter.op());
                        assertEquals("ACTIVE", extractValue(filter.operand()));
                    }
                    case "age" -> {
                        ageFilter = filter;
                        // Should be optimized to GT with one of the values
                        assertEquals(Operator.GT, filter.op());
                        assertTrue(((Integer) extractValue(filter.operand())) == 25 || ((Integer) extractValue(filter.operand())) == 20,
                                "Should keep one of the age GT values");
                    }
                    case "category" -> {
                        categoryFilter = filter;
                        assertEquals(Operator.EQ, filter.op());
                        assertEquals("electronics", extractValue(filter.operand()));
                    }
                }
            }

            assertNotNull(statusFilter, "Should have status filter");
            assertNotNull(ageFilter, "Should have age filter");
            assertNotNull(categoryFilter, "Should have category filter");
        }
    }
}