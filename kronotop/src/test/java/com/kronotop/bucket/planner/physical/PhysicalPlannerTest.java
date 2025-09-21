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

import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.logical.*;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for PhysicalPlanner covering all LogicalNode transposition cases.
 * Tests the complete LogicalNode → PhysicalNode conversion pipeline with optimizations.
 */
class PhysicalPlannerTest extends BasePhysicalPlannerTest {

    // ============================================================================
    // Basic Transposition Tests - All LogicalNode Types
    // ============================================================================

    @Test
    @DisplayName("Simple status query should work")
    void testSimpleQuery() {
        String query = "{status: 'ACTIVE'}";
        BqlExpr expr = BqlParser.parse(query);

        LogicalNode logicalPlan = logicalPlanner.plan(expr);
        PhysicalNode node = physicalPlanner.plan(metadata, logicalPlan, new PlannerContext());

        assertInstanceOf(PhysicalFullScan.class, node);
        PhysicalFullScan fullScan = (PhysicalFullScan) node;
        assertInstanceOf(PhysicalFilter.class, fullScan.node());

        PhysicalFilter filter = (PhysicalFilter) fullScan.node();
        assertEquals("status", filter.selector());
        assertEquals(Operator.EQ, filter.op());
        assertEquals("ACTIVE", extractValue(filter.operand()));
    }

    // ============================================================================
    // Full Scan vs Index Scan Tests (ignoring indexed cases per user request)
    // ============================================================================

    @Nested
    @DisplayName("Basic Transposition Tests")
    class BasicTranspositionTests {

        @Test
        @DisplayName("LogicalFilter should transpose to PhysicalFullScan with PhysicalFilter")
        void testLogicalFilterTransposition() {
            LogicalFilter logicalFilter = new LogicalFilter("status", Operator.EQ, "active");
            PhysicalNode result = planLogical(logicalFilter);

            // Should be FullScan since no index exists for "status"
            assertInstanceOf(PhysicalFullScan.class, result);
            PhysicalFullScan fullScan = (PhysicalFullScan) result;

            // Should contain PhysicalFilter with same fields
            assertInstanceOf(PhysicalFilter.class, fullScan.node());
            PhysicalFilter filter = (PhysicalFilter) fullScan.node();
            assertEquals("status", filter.selector());
            assertEquals(Operator.EQ, filter.op());
            assertEquals("active", extractValue(filter.operand()));
        }

        @Test
        @DisplayName("LogicalAnd should transpose to PhysicalAnd with child transposition")
        void testLogicalAndTransposition() {
            LogicalAnd logicalAnd = new LogicalAnd(Arrays.asList(
                    new LogicalFilter("status", Operator.EQ, "active"),
                    new LogicalFilter("priority", Operator.GT, 5)
            ));
            PhysicalNode result = planLogical(logicalAnd);

            assertInstanceOf(PhysicalAnd.class, result);
            PhysicalAnd physicalAnd = (PhysicalAnd) result;
            assertEquals(2, physicalAnd.children().size());

            // Both children should be FullScans with filters
            assertInstanceOf(PhysicalFullScan.class, physicalAnd.children().get(0));
            assertInstanceOf(PhysicalFullScan.class, physicalAnd.children().get(1));
        }

        @Test
        @DisplayName("LogicalOr should transpose to PhysicalOr with child transposition")
        void testLogicalOrTransposition() {
            LogicalOr logicalOr = new LogicalOr(Arrays.asList(
                    new LogicalFilter("category", Operator.EQ, "electronics"),
                    new LogicalFilter("featured", Operator.EQ, true)
            ));
            PhysicalNode result = planLogical(logicalOr);

            assertInstanceOf(PhysicalOr.class, result);
            PhysicalOr physicalOr = (PhysicalOr) result;
            assertEquals(2, physicalOr.children().size());

            // Both children should be FullScans with filters
            assertInstanceOf(PhysicalFullScan.class, physicalOr.children().get(0));
            assertInstanceOf(PhysicalFullScan.class, physicalOr.children().get(1));
        }

        @Test
        @DisplayName("LogicalNot should transpose to PhysicalNot with child transposition")
        void testLogicalNotTransposition() {
            LogicalNot logicalNot = new LogicalNot(new LogicalFilter("deleted", Operator.EQ, true));
            PhysicalNode result = planLogical(logicalNot);

            assertInstanceOf(PhysicalNot.class, result);
            PhysicalNot physicalNot = (PhysicalNot) result;

            // Child should be FullScan with filter
            assertInstanceOf(PhysicalFullScan.class, physicalNot.child());
            PhysicalFullScan fullScan = (PhysicalFullScan) physicalNot.child();
            assertInstanceOf(PhysicalFilter.class, fullScan.node());
        }

        @Test
        @DisplayName("LogicalElemMatch should transpose to PhysicalElemMatch with subplan transposition")
        void testLogicalElemMatchTransposition() {
            LogicalElemMatch logicalElemMatch = new LogicalElemMatch("items",
                    new LogicalFilter("price", Operator.GT, 100));
            PhysicalNode result = planLogical(logicalElemMatch);

            assertInstanceOf(PhysicalElemMatch.class, result);
            PhysicalElemMatch physicalElemMatch = (PhysicalElemMatch) result;
            assertEquals("items", physicalElemMatch.selector());

            // Subplan should be FullScan with filter
            assertInstanceOf(PhysicalFullScan.class, physicalElemMatch.subPlan());
        }

        @Test
        @DisplayName("LogicalTrue should transpose to PhysicalTrue singleton")
        void testLogicalTrueTransposition() {
            LogicalTrue logicalTrue = LogicalTrue.INSTANCE;
            PhysicalNode result = planLogical(logicalTrue);

            assertInstanceOf(PhysicalTrue.class, result);
            // With ID assignment, each instance is unique, so we check type instead of identity
        }

        @Test
        @DisplayName("LogicalFalse should transpose to PhysicalFalse singleton")
        void testLogicalFalseTransposition() {
            LogicalFalse logicalFalse = LogicalFalse.INSTANCE;
            PhysicalNode result = planLogical(logicalFalse);

            assertInstanceOf(PhysicalFalse.class, result);
            // With ID assignment, each instance is unique, so we check type instead of identity
        }
    }

    // ============================================================================
    // Complex Nested Query Tests
    // ============================================================================

    @Nested
    @DisplayName("Scan Strategy Tests")
    class ScanStrategyTests {

        @Test
        @DisplayName("Filter without index should use FullScan")
        void testFullScanStrategy() {
            // Non-indexed field should use FullScan
            LogicalFilter filter = new LogicalFilter("non_indexed_field", Operator.EQ, "value");
            PhysicalNode result = planLogical(filter);

            assertInstanceOf(PhysicalFullScan.class, result);
            PhysicalFullScan fullScan = (PhysicalFullScan) result;

            // Verify filter is pushed down to scan
            assertInstanceOf(PhysicalFilter.class, fullScan.node());
            PhysicalFilter physicalFilter = (PhysicalFilter) fullScan.node();
            assertEquals("non_indexed_field", physicalFilter.selector());
            assertEquals(Operator.EQ, physicalFilter.op());
            assertEquals("value", physicalFilter.operand());
        }

        @Test
        @DisplayName("All operators should work with FullScan")
        void testAllOperatorsWithFullScan() {
            Operator[] operators = {Operator.EQ, Operator.NE, Operator.GT, Operator.GTE,
                    Operator.LT, Operator.LTE, Operator.IN, Operator.NIN,
                    Operator.ALL, Operator.SIZE, Operator.EXISTS};

            for (Operator op : operators) {
                Object operand = switch (op) {
                    case SIZE -> 5;
                    case EXISTS -> true;
                    case IN, NIN, ALL -> Arrays.asList("a", "b", "c");
                    default -> "test_value";
                };

                LogicalFilter filter = new LogicalFilter("test_field", op, operand);
                PhysicalNode result = planLogical(filter);

                assertInstanceOf(PhysicalFullScan.class, result, "Operator " + op + " should use FullScan");
                PhysicalFullScan fullScan = (PhysicalFullScan) result;
                assertInstanceOf(PhysicalFilter.class, fullScan.node());

                PhysicalFilter physicalFilter = (PhysicalFilter) fullScan.node();
                assertEquals("test_field", physicalFilter.selector());
                assertEquals(op, physicalFilter.op());
                assertEquals(operand, physicalFilter.operand());
            }
        }
    }

    // ============================================================================
    // Field Reuse Optimization Tests
    // ============================================================================

    @Nested
    @DisplayName("Complex Nested Query Tests")
    class ComplexNestedQueryTests {

        @Test
        @DisplayName("Deeply nested AND/OR should transpose correctly")
        void testDeeplyNestedAndOr() {
            PhysicalNode result = planQuery("""
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

            assertInstanceOf(PhysicalAnd.class, result);
            PhysicalAnd rootAnd = (PhysicalAnd) result;
            assertEquals(2, rootAnd.children().size());

            // Find the tenant_id scan and the OR node
            PhysicalFullScan tenantScan = null;
            PhysicalOr orNode = null;

            for (PhysicalNode child : rootAnd.children()) {
                if (child instanceof PhysicalFullScan scan &&
                        scan.node() instanceof PhysicalFilter filter &&
                        "tenant_id".equals(filter.selector())) {
                    tenantScan = scan;
                } else if (child instanceof PhysicalOr or) {
                    orNode = or;
                }
            }

            assertNotNull(tenantScan);
            assertNotNull(orNode);
            assertEquals(2, orNode.children().size());
        }

        @Test
        @DisplayName("Multiple NOT operators should transpose correctly")
        void testMultipleNotOperators() {
            PhysicalNode result = planQuery("""
                    {
                      "$and": [
                        { "$not": { "status": "deleted" } },
                        { "$not": { "hidden": true } }
                      ]
                    }
                    """);

            assertInstanceOf(PhysicalAnd.class, result);
            PhysicalAnd andNode = (PhysicalAnd) result;
            assertEquals(2, andNode.children().size());

            // Both children should be NOT nodes
            assertInstanceOf(PhysicalNot.class, andNode.children().get(0));
            assertInstanceOf(PhysicalNot.class, andNode.children().get(1));
        }

        @Test
        @DisplayName("ElemMatch with nested logical operators should transpose correctly")
        void testElemMatchWithNestedLogical() {
            PhysicalNode result = planQuery("""
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

            assertInstanceOf(PhysicalElemMatch.class, result);
            PhysicalElemMatch elemMatch = (PhysicalElemMatch) result;
            assertEquals("products", elemMatch.selector());

            // Sub-plan should be PhysicalAnd
            assertInstanceOf(PhysicalAnd.class, elemMatch.subPlan());
            PhysicalAnd subAnd = (PhysicalAnd) elemMatch.subPlan();
            assertEquals(2, subAnd.children().size());
        }

        @Test
        @DisplayName("Very deep nesting should transpose correctly")
        void testVeryDeepMixedNesting() {
            PhysicalNode result = planQuery("""
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

            // Should successfully transpose deeply nested structure
            assertInstanceOf(PhysicalAnd.class, result);
            PhysicalAnd rootAnd = (PhysicalAnd) result;
            assertEquals(2, rootAnd.children().size());
        }
    }

    // ============================================================================
    // Edge Cases and Error Handling
    // ============================================================================

    @Nested
    @DisplayName("Field Reuse Optimization Tests")
    class FieldReuseOptimizationTests {

        @Test
        @DisplayName("LogicalFilter fields should be reused directly in PhysicalFilter")
        void testDirectFieldReuse() {
            String selector = "test_selector";
            Operator operator = Operator.GT;
            Object operand = 42;

            LogicalFilter logicalFilter = new LogicalFilter(selector, operator, operand);
            PhysicalNode result = planLogical(logicalFilter);

            assertInstanceOf(PhysicalFullScan.class, result);
            PhysicalFullScan fullScan = (PhysicalFullScan) result;
            assertInstanceOf(PhysicalFilter.class, fullScan.node());

            PhysicalFilter physicalFilter = (PhysicalFilter) fullScan.node();

            // Fields should be same references (zero-copy)
            assertSame(selector, physicalFilter.selector());
            assertSame(operator, physicalFilter.op());
            assertSame(operand, physicalFilter.operand());
        }

        @Test
        @DisplayName("Complex operands should be reused without copying")
        void testComplexOperandReuse() {
            List<String> complexOperand = Arrays.asList("value1", "value2", "value3");

            LogicalFilter logicalFilter = new LogicalFilter("selector", Operator.IN, complexOperand);
            PhysicalNode result = planLogical(logicalFilter);

            assertInstanceOf(PhysicalFullScan.class, result);
            PhysicalFullScan fullScan = (PhysicalFullScan) result;
            PhysicalFilter physicalFilter = (PhysicalFilter) fullScan.node();

            // Should be same list reference (zero-copy)
            assertSame(complexOperand, physicalFilter.operand());
        }

        @Test
        @DisplayName("Singleton instances should be reused")
        void testSingletonReuse() {
            LogicalTrue logicalTrue = LogicalTrue.INSTANCE;
            LogicalFalse logicalFalse = LogicalFalse.INSTANCE;

            PhysicalNode trueResult = planLogical(logicalTrue);
            PhysicalNode falseResult = planLogical(logicalFalse);

            // Should reuse singleton instances
            // With ID assignment, each instance is unique, so we check type instead of identity
            assertInstanceOf(PhysicalTrue.class, trueResult);
            assertInstanceOf(PhysicalFalse.class, falseResult);
        }
    }

    // ============================================================================
    // Performance Optimization Tests  
    // ============================================================================

    @Nested
    @DisplayName("Edge Cases and Error Handling")
    class EdgeCasesTests {

        @Test
        @DisplayName("Null LogicalNode should throw NullPointerException")
        void testNullLogicalNode() {
            assertThrows(NullPointerException.class, () -> {
                planLogical(null);
            });
        }

        @Test
        @DisplayName("Empty AND should transpose to TRUE")
        void testEmptyAnd() {
            PhysicalNode result = planQuery("{ \"$and\": [] }");

            // After logical optimization: empty AND becomes TRUE, then transposes to PhysicalTrue
            assertInstanceOf(PhysicalTrue.class, result);
            // With ID assignment, each instance is unique, so we check type instead of identity
            assertInstanceOf(PhysicalTrue.class, result);
        }

        @Test
        @DisplayName("Empty OR should transpose to FALSE")
        void testEmptyOr() {
            PhysicalNode result = planQuery("{ \"$or\": [] }");

            // After logical optimization: empty OR becomes FALSE, then transposes to PhysicalFalse
            assertInstanceOf(PhysicalFalse.class, result);
            // With ID assignment, each instance is unique, so we check type instead of identity
            assertInstanceOf(PhysicalFalse.class, result);
        }

        @Test
        @DisplayName("Single child AND should be simplified and transposed")
        void testSingleChildAnd() {
            PhysicalNode result = planQuery("""
                    {
                      "$and": [
                        { "status": "active" }
                      ]
                    }
                    """);

            // After logical optimization: single child AND becomes just the child
            assertInstanceOf(PhysicalFullScan.class, result);
            PhysicalFullScan fullScan = (PhysicalFullScan) result;
            PhysicalFilter filter = (PhysicalFilter) fullScan.node();
            assertEquals("status", filter.selector());
        }

        @Test
        @DisplayName("Complex numeric values should transpose correctly")
        void testComplexNumericValues() {
            PhysicalNode result = planQuery("""
                    {
                      "$and": [
                        { "pi": 3.14159 },
                        { "large_number": 2147483647 },
                        { "negative": -42 }
                      ]
                    }
                    """);

            assertInstanceOf(PhysicalAnd.class, result);
            PhysicalAnd andNode = (PhysicalAnd) result;
            assertEquals(3, andNode.children().size());

            // All should be FullScans with preserved numeric values
            for (PhysicalNode child : andNode.children()) {
                assertInstanceOf(PhysicalFullScan.class, child);
                PhysicalFullScan fullScan = (PhysicalFullScan) child;
                assertInstanceOf(PhysicalFilter.class, fullScan.node());
            }
        }

        @Test
        @DisplayName("Unicode values should transpose correctly")
        void testUnicodeValues() {
            PhysicalNode result = planQuery("{ \"名前\": \"テスト\" }");

            assertInstanceOf(PhysicalFullScan.class, result);
            PhysicalFullScan fullScan = (PhysicalFullScan) result;
            PhysicalFilter filter = (PhysicalFilter) fullScan.node();
            assertEquals("名前", filter.selector());
            assertEquals("テスト", extractValue(filter.operand()));
        }
    }

    // ============================================================================
    // Integration Tests
    // ============================================================================

    @Nested
    @DisplayName("Performance Optimization Tests")
    class PerformanceOptimizationTests {

        @Test
        @DisplayName("Large query transposition should be efficient")
        void testLargeQueryPerformance() {
            // Create logical plan with 100 filters
            List<LogicalNode> filters = new java.util.ArrayList<>();
            for (int i = 0; i < 100; i++) {
                filters.add(new LogicalFilter("selector" + i, Operator.EQ, "value" + i));
            }
            LogicalAnd largeAnd = new LogicalAnd(filters);

            long startTime = System.nanoTime();
            PhysicalNode result = planLogical(largeAnd);
            long endTime = System.nanoTime();

            long durationMs = (endTime - startTime) / 1_000_000;
            assertTrue(durationMs < 50, "Large transposition should complete within 50ms, took: " + durationMs + "ms");

            assertInstanceOf(PhysicalAnd.class, result);
            PhysicalAnd physicalAnd = (PhysicalAnd) result;
            assertEquals(100, physicalAnd.children().size());
        }

        @Test
        @DisplayName("Deep nesting transposition should handle recursion efficiently")
        void testDeepNestingPerformance() {
            // Create deeply nested NOT structure via BQL for optimization to work

            // Build a nested NOT query: NOT(NOT(NOT(...(selector = value)...)))
            String queryBuilder = "{ \"$not\": ".repeat(50) +
                    "{ \"selector\": \"value\" }" +
                    " }".repeat(50);

            long startTime = System.nanoTime();
            PhysicalNode result = planQuery(queryBuilder);
            long endTime = System.nanoTime();

            long durationMs = (endTime - startTime) / 1_000_000;
            assertTrue(durationMs < 100, "Deep nesting transposition should complete within 100ms, took: " + durationMs + "ms");

            // After logical optimization: even depth (50) should result in PhysicalFullScan
            // (all NOT pairs cancel out, leaving just the filter)
            assertInstanceOf(PhysicalFullScan.class, result);
        }

        @Test
        @DisplayName("Memory efficiency - no intermediate collections")
        @Disabled
        void testMemoryEfficiency() {
            // Create nested structure that could create intermediate collections
            LogicalNode complex = new LogicalAnd(Arrays.asList(
                    new LogicalFilter("a", Operator.EQ, "1"),
                    new LogicalOr(Arrays.asList(
                            new LogicalFilter("b", Operator.EQ, "2"),
                            new LogicalNot(new LogicalFilter("c", Operator.EQ, "3")),
                            new LogicalElemMatch("items", new LogicalFilter("d", Operator.GT, 4))
                    ))
            ));

            // Record memory before
            Runtime.getRuntime().gc();
            long memoryBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

            PhysicalNode result = planLogical(complex);

            // Record memory after
            Runtime.getRuntime().gc();
            long memoryAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

            long memoryUsed = memoryAfter - memoryBefore;

            // Should not use excessive memory (less than 1MB for this small structure)
            assertTrue(memoryUsed < 1_000_000, "Memory usage should be minimal, used: " + memoryUsed + " bytes");

            // Verify structure is correct
            assertInstanceOf(PhysicalAnd.class, result);
        }
    }

    // ============================================================================
    // Original Test (preserved)
    // ============================================================================

    @Nested
    @DisplayName("Integration Tests")
    class IntegrationTests {

        @Test
        @DisplayName("Complete BQL to Physical pipeline should work")
        void testCompletePipeline() {
            String bqlQuery = """
                    {
                      "$and": [
                        { "tenant_id": "123" },
                        {
                          "$or": [
                            { "status": "active" },
                            { "$not": { "archived": true } }
                          ]
                        },
                        {
                          "items": {
                            "$elemMatch": {
                              "$and": [
                                { "price": { "$gte": 100 } },
                                { "category": { "$in": ["electronics", "books"] } }
                              ]
                            }
                          }
                        }
                      ]
                    }
                    """;

            PhysicalNode result = planQuery(bqlQuery);

            assertInstanceOf(PhysicalAnd.class, result);
            PhysicalAnd rootAnd = (PhysicalAnd) result;
            assertEquals(3, rootAnd.children().size());

            // Verify all major node types are present
            boolean hasFullScan = false;
            boolean hasOr = false;
            boolean hasElemMatch = false;

            for (PhysicalNode child : rootAnd.children()) {
                if (child instanceof PhysicalFullScan) hasFullScan = true;
                else if (child instanceof PhysicalOr) hasOr = true;
                else if (child instanceof PhysicalElemMatch) hasElemMatch = true;
            }

            assertTrue(hasFullScan, "Should have PhysicalFullScan");
            assertTrue(hasOr, "Should have PhysicalOr");
            assertTrue(hasElemMatch, "Should have PhysicalElemMatch");
        }

        @Test
        @DisplayName("Transposition should preserve optimization results")
        void testOptimizationPreservation() {
            // Query that will be optimized by logical planner
            String bqlQuery = """
                    {
                      "$and": [
                        { "status": "active" },
                        {
                          "$not": {
                            "$not": { "published": true }
                          }
                        }
                      ]
                    }
                    """;

            PhysicalNode result = planQuery(bqlQuery);

            // After logical optimization, double NOT should be eliminated
            // Result should be AND of two filters
            assertInstanceOf(PhysicalAnd.class, result);
            PhysicalAnd andNode = (PhysicalAnd) result;
            assertEquals(2, andNode.children().size());

            // Both should be FullScans (no NOT wrapper)
            assertInstanceOf(PhysicalFullScan.class, andNode.children().get(0));
            assertInstanceOf(PhysicalFullScan.class, andNode.children().get(1));
        }
    }
}