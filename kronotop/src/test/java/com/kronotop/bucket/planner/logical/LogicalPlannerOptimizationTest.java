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
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.planner.Operator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Comprehensive test suite for LogicalPlanner optimization passes.
 * Tests the new advanced optimization transforms: contradiction detection,
 * tautology elimination, and constant folding.
 */
class LogicalPlannerOptimizationTest {

    private LogicalPlanner planner;

    @BeforeEach
    void setUp() {
        planner = new LogicalPlanner();
    }

    // ============================================================================
    // Contradiction Detection Transform Tests
    // ============================================================================

    @Nested
    @DisplayName("Contradiction Detection Transform Tests")
    class ContradictionDetectionTests {

        @Test
        @DisplayName("Simple equality contradiction should become FALSE")
        void shouldDetectSimpleEqualityContradiction() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "status": "active" },
                        { "status": "inactive" }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFalse.class, result);
            assertEquals("FALSE", result.toString());
        }

        @Test
        @DisplayName("Equality vs not-equal contradiction should become FALSE")
        void shouldDetectEqualityVsNotEqualContradiction() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "category": "electronics" },
                        { "category": { "$ne": "electronics" } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFalse.class, result);
        }

        @Test
        @DisplayName("Numeric range contradiction should become FALSE")
        void shouldDetectNumericRangeContradiction() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "price": { "$gt": 100 } },
                        { "price": { "$lt": 50 } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFalse.class, result);
        }

        @Test
        @DisplayName("Complex numeric contradiction should become FALSE")
        void shouldDetectComplexNumericContradiction() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "age": { "$gte": 25 } },
                        { "age": { "$lt": 25 } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFalse.class, result);
        }

        @Test
        @DisplayName("AND with contradictory conditions and valid condition should become FALSE")
        void shouldDetectMixedContradictionAndValidCondition() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "tenant_id": "123" },
                        { "status": "active" },
                        { "status": "deleted" },
                        { "priority": { "$gt": 5 } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFalse.class, result);
        }

        @Test
        @DisplayName("Nested contradiction should propagate to root")
        void shouldPropagateNestedContradiction() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "tenant_id": "123" },
                        {
                          "$and": [
                            { "status": "active" },
                            { "status": "inactive" }
                          ]
                        }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFalse.class, result);
        }

        @Test
        @DisplayName("OR with contradictory conditions should remain OR")
        void shouldPreserveContradictionInOr() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$or": [
                        { "status": "active" },
                        { "status": "inactive" }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // OR with contradictory conditions is valid and should not become FALSE
            assertInstanceOf(LogicalOr.class, result);
            LogicalOr orNode = (LogicalOr) result;
            assertEquals(2, orNode.children().size());
        }

        @Test
        @DisplayName("No contradiction between different selectors")
        void shouldNotDetectContradictionForDifferentSelectors() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "status": "active" },
                        { "category": "active" }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Different selectors with same values should not be contradictory
            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(2, andNode.children().size());
        }

        @Test
        @DisplayName("Compatible numeric conditions should not become FALSE")
        void shouldPreserveCompatibleNumericConditions() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "price": { "$gte": 100 } },
                        { "price": { "$lte": 200 } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Compatible range (100 <= price <= 200) should remain
            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(2, andNode.children().size());
        }
    }

    // ============================================================================
    // Tautology Elimination Transform Tests
    // ============================================================================

    @Nested
    @DisplayName("Tautology Elimination Transform Tests")
    class TautologyEliminationTests {

        @Test
        @DisplayName("EQ vs NE tautology should become TRUE")
        void shouldDetectEqualityVsNotEqualTautology() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$or": [
                        { "status": "active" },
                        { "status": { "$ne": "active" } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // selector = A OR selector != A is always TRUE
            assertInstanceOf(LogicalTrue.class, result);
            assertEquals("TRUE", result.toString());
        }

        @Test
        @DisplayName("Numeric range tautology should become TRUE")
        void shouldDetectNumericRangeTautology() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$or": [
                        { "age": { "$gt": 25 } },
                        { "age": { "$lte": 25 } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // age > 25 OR age <= 25 covers all possible values
            assertInstanceOf(LogicalTrue.class, result);
        }

        @Test
        @DisplayName("GTE vs LT tautology should become TRUE")
        void shouldDetectGteVsLtTautology() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$or": [
                        { "score": { "$gte": 100 } },
                        { "score": { "$lt": 100 } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // score >= 100 OR score < 100 covers all possible values
            assertInstanceOf(LogicalTrue.class, result);
        }

        @Test
        @DisplayName("OR with TRUE child should become TRUE")
        void shouldSimplifyOrWithTrueChild() {
            // This tests constant folding in OR context
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$or": [
                        { "selector1": "value1" },
                        {
                          "$or": [
                            { "status": "active" },
                            { "status": { "$ne": "active" } }
                          ]
                        }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // The nested OR is a tautology (TRUE), so the entire OR becomes TRUE
            assertInstanceOf(LogicalTrue.class, result);
        }

        @Test
        @DisplayName("AND with TRUE child should simplify")
        void shouldSimplifyAndWithTrueChild() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "selector1": "value1" },
                        {
                          "$or": [
                            { "status": "active" },
                            { "status": { "$ne": "active" } }
                          ]
                        }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // The nested OR is TRUE, so AND(selector1=value1, TRUE) = selector1=value1
            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("selector1", filter.selector());
            assertEquals(Operator.EQ, filter.op());
        }

        @Test
        @DisplayName("No tautology in AND context")
        void shouldNotDetectTautologyInAnd() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "status": "active" },
                        { "status": { "$ne": "active" } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // In AND context, status = active AND status != active is a contradiction, not tautology
            assertInstanceOf(LogicalFalse.class, result);
        }
    }

    // ============================================================================
    // Constant Folding Transform Tests
    // ============================================================================

    @Nested
    @DisplayName("Constant Folding Transform Tests")
    class ConstantFoldingTests {

        @Test
        @DisplayName("AND with FALSE should become FALSE")
        void shouldFoldAndWithFalse() {
            // Create a plan that would result in AND(condition, FALSE)
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "selector1": "value1" },
                        {
                          "$and": [
                            { "status": "active" },
                            { "status": "deleted" }
                          ]
                        }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // The nested AND is FALSE (contradiction), so entire AND becomes FALSE
            assertInstanceOf(LogicalFalse.class, result);
        }

        @Test
        @DisplayName("OR with TRUE should become TRUE")
        void shouldFoldOrWithTrue() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$or": [
                        { "selector1": "value1" },
                        {
                          "$or": [
                            { "status": "active" },
                            { "status": { "$ne": "active" } }
                          ]
                        }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // The nested OR is TRUE (tautology), so entire OR becomes TRUE
            assertInstanceOf(LogicalTrue.class, result);
        }

        @Test
        @DisplayName("AND with only TRUE children should become TRUE")
        void shouldFoldAndWithOnlyTrueChildren() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        {
                          "$or": [
                            { "selector1": "value1" },
                            { "selector1": { "$ne": "value1" } }
                          ]
                        },
                        {
                          "$or": [
                            { "selector2": { "$gt": 0 } },
                            { "selector2": { "$lte": 0 } }
                          ]
                        }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Both OR conditions are tautologies (TRUE), so AND(TRUE, TRUE) = TRUE
            assertInstanceOf(LogicalTrue.class, result);
        }

        @Test
        @DisplayName("OR with only FALSE children should become FALSE")
        void shouldFoldOrWithOnlyFalseChildren() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$or": [
                        {
                          "$and": [
                            { "selector1": "value1" },
                            { "selector1": "value2" }
                          ]
                        },
                        {
                          "$and": [
                            { "selector2": { "$gt": 100 } },
                            { "selector2": { "$lt": 50 } }
                          ]
                        }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Both AND conditions are contradictions (FALSE), so OR(FALSE, FALSE) = FALSE
            assertInstanceOf(LogicalFalse.class, result);
        }

        @Test
        @DisplayName("NOT(TRUE) should become FALSE")
        void shouldFoldNotTrue() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$not": {
                        "$or": [
                          { "status": "active" },
                          { "status": { "$ne": "active" } }
                        ]
                      }
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // NOT(TRUE) = FALSE
            assertInstanceOf(LogicalFalse.class, result);
        }

        @Test
        @DisplayName("NOT(FALSE) should become TRUE")
        void shouldFoldNotFalse() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$not": {
                        "$and": [
                          { "status": "active" },
                          { "status": "deleted" }
                        ]
                      }
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // NOT(FALSE) = TRUE
            assertInstanceOf(LogicalTrue.class, result);
        }

        @Test
        @DisplayName("ElemMatch with FALSE condition should become FALSE")
        void shouldFoldElemMatchWithFalse() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "items": {
                        "$elemMatch": {
                          "$and": [
                            { "status": "active" },
                            { "status": "deleted" }
                          ]
                        }
                      }
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // ElemMatch with FALSE condition is FALSE
            assertInstanceOf(LogicalFalse.class, result);
        }

        @Test
        @DisplayName("ElemMatch with TRUE condition should become TRUE")
        void shouldFoldElemMatchWithTrue() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "items": {
                        "$elemMatch": {
                          "$or": [
                            { "status": "active" },
                            { "status": { "$ne": "active" } }
                          ]
                        }
                      }
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // ElemMatch with TRUE condition is TRUE
            assertInstanceOf(LogicalTrue.class, result);
        }
    }

    // ============================================================================
    // Transform Pipeline Integration Tests
    // ============================================================================

    @Nested
    @DisplayName("Transform Pipeline Integration Tests")
    class TransformPipelineIntegrationTests {

        @Test
        @DisplayName("Complex optimization pipeline should work correctly")
        void shouldHandleComplexOptimizationPipeline() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "tenant_id": "123" },
                        {
                          "$and": [
                            { "status": "active" },
                            {
                              "$or": [
                                { "role": "admin" },
                                { "role": { "$ne": "admin" } }
                              ]
                            }
                          ]
                        },
                        {
                          "$not": {
                            "$not": { "deleted": false }
                          }
                        }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // After all optimizations:
            // 1. FlattenAndOrTransform: Flattens nested ANDs
            // 2. RemoveDoubleNotTransform: NOT(NOT(deleted=false)) → deleted=false
            // 3. TautologyEliminationTransform: OR(role=admin, role!=admin) → TRUE
            // 4. ConstantFoldingTransform: AND(tenant_id=123, status=active, TRUE, deleted=false) 
            //    → AND(tenant_id=123, status=active, deleted=false)

            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(3, andNode.children().size());

            // All children should be filters
            for (LogicalNode child : andNode.children()) {
                assertInstanceOf(LogicalFilter.class, child);
            }
        }

        @Test
        @DisplayName("Optimization with custom pipeline should work")
        void shouldOptimizeWithCustomPipeline() {
            // Create planner with only basic transforms (no advanced optimizations)
            // Note: Inner classes are private, so we test the absence of optimization differently
            LogicalPlanner basicPlanner = new LogicalPlanner(List.of());

            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "status": "active" },
                        { "status": "deleted" }
                      ]
                    }
                    """);

            LogicalNode result = basicPlanner.plan(expr);

            // Without optimization transforms, the contradiction remains undetected
            // and we get the raw AND structure
            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(2, andNode.children().size());
        }

        @Test
        @DisplayName("All optimization passes should be idempotent")
        void shouldBeIdempotent() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "status": "active" },
                        { "status": "deleted" }
                      ]
                    }
                    """);

            // Run optimization multiple times
            LogicalNode result1 = planner.plan(expr);
            LogicalNode result2 = planner.plan(expr);

            // Results should be identical
            assertEquals(result1.toString(), result2.toString());
            assertInstanceOf(LogicalFalse.class, result1);
            assertInstanceOf(LogicalFalse.class, result2);
        }

        @Test
        @DisplayName("Optimization should preserve semantics")
        void shouldPreserveSemantics() {
            // Test that optimization doesn't change the logical meaning
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "tenant_id": "123" },
                        { "priority": { "$gte": 1 } },
                        { "priority": { "$lte": 10 } }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Should preserve all conditions (no contradictions or tautologies)
            assertInstanceOf(LogicalAnd.class, result);
            LogicalAnd andNode = (LogicalAnd) result;
            assertEquals(3, andNode.children().size());

            // All conditions should be preserved
            LogicalFilter tenantFilter = (LogicalFilter) andNode.children().get(0);
            LogicalFilter minPriorityFilter = (LogicalFilter) andNode.children().get(1);
            LogicalFilter maxPriorityFilter = (LogicalFilter) andNode.children().get(2);

            assertEquals("tenant_id", tenantFilter.selector());
            assertEquals("priority", minPriorityFilter.selector());
            assertEquals("priority", maxPriorityFilter.selector());
            assertEquals(Operator.GTE, minPriorityFilter.op());
            assertEquals(Operator.LTE, maxPriorityFilter.op());
        }
    }

    // ============================================================================
    // Edge Cases and Error Conditions
    // ============================================================================

    @Nested
    @DisplayName("Edge Cases and Error Conditions")
    class EdgeCasesTests {

        @Test
        @DisplayName("Empty AND should become TRUE")
        void shouldHandleEmptyAnd() {
            BqlExpr expr = BqlParser.parse("{ \"$and\": [] }");

            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalTrue.class, result);
        }

        @Test
        @DisplayName("Empty OR should become FALSE")
        void shouldHandleEmptyOr() {
            BqlExpr expr = BqlParser.parse("{ \"$or\": [] }");

            LogicalNode result = planner.plan(expr);

            assertInstanceOf(LogicalFalse.class, result);
        }

        @Test
        @DisplayName("Single child AND should be simplified")
        void shouldSimplifySingleChildAnd() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "status": "active" }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Single child AND should be simplified to just the child
            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("status", filter.selector());
            assertEquals(Operator.EQ, filter.op());
        }

        @Test
        @DisplayName("Single child OR should be simplified")
        void shouldSimplifySingleChildOr() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$or": [
                        { "category": "electronics" }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Single child OR should be simplified to just the child
            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("category", filter.selector());
            assertEquals(Operator.EQ, filter.op());
        }

        @Test
        @DisplayName("Very deep nesting should be optimized correctly")
        void shouldOptimizeVeryDeepNesting() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        {
                          "$and": [
                            {
                              "$and": [
                                { "selector1": "value1" },
                                {
                                  "$or": [
                                    { "selector2": "value2" },
                                    { "selector2": { "$ne": "value2" } }
                                  ]
                                }
                              ]
                            }
                          ]
                        }
                      ]
                    }
                    """);

            LogicalNode result = planner.plan(expr);

            // Deep nesting should be flattened and optimized:
            // - AND flattening collapses nested ANDs
            // - OR(selector2=value2, selector2!=value2) becomes TRUE
            // - AND(selector1=value1, TRUE) becomes selector1=value1
            assertInstanceOf(LogicalFilter.class, result);
            LogicalFilter filter = (LogicalFilter) result;
            assertEquals("selector1", filter.selector());
        }
    }
}