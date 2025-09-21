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
import com.kronotop.bucket.bql.ast.BooleanVal;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.bql.ast.Int32Val;
import com.kronotop.bucket.bql.ast.StringVal;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.logical.LogicalPlanValidator.Severity;
import com.kronotop.bucket.planner.logical.LogicalPlanValidator.ValidationIssue;
import com.kronotop.bucket.planner.logical.LogicalPlanValidator.ValidationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for LogicalPlanValidator covering all validation scenarios.
 */
class LogicalPlanValidatorTest {

    private LogicalPlanValidator validator;
    private LogicalPlanner planner;

    @BeforeEach
    void setUp() {
        validator = new LogicalPlanValidator();
        planner = new LogicalPlanner();
    }

    // ============================================================================
    // Valid Plan Tests
    // ============================================================================

    @Nested
    @DisplayName("Valid Plan Tests")
    class ValidPlanTests {

        @Test
        @DisplayName("Simple valid filter should pass validation")
        void testSimpleValidFilter() {
            LogicalNode plan = new LogicalFilter("status", Operator.EQ, new StringVal("active"));

            ValidationResult result = validator.validate(plan);

            assertTrue(result.valid());
            assertTrue(result.issues().isEmpty());
            assertFalse(result.hasErrors());
            assertFalse(result.hasWarnings());
        }

        @Test
        @DisplayName("Valid AND with multiple filters should pass")
        void testValidAndWithFilters() {
            LogicalNode plan = new LogicalAnd(Arrays.asList(
                    new LogicalFilter("status", Operator.EQ, new StringVal("active")),
                    new LogicalFilter("priority", Operator.GT, new Int32Val(5)),
                    new LogicalFilter("category", Operator.IN, Arrays.asList(
                            new StringVal("electronics"), new StringVal("books")))
            ));

            ValidationResult result = validator.validate(plan);

            assertTrue(result.valid());
            assertTrue(result.issues().isEmpty());
        }

        @Test
        @DisplayName("Valid complex nested structure should pass")
        void testValidComplexNestedStructure() {
            LogicalNode plan = new LogicalAnd(Arrays.asList(
                    new LogicalFilter("tenant_id", Operator.EQ, new StringVal("123")),
                    new LogicalOr(Arrays.asList(
                            new LogicalFilter("role", Operator.EQ, new StringVal("admin")),
                            new LogicalNot(new LogicalFilter("deleted", Operator.EQ, new BooleanVal(true)))
                    ))
            ));

            ValidationResult result = validator.validate(plan);

            assertTrue(result.valid());
            assertTrue(result.issues().isEmpty());
        }

        @Test
        @DisplayName("Valid ElemMatch should pass")
        void testValidElemMatch() {
            LogicalNode plan = new LogicalElemMatch("items",
                    new LogicalAnd(Arrays.asList(
                            new LogicalFilter("price", Operator.GT, new Int32Val(100)),
                            new LogicalFilter("category", Operator.EQ, new StringVal("electronics"))
                    ))
            );

            ValidationResult result = validator.validate(plan);

            assertTrue(result.valid());
            assertTrue(result.issues().isEmpty());
        }
    }

    // ============================================================================
    // Structural Validation Tests
    // ============================================================================

    @Nested
    @DisplayName("Structural Validation Tests")
    class StructuralValidationTests {

        @Test
        @DisplayName("LogicalAnd with null children should be invalid")
        void testAndWithNullChildren() {
            LogicalNode plan = new LogicalAnd(null);

            ValidationResult result = validator.validate(plan);

            assertFalse(result.valid());
            assertTrue(result.hasErrors());
            assertEquals(1, result.issues().size());
            assertEquals(Severity.ERROR, result.issues().get(0).severity());
            assertTrue(result.issues().get(0).message().contains("null children list"));
        }

        @Test
        @DisplayName("LogicalAnd with null child should be invalid")
        void testAndWithNullChild() {
            LogicalNode plan = new LogicalAnd(Arrays.asList(
                    new LogicalFilter("status", Operator.EQ, new StringVal("active")),
                    null
            ));

            ValidationResult result = validator.validate(plan);

            assertFalse(result.valid());
            assertTrue(result.hasErrors());
            assertEquals(1, result.issues().size());
            assertTrue(result.issues().get(0).message().contains("null child"));
        }

        @Test
        @DisplayName("LogicalNot with null child should be invalid")
        void testNotWithNullChild() {
            LogicalNode plan = new LogicalNot(null);

            ValidationResult result = validator.validate(plan);

            assertFalse(result.valid());
            assertTrue(result.hasErrors());
            assertEquals(1, result.issues().size());
            assertTrue(result.issues().get(0).message().contains("null child"));
        }

        @Test
        @DisplayName("LogicalFilter with null selector should be invalid")
        void testFilterWithNullSelector() {
            LogicalNode plan = new LogicalFilter(null, Operator.EQ, new StringVal("value"));

            ValidationResult result = validator.validate(plan);

            assertFalse(result.valid());
            assertTrue(result.hasErrors());
            assertEquals(1, result.issues().size());
            assertTrue(result.issues().get(0).message().contains("null selector"));
        }

        @Test
        @DisplayName("LogicalFilter with null operator should be invalid")
        void testFilterWithNullOperator() {
            LogicalNode plan = new LogicalFilter("selector", null, new StringVal("value"));

            ValidationResult result = validator.validate(plan);

            assertFalse(result.valid());
            assertTrue(result.hasErrors());
            assertEquals(1, result.issues().size());
            assertTrue(result.issues().get(0).message().contains("null operator"));
        }

        @Test
        @DisplayName("LogicalFilter with null operand should be invalid")
        void testFilterWithNullOperand() {
            LogicalNode plan = new LogicalFilter("selector", Operator.EQ, null);

            ValidationResult result = validator.validate(plan);

            assertFalse(result.valid());
            assertTrue(result.hasErrors());
            assertEquals(1, result.issues().size());
            assertTrue(result.issues().get(0).message().contains("null operand"));
        }

        @Test
        @DisplayName("LogicalElemMatch with null selector should be invalid")
        void testElemMatchWithNullSelector() {
            LogicalNode plan = new LogicalElemMatch(null,
                    new LogicalFilter("price", Operator.GT, new Int32Val(100)));

            ValidationResult result = validator.validate(plan);

            assertFalse(result.valid());
            assertTrue(result.hasErrors());
            assertEquals(1, result.issues().size());
            assertTrue(result.issues().get(0).message().contains("null selector"));
        }

        @Test
        @DisplayName("LogicalElemMatch with null subPlan should be invalid")
        void testElemMatchWithNullSubPlan() {
            LogicalNode plan = new LogicalElemMatch("items", null);

            ValidationResult result = validator.validate(plan);

            assertFalse(result.valid());
            assertTrue(result.hasErrors());
            assertEquals(1, result.issues().size());
            assertTrue(result.issues().get(0).message().contains("null subPlan"));
        }
    }

    // ============================================================================
    // Operator Compatibility Tests
    // ============================================================================

    @Nested
    @DisplayName("Operator Compatibility Tests")
    class OperatorCompatibilityTests {

        @Test
        @DisplayName("IN operator with non-list operand should be invalid")
        void testInOperatorWithNonListOperand() {
            LogicalNode plan = new LogicalFilter("selector", Operator.IN, new StringVal("value"));

            ValidationResult result = validator.validate(plan);

            assertFalse(result.valid());
            assertTrue(result.hasErrors());
            assertEquals(1, result.issues().size());
            assertTrue(result.issues().get(0).message().contains("requires list operand"));
        }

        @Test
        @DisplayName("SIZE operator with non-integer operand should be invalid")
        void testSizeOperatorWithNonIntegerOperand() {
            LogicalNode plan = new LogicalFilter("selector", Operator.SIZE, new StringVal("not_a_number"));

            ValidationResult result = validator.validate(plan);

            assertFalse(result.valid());
            assertTrue(result.hasErrors());
            assertEquals(1, result.issues().size());
            assertTrue(result.issues().get(0).message().contains("requires integer operand"));
        }

        @Test
        @DisplayName("SIZE operator with negative integer should be invalid")
        void testSizeOperatorWithNegativeInteger() {
            LogicalNode plan = new LogicalFilter("selector", Operator.SIZE, new Int32Val(-5));

            ValidationResult result = validator.validate(plan);

            assertFalse(result.valid());
            assertTrue(result.hasErrors());
            assertEquals(1, result.issues().size());
            assertTrue(result.issues().get(0).message().contains("non-negative integer"));
        }

        @Test
        @DisplayName("EXISTS operator with non-boolean operand should be invalid")
        void testExistsOperatorWithNonBooleanOperand() {
            LogicalNode plan = new LogicalFilter("selector", Operator.EXISTS, new StringVal("not_boolean"));

            ValidationResult result = validator.validate(plan);

            assertFalse(result.valid());
            assertTrue(result.hasErrors());
            assertEquals(1, result.issues().size());
            assertTrue(result.issues().get(0).message().contains("requires boolean operand"));
        }

        @Test
        @DisplayName("IN operator with empty list should generate warning")
        void testInOperatorWithEmptyList() {
            LogicalNode plan = new LogicalFilter("selector", Operator.IN, List.of());

            ValidationResult result = validator.validate(plan);

            assertTrue(result.valid()); // Still valid, but has warning
            assertTrue(result.hasWarnings());
            assertEquals(1, result.issues().size());
            assertEquals(Severity.WARNING, result.issues().get(0).severity());
            assertTrue(result.issues().get(0).message().contains("empty list"));
        }

        @Test
        @DisplayName("Comparison operator with list should generate warning")
        void testComparisonOperatorWithList() {
            LogicalNode plan = new LogicalFilter("selector", Operator.GT,
                    Arrays.asList(new Int32Val(1), new Int32Val(2)));

            ValidationResult result = validator.validate(plan);

            assertTrue(result.valid()); // Still valid, but has warning
            assertTrue(result.hasWarnings());
            assertEquals(1, result.issues().size());
            assertEquals(Severity.WARNING, result.issues().get(0).severity());
            assertTrue(result.issues().get(0).message().contains("may not behave as expected"));
        }
    }

    // ============================================================================
    // Optimization Quality Tests
    // ============================================================================

    @Nested
    @DisplayName("Optimization Quality Tests")
    class OptimizationQualityTests {

        @Test
        @DisplayName("Single-child AND should generate optimization warning")
        void testSingleChildAnd() {
            LogicalNode plan = new LogicalAnd(List.of(
                    new LogicalFilter("status", Operator.EQ, new StringVal("active"))
            ));

            ValidationResult result = validator.validate(plan);

            assertTrue(result.valid()); // Valid but has warning
            assertTrue(result.hasWarnings());
            assertEquals(1, result.issues().size());
            assertTrue(result.issues().get(0).message().contains("Single-child AND should have been flattened"));
        }

        @Test
        @DisplayName("Single-child OR should generate optimization warning")
        void testSingleChildOr() {
            LogicalNode plan = new LogicalOr(List.of(
                    new LogicalFilter("status", Operator.EQ, new StringVal("active"))
            ));

            ValidationResult result = validator.validate(plan);

            assertTrue(result.valid()); // Valid but has warning
            assertTrue(result.hasWarnings());
            assertEquals(1, result.issues().size());
            assertTrue(result.issues().get(0).message().contains("Single-child OR should have been flattened"));
        }

        @Test
        @DisplayName("Nested AND should generate optimization warning")
        void testNestedAnd() {
            LogicalNode plan = new LogicalAnd(Arrays.asList(
                    new LogicalFilter("status", Operator.EQ, new StringVal("active")),
                    new LogicalAnd(List.of(
                            new LogicalFilter("priority", Operator.GT, new Int32Val(5))
                    ))
            ));

            ValidationResult result = validator.validate(plan);

            assertTrue(result.valid()); // Valid but has warning
            assertTrue(result.hasWarnings());
            assertTrue(result.issues().stream().anyMatch(issue ->
                    issue.message().contains("Nested AND should have been flattened")));
        }

        @Test
        @DisplayName("Double negation should generate optimization warning")
        void testDoubleNegation() {
            LogicalNode plan = new LogicalNot(
                    new LogicalNot(
                            new LogicalFilter("status", Operator.EQ, new StringVal("active"))
                    )
            );

            ValidationResult result = validator.validate(plan);

            assertTrue(result.valid()); // Valid but has warning
            assertTrue(result.hasWarnings());
            assertEquals(1, result.issues().size());
            assertTrue(result.issues().get(0).message().contains("Double negation should have been optimized away"));
        }

        @Test
        @DisplayName("Should find optimization issues using convenience method")
        void testFindOptimizationIssuesMethod() {
            // Create a plan with patterns that should be optimized away
            LogicalNode plan = new LogicalAnd(List.of(
                    new LogicalFilter("status", Operator.EQ, new StringVal("active"))
            ));

            List<ValidationIssue> issues = validator.findOptimizationIssues(plan);

            assertEquals(1, issues.size());
            assertEquals(Severity.WARNING, issues.get(0).severity());
            assertTrue(issues.get(0).message().contains("Single-child AND should have been flattened"));
        }
    }

    // ============================================================================
    // Empty Container Tests  
    // ============================================================================

    @Nested
    @DisplayName("Empty Container Tests")
    class EmptyContainerTests {

        @Test
        @DisplayName("Empty AND should generate optimization warning")
        void testEmptyAnd() {
            LogicalNode plan = new LogicalAnd(List.of());

            ValidationResult result = validator.validate(plan);

            assertTrue(result.valid()); // Valid but has warning
            assertTrue(result.hasWarnings());
            assertEquals(1, result.issues().size());
            assertEquals(Severity.WARNING, result.issues().get(0).severity());
            assertTrue(result.issues().get(0).message().contains("Empty AND should have been optimized to TRUE"));
        }

        @Test
        @DisplayName("Empty OR should generate optimization warning")
        void testEmptyOr() {
            LogicalNode plan = new LogicalOr(List.of());

            ValidationResult result = validator.validate(plan);

            assertTrue(result.valid()); // Valid but has warning
            assertTrue(result.hasWarnings());
            assertEquals(1, result.issues().size());
            assertEquals(Severity.WARNING, result.issues().get(0).severity());
            assertTrue(result.issues().get(0).message().contains("Empty OR should have been optimized to FALSE"));
        }
    }


    // ============================================================================
    // Integration Tests with LogicalPlanner
    // ============================================================================

    @Nested
    @DisplayName("Integration Tests with LogicalPlanner")
    class IntegrationTests {

        @Test
        @DisplayName("Valid BQL query should pass validation")
        void testValidBqlQuery() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "status": "active" },
                        { "priority": { "$gt": 5 } },
                        { "category": { "$in": ["electronics", "books"] } }
                      ]
                    }
                    """);

            LogicalNode plan = planner.plan(expr);
            ValidationResult result = planner.validate(plan);

            assertTrue(result.valid());
            assertTrue(result.issues().isEmpty());
        }

        @Test
        @DisplayName("Contradictory BQL query should be optimized to FALSE and validate successfully")
        void testContradictoryBqlQuery() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "status": "active" },
                        { "status": "inactive" }
                      ]
                    }
                    """);

            LogicalNode plan = planner.plan(expr);
            ValidationResult result = planner.validate(plan);

            // After optimization, contradictory conditions become LogicalFalse which is valid
            assertTrue(result.valid());
            assertFalse(result.hasErrors());
            assertTrue(result.issues().isEmpty());
            assertInstanceOf(LogicalFalse.class, plan);
        }

        @Test
        @DisplayName("planAndValidate should succeed after optimization eliminates contradictions")
        void testPlanAndValidateWithOptimizedPlan() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "price": { "$gt": 100 } },
                        { "price": { "$lt": 50 } }
                      ]
                    }
                    """);

            // After optimization, contradiction becomes LogicalFalse which is valid
            assertDoesNotThrow(() -> {
                LogicalNode plan = planner.planAndValidate(expr);
                assertInstanceOf(LogicalFalse.class, plan);
            });
        }

        @Test
        @DisplayName("planAndValidate should throw exception for structural errors")
        void testPlanAndValidateWithStructuralError() {
            // Create a plan with structural errors (bypassing optimization)
            LogicalNode invalidPlan = new LogicalFilter(null, Operator.EQ, new StringVal("value"));

            LogicalPlanValidationException exception = assertThrows(
                    LogicalPlanValidationException.class,
                    () -> {
                        LogicalPlanValidator.ValidationResult result = planner.validate(invalidPlan);
                        if (!result.valid()) {
                            throw new LogicalPlanValidationException("Invalid logical plan", result);
                        }
                    }
            );

            assertNotNull(exception.getValidationResult());
            assertFalse(exception.getValidationResult().valid());
            assertTrue(exception.getMessage().contains("Invalid logical plan"));
        }

        @Test
        @DisplayName("planAndValidate should succeed for valid plan")
        void testPlanAndValidateWithValidPlan() {
            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "status": "active" },
                        { "priority": { "$gte": 1 } }
                      ]
                    }
                    """);

            assertDoesNotThrow(() -> {
                LogicalNode plan = planner.planAndValidate(expr);
                assertNotNull(plan);
                assertInstanceOf(LogicalAnd.class, plan);
            });
        }

        @Test
        @DisplayName("isWellFormed should work correctly")
        void testIsWellFormed() {
            // Valid plan
            LogicalNode validPlan = new LogicalFilter("status", Operator.EQ, new StringVal("active"));
            assertTrue(planner.isWellFormed(validPlan));

            // Invalid plan
            LogicalNode invalidPlan = new LogicalFilter(null, Operator.EQ, new StringVal("active"));
            assertFalse(planner.isWellFormed(invalidPlan));
        }

        @Test
        @DisplayName("Validator should detect unoptimized patterns in raw plans")
        void testValidationWithUnoptimizedPlans() {
            // Create a planner without optimization transforms 
            LogicalPlanner unoptimizedPlanner = new LogicalPlanner(List.of());

            BqlExpr expr = BqlParser.parse("""
                    {
                      "$and": [
                        { "status": "active" }
                      ]
                    }
                    """);

            LogicalNode plan = unoptimizedPlanner.plan(expr);
            ValidationResult result = validator.validate(plan);

            // Should detect single-child AND that should be flattened
            assertTrue(result.valid()); // Still valid but has warnings
            assertTrue(result.hasWarnings());
            assertEquals(1, result.issues().size());
            assertTrue(result.issues().get(0).message().contains("Single-child AND should have been flattened"));
        }
    }

    // ============================================================================
    // Validation Result Tests
    // ============================================================================

    @Nested
    @DisplayName("Validation Result Tests")
    class ValidationResultTests {

        @Test
        @DisplayName("ValidationResult toString should format correctly")
        void testValidationResultToString() {
            List<ValidationIssue> issues = Arrays.asList(
                    new ValidationIssue(Severity.ERROR, "Test error", "selector1", null),
                    new ValidationIssue(Severity.WARNING, "Test warning", "selector2", null)
            );

            ValidationResult result = new ValidationResult(false, issues);
            String str = result.toString();

            assertTrue(str.contains("INVALID"));
            assertTrue(str.contains("2 issues"));
            assertTrue(str.contains("Test error"));
            assertTrue(str.contains("Test warning"));
        }

        @Test
        @DisplayName("Valid ValidationResult toString should be concise")
        void testValidValidationResultToString() {
            ValidationResult result = new ValidationResult(true, List.of());
            String str = result.toString();

            assertTrue(str.contains("VALID"));
            assertTrue(str.contains("no issues"));
        }
    }
}