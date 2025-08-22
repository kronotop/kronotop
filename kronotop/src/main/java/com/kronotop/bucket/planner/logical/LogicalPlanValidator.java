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

import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.planner.Operator;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Validates logical plans for structural correctness and operator compatibility.
 *
 * <p><b>Post-Optimization Validator</b>: This validator is designed to work with optimized plans
 * where semantic issues (contradictions, tautologies, double negations) have already been
 * resolved by the LogicalPlanner's optimization transforms.
 *
 * <p>This validator focuses on:
 * <ul>
 *   <li><b>Structural Integrity</b>: Null checks, malformed trees</li>
 *   <li><b>Operator Compatibility</b>: Type mismatches, invalid operator-operand combinations</li>
 *   <li><b>Optimization Quality</b>: Detecting unoptimized patterns that should have been transformed</li>
 *   <li><b>Constant Nodes</b>: Validating LogicalTrue/LogicalFalse usage</li>
 * </ul>
 *
 * <p><b>Note</b>: For pre-optimization validation, use an unoptimized LogicalPlanner instance.
 */
public final class LogicalPlanValidator {

    /**
     * Validates a logical plan and returns detailed results.
     */
    public ValidationResult validate(LogicalNode plan) {
        Objects.requireNonNull(plan, "plan must not be null");

        List<ValidationIssue> issues = new ArrayList<>();

        // Check for structural validity
        checkStructuralValidity(plan, issues);

        // Check for operator compatibility and type mismatches
        checkOperatorCompatibility(plan, issues);

        // Check for unoptimized patterns (should have been eliminated by planner)
        checkOptimizationQuality(plan, issues);

        // Check for proper constant node usage
        checkConstantNodes(plan, issues);

        boolean isValid = issues.stream().noneMatch(issue -> issue.severity() == Severity.ERROR);

        return new ValidationResult(isValid, issues);
    }

    /**
     * Quick check if a plan is well-formed (no structural errors).
     */
    public boolean isWellFormed(LogicalNode plan) {
        ValidationResult result = validate(plan);
        return result.valid();
    }

    /**
     * Finds optimization quality issues in a logical plan.
     * These are patterns that should have been eliminated by the optimizer.
     */
    public List<ValidationIssue> findOptimizationIssues(LogicalNode plan) {
        List<ValidationIssue> issues = new ArrayList<>();
        checkOptimizationQuality(plan, issues);
        return issues;
    }

    private void checkStructuralValidity(LogicalNode node, List<ValidationIssue> issues) {
        switch (node) {
            case LogicalAnd(List<LogicalNode> children) -> {
                if (children == null) {
                    issues.add(new ValidationIssue(Severity.ERROR,
                            "LogicalAnd has null children list", null, node));
                } else {
                    for (LogicalNode child : children) {
                        if (child == null) {
                            issues.add(new ValidationIssue(Severity.ERROR,
                                    "LogicalAnd contains null child", null, node));
                        } else {
                            checkStructuralValidity(child, issues);
                        }
                    }
                }
            }
            case LogicalOr(List<LogicalNode> children) -> {
                if (children == null) {
                    issues.add(new ValidationIssue(Severity.ERROR,
                            "LogicalOr has null children list", null, node));
                } else {
                    for (LogicalNode child : children) {
                        if (child == null) {
                            issues.add(new ValidationIssue(Severity.ERROR,
                                    "LogicalOr contains null child", null, node));
                        } else {
                            checkStructuralValidity(child, issues);
                        }
                    }
                }
            }
            case LogicalNot(LogicalNode child) -> {
                if (child == null) {
                    issues.add(new ValidationIssue(Severity.ERROR,
                            "LogicalNot has null child", null, node));
                } else {
                    checkStructuralValidity(child, issues);
                }
            }
            case LogicalElemMatch(String selector, LogicalNode subPlan) -> {
                if (selector == null) {
                    issues.add(new ValidationIssue(Severity.ERROR,
                            "LogicalElemMatch has null selector", null, node));
                }
                if (subPlan == null) {
                    issues.add(new ValidationIssue(Severity.ERROR,
                            "LogicalElemMatch has null subPlan", selector, node));
                } else {
                    checkStructuralValidity(subPlan, issues);
                }
            }
            case LogicalFilter(String selector, Operator op, Object operand) -> {
                if (selector == null) {
                    issues.add(new ValidationIssue(Severity.ERROR,
                            "LogicalFilter has null selector", null, node));
                }
                if (op == null) {
                    issues.add(new ValidationIssue(Severity.ERROR,
                            "LogicalFilter has null operator", selector, node));
                }
                if (operand == null) {
                    issues.add(new ValidationIssue(Severity.ERROR,
                            "LogicalFilter has null operand", selector, node));
                }
            }
            case LogicalTrue ignored -> {
                // LogicalTrue is a valid constant node
            }
            case LogicalFalse ignored -> {
                // LogicalFalse is a valid constant node
            }
        }
    }

    /**
     * Checks for operator compatibility and type mismatches.
     */
    private void checkOperatorCompatibility(LogicalNode node, List<ValidationIssue> issues) {
        switch (node) {
            case LogicalAnd(List<LogicalNode> children) -> {
                if (children != null) {
                    for (LogicalNode child : children) {
                        if (child != null) {
                            checkOperatorCompatibility(child, issues);
                        }
                    }
                }
            }
            case LogicalOr(List<LogicalNode> children) -> {
                if (children != null) {
                    for (LogicalNode child : children) {
                        if (child != null) {
                            checkOperatorCompatibility(child, issues);
                        }
                    }
                }
            }
            case LogicalNot(LogicalNode child) -> {
                if (child != null) {
                    checkOperatorCompatibility(child, issues);
                }
            }
            case LogicalElemMatch(String selector, LogicalNode subPlan) -> {
                if (subPlan != null) {
                    checkOperatorCompatibility(subPlan, issues);
                }
            }
            case LogicalFilter(String selector, Operator op, Object operand) ->
                    checkFilterOperatorCompatibility(selector, op, operand, issues);
            case LogicalTrue ignored -> {
                // LogicalTrue is always valid
            }
            case LogicalFalse ignored -> {
                // LogicalFalse is always valid
            }
        }
    }

    /**
     * Checks individual filter for operator-operand compatibility.
     */
    private void checkFilterOperatorCompatibility(String selector, Operator op, Object operand, List<ValidationIssue> issues) {
        if (op == null || operand == null) {
            return; // Already reported as structural error
        }

        switch (op) {
            case IN, NIN, ALL -> {
                if (!(operand instanceof List)) {
                    issues.add(new ValidationIssue(Severity.ERROR,
                            op + " operator requires list operand", selector, null));
                } else {
                    @SuppressWarnings("unchecked")
                    List<Object> list = (List<Object>) operand;
                    if (list.isEmpty()) {
                        issues.add(new ValidationIssue(Severity.WARNING,
                                op + " operator with empty list", selector, null));
                    }
                }
            }
            case SIZE -> {
                if (!isIntegerOperand(operand)) {
                    issues.add(new ValidationIssue(Severity.ERROR,
                            "SIZE operator requires integer operand", selector, null));
                } else if (extractLongValue(operand) < 0) {
                    issues.add(new ValidationIssue(Severity.ERROR,
                            "SIZE operator requires non-negative integer", selector, null));
                }
            }
            case EXISTS -> {
                if (!isBooleanOperand(operand)) {
                    issues.add(new ValidationIssue(Severity.ERROR,
                            "EXISTS operator requires boolean operand", selector, null));
                }
            }
            case GT, GTE, LT, LTE -> {
                if (operand instanceof List) {
                    issues.add(new ValidationIssue(Severity.WARNING,
                            "Comparison operator " + op + " with list operand may not behave as expected", selector, null));
                }
            }
            case EQ, NE -> {
                // EQ and NE can work with any operand type
            }
        }
    }

    // ────────────────────────────────────────────────────────────────────────────────
    // Private validation methods
    // ────────────────────────────────────────────────────────────────────────────────

    /**
     * Checks for patterns that should have been optimized away.
     */
    private void checkOptimizationQuality(LogicalNode node, List<ValidationIssue> issues) {
        switch (node) {
            case LogicalAnd(List<LogicalNode> children) -> {
                if (children != null) {
                    if (children.isEmpty()) {
                        issues.add(new ValidationIssue(Severity.WARNING,
                                "Empty AND should have been optimized to TRUE", null, node));
                    } else if (children.size() == 1) {
                        issues.add(new ValidationIssue(Severity.WARNING,
                                "Single-child AND should have been flattened", null, node));
                    }

                    // Check for nested AND/OR that should be flattened
                    for (LogicalNode child : children) {
                        if (child instanceof LogicalAnd) {
                            issues.add(new ValidationIssue(Severity.WARNING,
                                    "Nested AND should have been flattened", null, node));
                        }
                        if (child != null) {
                            checkOptimizationQuality(child, issues);
                        }
                    }
                }
            }
            case LogicalOr(List<LogicalNode> children) -> {
                if (children != null) {
                    if (children.isEmpty()) {
                        issues.add(new ValidationIssue(Severity.WARNING,
                                "Empty OR should have been optimized to FALSE", null, node));
                    } else if (children.size() == 1) {
                        issues.add(new ValidationIssue(Severity.WARNING,
                                "Single-child OR should have been flattened", null, node));
                    }

                    // Check for nested OR that should be flattened
                    for (LogicalNode child : children) {
                        if (child instanceof LogicalOr) {
                            issues.add(new ValidationIssue(Severity.WARNING,
                                    "Nested OR should have been flattened", null, node));
                        }
                        if (child != null) {
                            checkOptimizationQuality(child, issues);
                        }
                    }
                }
            }
            case LogicalNot(LogicalNode child) -> {
                if (child != null) {
                    // Check for double negation
                    if (child instanceof LogicalNot) {
                        issues.add(new ValidationIssue(Severity.WARNING,
                                "Double negation should have been optimized away", null, node));
                    }
                    checkOptimizationQuality(child, issues);
                }
            }
            case LogicalElemMatch(String selector, LogicalNode subPlan) -> {
                if (subPlan != null) {
                    checkOptimizationQuality(subPlan, issues);
                }
            }
            case LogicalFilter ignored -> {
                // Individual filters are fine
            }
            case LogicalTrue ignored -> {
                // LogicalTrue is an optimized constant
            }
            case LogicalFalse ignored -> {
                // LogicalFalse is an optimized constant
            }
        }
    }

    /**
     * Checks for proper usage of constant nodes.
     */
    private void checkConstantNodes(LogicalNode node, List<ValidationIssue> issues) {
        switch (node) {
            case LogicalAnd(List<LogicalNode> children) -> {
                if (children != null) {
                    for (LogicalNode child : children) {
                        if (child instanceof LogicalFalse) {
                            issues.add(new ValidationIssue(Severity.WARNING,
                                    "AND containing FALSE should have been optimized to FALSE", null, node));
                        } else if (child instanceof LogicalTrue) {
                            issues.add(new ValidationIssue(Severity.WARNING,
                                    "AND containing TRUE should have been optimized away", null, node));
                        }
                        if (child != null) {
                            checkConstantNodes(child, issues);
                        }
                    }
                }
            }
            case LogicalOr(List<LogicalNode> children) -> {
                if (children != null) {
                    for (LogicalNode child : children) {
                        if (child instanceof LogicalTrue) {
                            issues.add(new ValidationIssue(Severity.WARNING,
                                    "OR containing TRUE should have been optimized to TRUE", null, node));
                        } else if (child instanceof LogicalFalse) {
                            issues.add(new ValidationIssue(Severity.WARNING,
                                    "OR containing FALSE should have been optimized away", null, node));
                        }
                        if (child != null) {
                            checkConstantNodes(child, issues);
                        }
                    }
                }
            }
            case LogicalNot(LogicalNode child) -> {
                if (child instanceof LogicalTrue) {
                    issues.add(new ValidationIssue(Severity.WARNING,
                            "NOT(TRUE) should have been optimized to FALSE", null, node));
                } else if (child instanceof LogicalFalse) {
                    issues.add(new ValidationIssue(Severity.WARNING,
                            "NOT(FALSE) should have been optimized to TRUE", null, node));
                }
                if (child != null) {
                    checkConstantNodes(child, issues);
                }
            }
            case LogicalElemMatch(String selector, LogicalNode subPlan) -> {
                if (subPlan instanceof LogicalFalse) {
                    issues.add(new ValidationIssue(Severity.WARNING,
                            "ElemMatch with FALSE condition should have been optimized to FALSE", selector, node));
                } else if (subPlan instanceof LogicalTrue) {
                    issues.add(new ValidationIssue(Severity.WARNING,
                            "ElemMatch with TRUE condition should have been optimized to TRUE", selector, node));
                }
                if (subPlan != null) {
                    checkConstantNodes(subPlan, issues);
                }
            }
            case LogicalFilter ignored -> {
                // Individual filters don't contain constants
            }
            case LogicalTrue ignored -> {
                // LogicalTrue is valid
            }
            case LogicalFalse ignored -> {
                // LogicalFalse is valid
            }
        }
    }

    /**
     * Helper method to check if operand is a boolean.
     */
    private boolean isBooleanOperand(Object operand) {
        return operand instanceof Boolean || operand instanceof BooleanVal;
    }

    /**
     * Helper method to check if operand is an integer.
     */
    private boolean isIntegerOperand(Object operand) {
        return operand instanceof Integer || operand instanceof Int32Val || operand instanceof Int64Val;
    }

    private boolean isNumericOperand(Object operand) {
        return operand instanceof Number || operand instanceof Int32Val || operand instanceof Int64Val || operand instanceof Decimal128Val || operand instanceof DateTimeVal || operand instanceof TimestampVal;
        // Note: NullVal and BinaryVal are explicitly not numeric
    }

    private boolean isNullOperand(Object operand) {
        return operand instanceof NullVal || operand == null;
    }

    private boolean isBinaryOperand(Object operand) {
        return operand instanceof BinaryVal || operand instanceof byte[];
    }

    /**
     * Extracts integer value from operand, including BqlValue wrappers.
     */
    private long extractLongValue(Object operand) {
        if (operand instanceof Int32Val(int value)) {
            return value;
        } else if (operand instanceof Int64Val(long value)) {
            return value;
        } else if (operand instanceof Integer intValue) {
            return intValue;
        } else if (operand instanceof Long longValue) {
            return longValue;
        }
        throw new IllegalArgumentException("Operand is not an integer: " + operand);
    }

    private Number extractNumericValue(Object operand) {
        if (operand instanceof Int32Val(int value)) {
            return value;
        } else if (operand instanceof Int64Val(long value)) {
            return value;
        } else if (operand instanceof Decimal128Val(java.math.BigDecimal value)) {
            return value;
        } else if (operand instanceof DateTimeVal(long value)) {
            return value;
        } else if (operand instanceof TimestampVal(long value)) {
            return value;
        } else if (operand instanceof Number number) {
            return number;
        } else if (operand instanceof NullVal) {
            throw new IllegalArgumentException("Null values are not numeric");
        } else if (operand instanceof BinaryVal) {
            throw new IllegalArgumentException("Binary values are not numeric");
        }
        throw new IllegalArgumentException("Operand is not numeric: " + operand);
    }

    // ────────────────────────────────────────────────────────────────────────────────
    // Helper methods
    // ────────────────────────────────────────────────────────────────────────────────

    /**
     * Severity levels for validation issues.
     */
    public enum Severity {
        ERROR,    // Plan is invalid and cannot be executed
        WARNING   // Plan is valid but may be suboptimal
    }

    /**
     * Represents the result of a validation operation.
     */
    public record ValidationResult(boolean valid, List<ValidationIssue> issues) {
        public ValidationResult(boolean valid, List<ValidationIssue> issues) {
            this.valid = valid;
            this.issues = List.copyOf(issues);
        }

        public boolean hasErrors() {
            return issues.stream().anyMatch(issue -> issue.severity() == Severity.ERROR);
        }

        public boolean hasWarnings() {
            return issues.stream().anyMatch(issue -> issue.severity() == Severity.WARNING);
        }

        @Nonnull
        @Override
        public String toString() {
            if (valid && issues.isEmpty()) {
                return "ValidationResult: VALID (no issues)";
            }

            StringBuilder sb = new StringBuilder();
            sb.append("ValidationResult: ").append(valid ? "VALID" : "INVALID");
            sb.append(" (").append(issues.size()).append(" issues)\n");

            for (ValidationIssue issue : issues) {
                sb.append("  ").append(issue).append("\n");
            }

            return sb.toString();
        }
    }

    /**
     * Represents a single validation issue found in a logical plan.
     */
    public record ValidationIssue(
            Severity severity,
            String message,
            String selector,
            LogicalNode node
    ) {
        @Nonnull
        @Override
        public String toString() {
            return String.format("%s: %s (selector=%s, node=%s)",
                    severity, message, selector != null ? selector : "N/A",
                    node != null ? node.getClass().getSimpleName() : "N/A");
        }
    }
}