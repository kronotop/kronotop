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

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.planner.Operator;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

// ──────────────────────────────────────────────────────────────────────────────────
// Logical plan node hierarchy + visitor
// ──────────────────────────────────────────────────────────────────────────────────

/**
 * LogicalPlanner orchestrates the full <b>logical‑planning pipeline</b>:
 * <ol>
 *   <li>Converts a {@link BqlExpr} (parser AST) into an initial {@link LogicalNode} tree.</li>
 *   <li>Runs a configurable sequence of <i>logical transforms</i> (visitors) that
 *       simplify / normalise the tree while keeping the semantics intact.</li>
 *   <li>Returns the final, optimised logical plan which will later feed the physical planner.</li>
 * </ol>
 * The default pipeline currently performs two semantics‑preserving passes:
 * <ul>
 *   <li><code>FlattenAndOrTransform</code> – merges nested AND/OR nodes.</li>
 *   <li><code>RemoveDoubleNotTransform</code> – eliminates redundant NOT NOT pairs.</li>
 * </ul>
 * New passes can be injected via the secondary constructor.
 */
public final class LogicalPlanner {

    private final List<LogicalTransform> pipeline;
    private final LogicalPlanValidator validator;

    /**
     * Creates a planner with the built‑in optimisation passes.
     */
    public LogicalPlanner() {
        this.pipeline = List.of(
                new FlattenAndOrTransform(),
                new RemoveDoubleNotTransform(),
                new ContradictionDetectionTransform(),
                new TautologyEliminationTransform(),
                new RedundantConditionEliminationTransform(),
                new ConstantFoldingTransform()
        );
        this.validator = new LogicalPlanValidator();
    }

    /**
     * Creates a planner with a custom pipeline (useful for tests or experimental rules).
     */
    public LogicalPlanner(List<LogicalTransform> customPipeline) {
        this.pipeline = List.copyOf(customPipeline);
        this.validator = new LogicalPlanValidator();
    }

    // ────────────────────────────────────────────────────────────────────────────────
    // Public API
    // ────────────────────────────────────────────────────────────────────────────────

    /**
     * Produces an <i>optimised</i> logical plan tree from the given parser AST.
     */
    public LogicalNode plan(BqlExpr root) {
        Objects.requireNonNull(root, "root expression must not be null");

        // Step 1 – raw conversion
        LogicalNode logical = convert(root);

        // Step 2 – run pipeline (each pass may rewrite the tree)
        for (LogicalTransform pass : pipeline) {
            logical = pass.transform(logical);
        }

        return logical;
    }

    /**
     * Produces an optimised logical plan tree and validates it for correctness.
     * Throws {@link LogicalPlanValidationException} if the plan is invalid.
     */
    public LogicalNode planAndValidate(BqlExpr root) {
        LogicalNode plan = plan(root);

        LogicalPlanValidator.ValidationResult result = validator.validate(plan);
        if (!result.valid()) {
            throw new LogicalPlanValidationException("Invalid logical plan", result);
        }

        return plan;
    }

    /**
     * Validates an existing logical plan.
     */
    public LogicalPlanValidator.ValidationResult validate(LogicalNode plan) {
        return validator.validate(plan);
    }

    /**
     * Checks if a logical plan is well-formed (no structural errors).
     */
    public boolean isWellFormed(LogicalNode plan) {
        return validator.isWellFormed(plan);
    }

    // ────────────────────────────────────────────────────────────────────────────────
    // Raw AST → LogicalNode conversion (unchanged from previous version)
    // ────────────────────────────────────────────────────────────────────────────────

    private LogicalNode convert(BqlExpr expr) {
        if (expr instanceof BqlAnd(List<BqlExpr> children)) {
            return new LogicalAnd(convertList(children));
        }
        if (expr instanceof BqlOr(List<BqlExpr> children)) {
            return new LogicalOr(convertList(children));
        }
        if (expr instanceof BqlNot(BqlExpr _expr)) {
            return new LogicalNot(convert(_expr));
        }
        if (expr instanceof BqlElemMatch(String selector, BqlExpr _expr)) {
            return new LogicalElemMatch(selector, convert(_expr));
        }
        // Selector comparisons → filter
        if (expr instanceof BqlEq(String selector, BqlValue value)) return toFilter(selector, Operator.EQ, value);
        if (expr instanceof BqlNe(String selector, BqlValue value)) return toFilter(selector, Operator.NE, value);
        if (expr instanceof BqlGt(String selector, BqlValue value)) return toFilter(selector, Operator.GT, value);
        if (expr instanceof BqlGte(String selector, BqlValue value)) return toFilter(selector, Operator.GTE, value);
        if (expr instanceof BqlLt(String selector, BqlValue value)) return toFilter(selector, Operator.LT, value);
        if (expr instanceof BqlLte(String selector, BqlValue value)) return toFilter(selector, Operator.LTE, value);
        if (expr instanceof BqlIn(String selector, List<BqlValue> values))
            return toFilter(selector, Operator.IN, values);
        if (expr instanceof BqlNin(String selector, List<BqlValue> values))
            return toFilter(selector, Operator.NIN, values);
        if (expr instanceof BqlAll(String selector, List<BqlValue> values))
            return toFilter(selector, Operator.ALL, values);
        if (expr instanceof BqlSize(String selector, int size)) return toFilter(selector, Operator.SIZE, size);
        if (expr instanceof BqlExists(String selector, boolean exists))
            return toFilter(selector, Operator.EXISTS, exists);

        throw new IllegalArgumentException("Unsupported BqlExpr type: " + expr.getClass());
    }

    private List<LogicalNode> convertList(List<BqlExpr> children) {
        return children.stream().map(this::convert).collect(Collectors.toCollection(ArrayList::new));
    }

    private LogicalFilter toFilter(String selector, Operator op, Object operand) {
        return new LogicalFilter(selector, op, operand);
    }

    // ────────────────────────────────────────────────────────────────────────────────
    // Logical transform (pipeline) API + sample passes
    // ────────────────────────────────────────────────────────────────────────────────

    /**
     * One optimisation / normalisation pass that may rewrite the tree.
     */
    public interface LogicalTransform {
        LogicalNode transform(LogicalNode root);
    }

    /**
     * Shared utility methods for transform implementations.
     */
    static final class TransformUtils {

        /**
         * Extracts the actual value from BqlValue wrapper objects.
         */
        static Object extractValue(Object operand) {
            if (operand instanceof StringVal(String value)) {
                return value;
            } else if (operand instanceof Int32Val(int value)) {
                return value;
            } else if (operand instanceof Int64Val(long value)) {
                return value;
            } else if (operand instanceof Decimal128Val(java.math.BigDecimal value)) {
                return value;
            } else if (operand instanceof DoubleVal(double value)) {
                return value;
            } else if (operand instanceof BooleanVal(boolean value)) {
                return value;
            } else if (operand instanceof NullVal) {
                return null;
            } else if (operand instanceof BinaryVal(byte[] value)) {
                return value;
            } else if (operand instanceof DateTimeVal(long value)) {
                return value;
            } else if (operand instanceof TimestampVal(long value)) {
                return value;
            } else if (operand instanceof VersionstampVal(Versionstamp value)) {
                return value;
            }
            return operand;
        }

        /**
         * Checks if both operands are numeric (including BqlValue wrappers).
         */
        static boolean areNumericOperands(Object operand1, Object operand2) {
            return isNumeric(operand1) && isNumeric(operand2);
        }

        /**
         * Checks if an operand is numeric (including BqlValue wrappers).
         */
        static boolean isNumeric(Object operand) {
            return operand instanceof Number ||
                    operand instanceof Int32Val ||
                    operand instanceof Int64Val ||
                    operand instanceof Decimal128Val ||
                    operand instanceof DoubleVal ||
                    operand instanceof DateTimeVal ||
                    operand instanceof TimestampVal;
            // Note: NullVal and BinaryVal are explicitly not numeric
        }

        /**
         * Extracts numeric value from operand (including BqlValue wrappers).
         */
        static Number extractNumericValue(Object operand) {
            if (operand instanceof Int32Val(int value)) {
                return value;
            } else if (operand instanceof Int64Val(long value)) {
                return value;
            } else if (operand instanceof Decimal128Val(java.math.BigDecimal value)) {
                return value;
            } else if (operand instanceof DoubleVal(double value)) {
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

        /**
         * Groups filters by selector name from a list of LogicalNodes.
         */
        static Map<String, List<LogicalFilter>> groupFiltersBySelector(List<LogicalNode> children) {
            Map<String, List<LogicalFilter>> filtersBySelector = new HashMap<>();

            for (LogicalNode child : children) {
                if (child instanceof LogicalFilter filter) {
                    filtersBySelector.computeIfAbsent(filter.selector(), k -> new ArrayList<>()).add(filter);
                }
            }

            return filtersBySelector;
        }

        /**
         * Extracts values from a list, applying extractValue to each element.
         */
        static List<Object> extractValues(List<Object> list) {
            return list.stream().map(TransformUtils::extractValue).collect(Collectors.toList());
        }

        /**
         * Common child processing for AND nodes with constant folding.
         * Filters out TRUE children and returns FALSE immediately if any child is FALSE.
         */
        static LogicalNode processAndChildren(List<LogicalNode> children, Function<LogicalNode, LogicalNode> rewriter) {
            if (children == null || children.isEmpty()) {
                return LogicalTrue.INSTANCE;
            }

            List<LogicalNode> rewrittenChildren = new ArrayList<>();
            for (LogicalNode child : children) {
                LogicalNode rewritten = rewriter.apply(child);
                if (rewritten instanceof LogicalFalse) {
                    return LogicalFalse.INSTANCE; // AND with FALSE is FALSE
                }
                // Skip TRUE children (they don't affect AND result)
                if (!(rewritten instanceof LogicalTrue)) {
                    rewrittenChildren.add(rewritten);
                }
            }

            return simplifyAndNode(rewrittenChildren);
        }

        /**
         * Common child processing for OR nodes with constant folding.
         * Filters out FALSE children and returns TRUE immediately if any child is TRUE.
         */
        static LogicalNode processOrChildren(List<LogicalNode> children, Function<LogicalNode, LogicalNode> rewriter) {
            if (children == null || children.isEmpty()) {
                return LogicalFalse.INSTANCE;
            }

            List<LogicalNode> rewrittenChildren = new ArrayList<>();
            for (LogicalNode child : children) {
                LogicalNode rewritten = rewriter.apply(child);
                if (rewritten instanceof LogicalTrue) {
                    return LogicalTrue.INSTANCE; // OR with TRUE is TRUE
                }
                // Skip FALSE children (they don't affect OR result)
                if (!(rewritten instanceof LogicalFalse)) {
                    rewrittenChildren.add(rewritten);
                }
            }

            return simplifyOrNode(rewrittenChildren);
        }

        /**
         * Simplifies AND node based on number of children.
         */
        static LogicalNode simplifyAndNode(List<LogicalNode> children) {
            if (children.isEmpty()) {
                return LogicalTrue.INSTANCE;
            } else if (children.size() == 1) {
                return children.get(0);
            } else {
                return new LogicalAnd(children);
            }
        }

        /**
         * Simplifies OR node based on number of children.
         */
        static LogicalNode simplifyOrNode(List<LogicalNode> children) {
            if (children.isEmpty()) {
                return LogicalFalse.INSTANCE;
            } else if (children.size() == 1) {
                return children.get(0);
            } else {
                return new LogicalOr(children);
            }
        }

        /**
         * Common pattern for checking selector-specific properties in filters.
         * Used for contradictions, tautologies, etc.
         */
        static boolean hasSelectorProperty(List<LogicalFilter> filters,
                                           BiPredicate<LogicalFilter, LogicalFilter> propertyChecker) {
            for (int i = 0; i < filters.size(); i++) {
                for (int j = i + 1; j < filters.size(); j++) {
                    if (propertyChecker.test(filters.get(i), filters.get(j))) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    /**
     * Flattens nested AND/OR nodes: AND(a, AND(b,c))  → AND(a,b,c).
     */
    private static final class FlattenAndOrTransform implements LogicalTransform {
        @Override
        public LogicalNode transform(LogicalNode root) {
            return rewrite(root);
        }

        private LogicalNode rewrite(LogicalNode n) {
            if (n instanceof LogicalAnd(List<LogicalNode> children)) {
                List<LogicalNode> flat = new ArrayList<>();
                for (LogicalNode child : children) {
                    LogicalNode r = rewrite(child);
                    if (r instanceof LogicalAnd(List<LogicalNode> _children)) flat.addAll(_children);
                    else flat.add(r);
                }
                return new LogicalAnd(flat);
            }
            if (n instanceof LogicalOr(List<LogicalNode> children)) {
                List<LogicalNode> flat = new ArrayList<>();
                for (LogicalNode child : children) {
                    LogicalNode r = rewrite(child);
                    if (r instanceof LogicalOr(List<LogicalNode> _children)) flat.addAll(_children);
                    else flat.add(r);
                }
                return new LogicalOr(flat);
            }
            if (n instanceof LogicalNot(LogicalNode child)) {
                return new LogicalNot(rewrite(child));
            }
            if (n instanceof LogicalElemMatch(String selector, LogicalNode subPlan)) {
                return new LogicalElemMatch(selector, rewrite(subPlan));
            }
            return n; // Filter or untouched node
        }
    }

    /**
     * Eliminates NOT(NOT(x)) patterns.
     */
    private static final class RemoveDoubleNotTransform implements LogicalTransform {
        @Override
        public LogicalNode transform(LogicalNode root) {
            return rewrite(root);
        }

        private LogicalNode rewrite(LogicalNode n) {
            if (n instanceof LogicalNot(LogicalNode child)) {
                LogicalNode inner = rewrite(child);
                if (inner instanceof LogicalNot(LogicalNode child1)) {
                    // NOT(NOT(x)) → x             (double negation elimination)
                    return rewrite(child1);
                }
                return new LogicalNot(inner);
            }
            // Use shared recursion pattern for other node types
            // Use shared recursion for other node types
            if (n instanceof LogicalAnd(List<LogicalNode> children)) {
                return new LogicalAnd(children.stream().map(this::rewrite).toList());
            }
            if (n instanceof LogicalOr(List<LogicalNode> children)) {
                return new LogicalOr(children.stream().map(this::rewrite).toList());
            }
            if (n instanceof LogicalElemMatch(String selector, LogicalNode subPlan)) {
                return new LogicalElemMatch(selector, rewrite(subPlan));
            }
            return n; // Filter or untouched node
        }
    }

    /**
     * Detects contradictory conditions and replaces them with FALSE.
     * Examples:
     * - AND(selector = A, selector = B) → FALSE
     * - AND(selector > 100, selector < 50) → FALSE
     * - AND(selector = A, selector != A) → FALSE
     */
    private static final class ContradictionDetectionTransform implements LogicalTransform {
        @Override
        public LogicalNode transform(LogicalNode root) {
            return rewrite(root);
        }

        private LogicalNode rewrite(LogicalNode n) {
            switch (n) {
                case LogicalAnd(List<LogicalNode> children) -> {
                    if (children == null || children.isEmpty()) {
                        return n;
                    }

                    // First, recursively rewrite children
                    List<LogicalNode> rewrittenChildren = new ArrayList<>();
                    for (LogicalNode child : children) {
                        LogicalNode rewritten = rewrite(child);
                        // If any child is FALSE, the entire AND is FALSE
                        if (rewritten instanceof LogicalFalse) {
                            return LogicalFalse.INSTANCE;
                        }
                        // Skip TRUE children (they don't affect AND result)
                        if (!(rewritten instanceof LogicalTrue)) {
                            rewrittenChildren.add(rewritten);
                        }
                    }

                    // Check for contradictions among filters
                    if (hasContradictions(rewrittenChildren)) {
                        return LogicalFalse.INSTANCE;
                    }

                    // Return simplified AND
                    if (rewrittenChildren.isEmpty()) {
                        return LogicalTrue.INSTANCE; // Empty AND is TRUE
                    } else if (rewrittenChildren.size() == 1) {
                        return rewrittenChildren.get(0); // Single child AND
                    } else {
                        return new LogicalAnd(rewrittenChildren);
                    }
                }
                case LogicalOr(List<LogicalNode> children) -> {
                    if (children == null || children.isEmpty()) {
                        return n;
                    }

                    // Recursively rewrite children
                    List<LogicalNode> rewrittenChildren = new ArrayList<>();
                    for (LogicalNode child : children) {
                        LogicalNode rewritten = rewrite(child);
                        // If any child is TRUE, the entire OR is TRUE
                        if (rewritten instanceof LogicalTrue) {
                            return LogicalTrue.INSTANCE;
                        }
                        // Skip FALSE children (they don't affect OR result)
                        if (!(rewritten instanceof LogicalFalse)) {
                            rewrittenChildren.add(rewritten);
                        }
                    }

                    // Return simplified OR
                    if (rewrittenChildren.isEmpty()) {
                        return LogicalFalse.INSTANCE; // Empty OR is FALSE
                    } else if (rewrittenChildren.size() == 1) {
                        return rewrittenChildren.get(0); // Single child OR
                    } else {
                        return new LogicalOr(rewrittenChildren);
                    }
                }
                case LogicalNot(LogicalNode child) -> {
                    LogicalNode rewrittenChild = rewrite(child);
                    if (rewrittenChild instanceof LogicalTrue) {
                        return LogicalFalse.INSTANCE; // NOT(TRUE) = FALSE
                    } else if (rewrittenChild instanceof LogicalFalse) {
                        return LogicalTrue.INSTANCE; // NOT(FALSE) = TRUE
                    } else {
                        return new LogicalNot(rewrittenChild);
                    }
                }
                case LogicalElemMatch(String selector, LogicalNode subPlan) -> {
                    LogicalNode rewrittenSubPlan = rewrite(subPlan);
                    if (rewrittenSubPlan instanceof LogicalFalse) {
                        return LogicalFalse.INSTANCE; // ElemMatch with FALSE condition is FALSE
                    } else if (rewrittenSubPlan instanceof LogicalTrue) {
                        return LogicalTrue.INSTANCE; // ElemMatch with TRUE condition is TRUE
                    } else {
                        return new LogicalElemMatch(selector, rewrittenSubPlan);
                    }
                }
                default -> {
                    return n; // Filter, TRUE, FALSE - no rewriting needed
                }
            }
        }

        private boolean hasContradictions(List<LogicalNode> children) {
            Map<String, List<LogicalFilter>> filtersBySelector = TransformUtils.groupFiltersBySelector(children);

            // Check each selector for contradictions
            for (List<LogicalFilter> filters : filtersBySelector.values()) {
                if (filters.size() > 1 && TransformUtils.hasSelectorProperty(filters, this::areContradictory)) {
                    return true;
                }
            }

            return false;
        }

        private boolean areContradictory(LogicalFilter filter1, LogicalFilter filter2) {
            if (!filter1.selector().equals(filter2.selector())) {
                return false;
            }

            Operator op1 = filter1.op();
            Operator op2 = filter2.op();
            Object operand1 = filter1.operand();
            Object operand2 = filter2.operand();

            // EQ with different values
            if (op1 == Operator.EQ && op2 == Operator.EQ) {
                return !Objects.equals(TransformUtils.extractValue(operand1), TransformUtils.extractValue(operand2));
            }

            // EQ vs NE with same value
            if ((op1 == Operator.EQ && op2 == Operator.NE) || (op1 == Operator.NE && op2 == Operator.EQ)) {
                return Objects.equals(TransformUtils.extractValue(operand1), TransformUtils.extractValue(operand2));
            }

            // Numeric contradictions
            if (TransformUtils.areNumericOperands(operand1, operand2)) {
                return checkNumericContradictions(op1, TransformUtils.extractNumericValue(operand1),
                        op2, TransformUtils.extractNumericValue(operand2));
            }

            return false;
        }

        private boolean checkNumericContradictions(Operator op1, Number val1, Operator op2, Number val2) {
            double d1 = val1.doubleValue();
            double d2 = val2.doubleValue();

            // GT/GTE vs LT/LTE contradictions
            if ((op1 == Operator.GT && op2 == Operator.LTE && d1 >= d2) ||
                    (op1 == Operator.GTE && op2 == Operator.LT && d1 >= d2) ||
                    (op1 == Operator.GT && op2 == Operator.LT && d1 >= d2) ||
                    (op1 == Operator.GTE && op2 == Operator.LTE && d1 > d2)) {
                return true;
            }

            // Reverse check
            return (op2 == Operator.GT && op1 == Operator.LTE && d2 >= d1) ||
                    (op2 == Operator.GTE && op1 == Operator.LT && d2 >= d1) ||
                    (op2 == Operator.GT && op1 == Operator.LT && d2 >= d1) ||
                    (op2 == Operator.GTE && op1 == Operator.LTE && d2 > d1);
        }
    }

    /**
     * Eliminates tautological conditions and simplifies logical expressions.
     * Examples:
     * - OR(selector = A, selector != A) → TRUE
     * - OR(selector > 5, selector <= 5) → TRUE
     * - AND(TRUE, condition) → condition
     * - OR(FALSE, condition) → condition
     */
    private static final class TautologyEliminationTransform implements LogicalTransform {
        @Override
        public LogicalNode transform(LogicalNode root) {
            return rewrite(root);
        }

        private LogicalNode rewrite(LogicalNode n) {
            switch (n) {
                case LogicalAnd(List<LogicalNode> children) -> {
                    return TransformUtils.processAndChildren(children, this::rewrite);
                }
                case LogicalOr(List<LogicalNode> children) -> {
                    // Check for tautologies (e.g., selector = A OR selector != A)
                    if (hasTautology(children)) {
                        return LogicalTrue.INSTANCE;
                    }
                    return TransformUtils.processOrChildren(children, this::rewrite);
                }
                case LogicalNot(LogicalNode child) -> {
                    LogicalNode rewrittenChild = rewrite(child);
                    if (rewrittenChild instanceof LogicalTrue) {
                        return LogicalFalse.INSTANCE;
                    } else if (rewrittenChild instanceof LogicalFalse) {
                        return LogicalTrue.INSTANCE;
                    } else {
                        return new LogicalNot(rewrittenChild);
                    }
                }
                case LogicalElemMatch(String selector, LogicalNode subPlan) -> {
                    LogicalNode rewrittenSubPlan = rewrite(subPlan);
                    return new LogicalElemMatch(selector, rewrittenSubPlan);
                }
                default -> {
                    return n; // Filter, TRUE, FALSE - no rewriting needed
                }
            }
        }

        private boolean hasTautology(List<LogicalNode> children) {
            Map<String, List<LogicalFilter>> filtersBySelector = TransformUtils.groupFiltersBySelector(children);

            // Check each selector for tautologies
            for (List<LogicalFilter> filters : filtersBySelector.values()) {
                if (filters.size() > 1 && TransformUtils.hasSelectorProperty(filters, this::areTautological)) {
                    return true;
                }
            }

            return false;
        }

        private boolean areTautological(LogicalFilter filter1, LogicalFilter filter2) {
            if (!filter1.selector().equals(filter2.selector())) {
                return false;
            }

            Operator op1 = filter1.op();
            Operator op2 = filter2.op();
            Object operand1 = filter1.operand();
            Object operand2 = filter2.operand();

            // EQ vs NE with same value in OR context = tautology (selector = A OR selector != A)
            if ((op1 == Operator.EQ && op2 == Operator.NE) || (op1 == Operator.NE && op2 == Operator.EQ)) {
                return Objects.equals(TransformUtils.extractValue(operand1), TransformUtils.extractValue(operand2));
            }

            // Numeric range tautologies (e.g., selector > 5 OR selector <= 5)
            if (TransformUtils.areNumericOperands(operand1, operand2)) {
                return checkNumericTautologies(op1, TransformUtils.extractNumericValue(operand1),
                        op2, TransformUtils.extractNumericValue(operand2));
            }

            return false;
        }

        private boolean checkNumericTautologies(Operator op1, Number val1, Operator op2, Number val2) {
            double d1 = val1.doubleValue();
            double d2 = val2.doubleValue();

            // Complementary range conditions
            if ((op1 == Operator.GT && op2 == Operator.LTE && d1 == d2) ||
                    (op1 == Operator.GTE && op2 == Operator.LT && d1 == d2) ||
                    (op1 == Operator.LT && op2 == Operator.GTE && d1 == d2) ||
                    (op1 == Operator.LTE && op2 == Operator.GT && d1 == d2)) {
                return true;
            }

            // Reverse check
            return (op2 == Operator.GT && op1 == Operator.LTE && d2 == d1) ||
                    (op2 == Operator.GTE && op1 == Operator.LT && d2 == d1) ||
                    (op2 == Operator.LT && op1 == Operator.GTE && d2 == d1) ||
                    (op2 == Operator.LTE && op1 == Operator.GT && d2 == d1);
        }

        // Using shared utility methods
    }

    /**
     * Eliminates redundant conditions in logical expressions.
     * Examples:
     * - AND(selector > 5, selector > 3) → selector > 5 (more restrictive)
     * - OR(selector < 10, selector < 15) → selector < 15 (less restrictive)
     * - AND(selector = A, selector = A) → selector = A (duplicates)
     * - AND(selector >= 5, selector > 4) → selector >= 5 (subsumption)
     * - OR(selector IN [A,B], selector = A) → selector IN [A,B] (subsumption)
     */
    private static final class RedundantConditionEliminationTransform implements LogicalTransform {
        @Override
        public LogicalNode transform(LogicalNode root) {
            return rewrite(root);
        }

        private LogicalNode rewrite(LogicalNode n) {
            switch (n) {
                case LogicalAnd(List<LogicalNode> children) -> {
                    if (children == null || children.isEmpty()) {
                        return n;
                    }

                    // Recursively rewrite children first
                    List<LogicalNode> rewrittenChildren = new ArrayList<>();
                    for (LogicalNode child : children) {
                        LogicalNode rewritten = rewrite(child);
                        rewrittenChildren.add(rewritten);
                    }

                    // Eliminate redundant conditions in AND context
                    List<LogicalNode> nonRedundant = eliminateRedundantInAnd(rewrittenChildren);

                    if (nonRedundant.isEmpty()) {
                        return LogicalTrue.INSTANCE;
                    } else if (nonRedundant.size() == 1) {
                        return nonRedundant.get(0);
                    } else {
                        return new LogicalAnd(nonRedundant);
                    }
                }
                case LogicalOr(List<LogicalNode> children) -> {
                    if (children == null || children.isEmpty()) {
                        return n;
                    }

                    // Recursively rewrite children first
                    List<LogicalNode> rewrittenChildren = new ArrayList<>();
                    for (LogicalNode child : children) {
                        LogicalNode rewritten = rewrite(child);
                        rewrittenChildren.add(rewritten);
                    }

                    // Eliminate redundant conditions in OR context
                    List<LogicalNode> nonRedundant = eliminateRedundantInOr(rewrittenChildren);

                    if (nonRedundant.isEmpty()) {
                        return LogicalFalse.INSTANCE;
                    } else if (nonRedundant.size() == 1) {
                        return nonRedundant.get(0);
                    } else {
                        return new LogicalOr(nonRedundant);
                    }
                }
                case LogicalNot(LogicalNode child) -> {
                    return new LogicalNot(rewrite(child));
                }
                case LogicalElemMatch(String selector, LogicalNode subPlan) -> {
                    return new LogicalElemMatch(selector, rewrite(subPlan));
                }
                default -> {
                    return n; // Filter, TRUE, FALSE - no rewriting needed
                }
            }
        }

        private List<LogicalNode> eliminateRedundantInAnd(List<LogicalNode> children) {
            List<LogicalNode> result = new ArrayList<>();
            Map<String, List<LogicalFilter>> filtersBySelector = TransformUtils.groupFiltersBySelector(children);

            // Add non-filter nodes as-is
            for (LogicalNode child : children) {
                if (!(child instanceof LogicalFilter)) {
                    result.add(child);
                }
            }

            // Process each selector's filters for redundancy elimination
            for (Map.Entry<String, List<LogicalFilter>> entry : filtersBySelector.entrySet()) {
                String selector = entry.getKey();
                List<LogicalFilter> filters = entry.getValue();

                if (filters.size() == 1) {
                    result.add(filters.get(0));
                } else {
                    List<LogicalFilter> nonRedundant = eliminateRedundantFiltersInAnd(filters);
                    result.addAll(nonRedundant);
                }
            }

            return result;
        }

        private List<LogicalNode> eliminateRedundantInOr(List<LogicalNode> children) {
            List<LogicalNode> result = new ArrayList<>();
            Map<String, List<LogicalFilter>> filtersBySelector = TransformUtils.groupFiltersBySelector(children);

            // Add non-filter nodes as-is
            for (LogicalNode child : children) {
                if (!(child instanceof LogicalFilter)) {
                    result.add(child);
                }
            }

            // Process each selector's filters for redundancy elimination
            for (Map.Entry<String, List<LogicalFilter>> entry : filtersBySelector.entrySet()) {
                String selector = entry.getKey();
                List<LogicalFilter> filters = entry.getValue();

                if (filters.size() == 1) {
                    result.add(filters.get(0));
                } else {
                    List<LogicalFilter> nonRedundant = eliminateRedundantFiltersInOr(filters);
                    result.addAll(nonRedundant);
                }
            }

            return result;
        }


        private List<LogicalFilter> eliminateRedundantFiltersInAnd(List<LogicalFilter> filters) {
            List<LogicalFilter> result = new ArrayList<>();

            for (LogicalFilter current : filters) {
                boolean isRedundant = false;

                // Check if current filter is subsumed by any existing filter in result
                for (int i = 0; i < result.size(); i++) {
                    LogicalFilter existing = result.get(i);

                    if (isSubsumedInAnd(current, existing)) {
                        // Current is redundant (existing is more restrictive or equal)
                        isRedundant = true;
                        break;
                    } else if (isSubsumedInAnd(existing, current)) {
                        // Existing is redundant (current is more restrictive)
                        result.set(i, current);
                        isRedundant = true;
                        break;
                    }
                }

                if (!isRedundant) {
                    result.add(current);
                }
            }

            return result;
        }

        private List<LogicalFilter> eliminateRedundantFiltersInOr(List<LogicalFilter> filters) {
            List<LogicalFilter> result = new ArrayList<>();

            for (LogicalFilter current : filters) {
                boolean isRedundant = false;

                // Check if current filter is subsumed by any existing filter in result
                for (int i = 0; i < result.size(); i++) {
                    LogicalFilter existing = result.get(i);

                    if (isSubsumedInOr(current, existing)) {
                        // Current is redundant (existing is less restrictive or equal)
                        isRedundant = true;
                        break;
                    } else if (isSubsumedInOr(existing, current)) {
                        // Existing is redundant (current is less restrictive)
                        result.set(i, current);
                        isRedundant = true;
                        break;
                    }
                }

                if (!isRedundant) {
                    result.add(current);
                }
            }

            return result;
        }

        // Check if filter1 is subsumed by filter2 in AND context
        // (filter2 is more restrictive or equal, so filter1 is redundant)
        private boolean isSubsumedInAnd(LogicalFilter filter1, LogicalFilter filter2) {
            if (!filter1.selector().equals(filter2.selector())) {
                return false;
            }

            Operator op1 = filter1.op();
            Operator op2 = filter2.op();
            Object operand1 = filter1.operand();
            Object operand2 = filter2.operand();

            // Exact duplicates
            if (op1 == op2 && Objects.equals(TransformUtils.extractValue(operand1), TransformUtils.extractValue(operand2))) {
                return true;
            }

            // Numeric range subsumption in AND
            if (TransformUtils.areNumericOperands(operand1, operand2)) {
                return checkNumericSubsumption(op1, TransformUtils.extractNumericValue(operand1),
                        op2, TransformUtils.extractNumericValue(operand2), true);
            }

            // Set operation subsumption
            if (areSetOperators(op1, op2)) {
                return checkSetSubsumption(op1, operand1, op2, operand2);
            }

            // Semantic redundancy: EQ makes NE with different value redundant in AND
            if ((op1 == Operator.NE && op2 == Operator.EQ) &&
                    !Objects.equals(TransformUtils.extractValue(operand1), TransformUtils.extractValue(operand2))) {
                return true; // NE is redundant when EQ is more specific with different value
            }
            if ((op1 == Operator.EQ && op2 == Operator.NE) &&
                    !Objects.equals(TransformUtils.extractValue(operand1), TransformUtils.extractValue(operand2))) {
                return false; // EQ is not redundant (it's more specific)
            }

            return false;
        }

        // Check if filter1 is subsumed by filter2 in OR context
        // (filter2 is less restrictive or equal, so filter1 is redundant)
        private boolean isSubsumedInOr(LogicalFilter filter1, LogicalFilter filter2) {
            if (!filter1.selector().equals(filter2.selector())) {
                return false;
            }

            Operator op1 = filter1.op();
            Operator op2 = filter2.op();
            Object operand1 = filter1.operand();
            Object operand2 = filter2.operand();

            // Exact duplicates
            if (op1 == op2 && Objects.equals(TransformUtils.extractValue(operand1), TransformUtils.extractValue(operand2))) {
                return true;
            }

            // Numeric range subsumption in OR
            if (TransformUtils.areNumericOperands(operand1, operand2)) {
                return checkNumericSubsumption(op1, TransformUtils.extractNumericValue(operand1),
                        op2, TransformUtils.extractNumericValue(operand2), false);
            }

            // Set operation subsumption
            if (areSetOperators(op1, op2)) {
                return checkSetSubsumption(op1, operand1, op2, operand2);
            }

            return false;
        }

        private boolean checkNumericSubsumption(Operator op1, Number val1, Operator op2, Number val2, boolean isAndContext) {
            double d1 = val1.doubleValue();
            double d2 = val2.doubleValue();

            if (isAndContext) {
                // EQ/NE redundancy in AND context
                if (op1 == Operator.NE && op2 == Operator.EQ && d1 != d2) {
                    return true; // NE is redundant when EQ is more specific with different value
                }
                if (op1 == Operator.EQ && op2 == Operator.NE && d1 != d2) {
                    return false; // EQ is not redundant (it's more specific)
                }

                // AND context: keep more restrictive condition
                // GT/GTE subsumption: selector > 50 is subsumed by selector > 100 (100 is more restrictive)
                if (op1 == Operator.GT && op2 == Operator.GT && d1 <= d2) return true;
                if (op1 == Operator.GT && op2 == Operator.GTE && d1 < d2) return true;
                if (op1 == Operator.GTE && op2 == Operator.GTE && d1 <= d2) return true;
                if (op1 == Operator.GTE && op2 == Operator.GT && d1 <= d2) return true;

                // LT/LTE subsumption: selector < 50 is subsumed by selector < 30 (30 is more restrictive)
                if (op1 == Operator.LT && op2 == Operator.LT && d1 >= d2) return true;
                if (op1 == Operator.LT && op2 == Operator.LTE && d1 > d2) return true;
                if (op1 == Operator.LTE && op2 == Operator.LTE && d1 >= d2) return true;
                return op1 == Operator.LTE && op2 == Operator.LT && d1 >= d2;
            } else {
                // OR context: keep less restrictive condition
                // GT/GTE subsumption: selector > 100 is subsumed by selector > 50 in OR (50 covers more)
                if (op1 == Operator.GT && op2 == Operator.GT && d1 >= d2) return true;
                if (op1 == Operator.GT && op2 == Operator.GTE && d1 > d2) return true;
                if (op1 == Operator.GTE && op2 == Operator.GTE && d1 >= d2) return true;
                if (op1 == Operator.GTE && op2 == Operator.GT && d1 >= d2) return true;

                // LT/LTE subsumption: selector < 30 is subsumed by selector < 50 in OR (50 covers more)
                if (op1 == Operator.LT && op2 == Operator.LT && d1 <= d2) return true;
                if (op1 == Operator.LT && op2 == Operator.LTE && d1 < d2) return true;
                if (op1 == Operator.LTE && op2 == Operator.LTE && d1 <= d2) return true;
                return op1 == Operator.LTE && op2 == Operator.LT && d1 <= d2;
            }
        }

        private boolean areSetOperators(Operator op1, Operator op2) {
            return (op1 == Operator.IN || op1 == Operator.NIN || op1 == Operator.ALL) &&
                    (op2 == Operator.IN || op2 == Operator.NIN || op2 == Operator.ALL);
        }

        private boolean checkSetSubsumption(Operator op1, Object operand1, Operator op2, Object operand2) {
            if (!(operand1 instanceof List) || !(operand2 instanceof List)) {
                return false;
            }

            @SuppressWarnings("unchecked")
            List<Object> list1 = (List<Object>) operand1;
            @SuppressWarnings("unchecked")
            List<Object> list2 = (List<Object>) operand2;

            Set<Object> set1 = new HashSet<>(TransformUtils.extractValues(list1));
            Set<Object> set2 = new HashSet<>(TransformUtils.extractValues(list2));

            // IN subsumption: selector IN [A] is subsumed by selector IN [A,B] (keep superset)
            // This logic is the same for both AND and OR contexts
            if (op1 == Operator.IN && op2 == Operator.IN) {
                return set2.containsAll(set1); // set1 is subset of set2, so set1 is redundant
            }

            return false;
        }

        // Using shared utility methods
    }

    /**
     * Performs constant folding and simplification of boolean expressions.
     * Examples:
     * - AND(TRUE, TRUE) → TRUE
     * - AND(TRUE, FALSE) → FALSE
     * - OR(FALSE, FALSE) → FALSE
     * - OR(TRUE, FALSE) → TRUE
     * - NOT(NOT(x)) → x (handled by RemoveDoubleNotTransform)
     */
    private static final class ConstantFoldingTransform implements LogicalTransform {
        @Override
        public LogicalNode transform(LogicalNode root) {
            return rewrite(root);
        }

        private LogicalNode rewrite(LogicalNode n) {
            switch (n) {
                case LogicalAnd(List<LogicalNode> children) -> {
                    return TransformUtils.processAndChildren(children, this::rewrite);
                }
                case LogicalOr(List<LogicalNode> children) -> {
                    return TransformUtils.processOrChildren(children, this::rewrite);
                }
                case LogicalNot(LogicalNode child) -> {
                    LogicalNode rewrittenChild = rewrite(child);
                    if (rewrittenChild instanceof LogicalTrue) {
                        return LogicalFalse.INSTANCE; // NOT(TRUE) = FALSE
                    } else if (rewrittenChild instanceof LogicalFalse) {
                        return LogicalTrue.INSTANCE; // NOT(FALSE) = TRUE
                    } else {
                        return new LogicalNot(rewrittenChild);
                    }
                }
                case LogicalElemMatch(String selector, LogicalNode subPlan) -> {
                    LogicalNode rewrittenSubPlan = rewrite(subPlan);
                    if (rewrittenSubPlan instanceof LogicalFalse) {
                        return LogicalFalse.INSTANCE; // ElemMatch with FALSE is FALSE
                    } else if (rewrittenSubPlan instanceof LogicalTrue) {
                        return LogicalTrue.INSTANCE; // ElemMatch with TRUE is TRUE
                    } else {
                        return new LogicalElemMatch(selector, rewrittenSubPlan);
                    }
                }
                default -> {
                    return n; // Filter, TRUE, FALSE - already constants
                }
            }
        }
    }
}

