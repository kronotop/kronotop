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

import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Optimization rule that consolidates multiple range scans on the same field into a single range scan.
 * <p>
 * Examples:
 * - AND(age >= 18, age < 65) → PhysicalRangeScan("age", 18, 65, true, false, age_index)
 * - AND(score > 50, score <= 100) → PhysicalRangeScan("score", 50, 100, false, true, score_index)
 */
public class RangeScanConsolidationRule implements PhysicalOptimizationRule {

    @Override
    public PhysicalNode apply(PhysicalNode node, BucketMetadata metadata, PlannerContext context) {
        return switch (node) {
            case PhysicalAnd and -> optimizeAnd(and, metadata, context);
            case PhysicalNot not -> new PhysicalNot(context.nextId(), apply(not.child(), metadata, context));
            case PhysicalElemMatch elemMatch -> new PhysicalElemMatch(
                    context.nextId(),
                    elemMatch.selector(),
                    apply(elemMatch.subPlan(), metadata, context)
            );
            case PhysicalOr or -> new PhysicalOr(
                    context.nextId(),
                    or.children().stream()
                            .map(child -> apply(child, metadata, context))
                            .toList()
            );
            default -> node; // No optimization for other nodes
        };
    }

    private PhysicalNode optimizeAnd(PhysicalAnd and, BucketMetadata metadata, PlannerContext context) {
        List<PhysicalNode> optimizedChildren = new ArrayList<>();
        Map<String, List<RangeCondition>> rangeConditions = new HashMap<>();

        // First, recursively optimize children
        for (PhysicalNode child : and.children()) {
            PhysicalNode optimized = apply(child, metadata, context);

            // Check if this is an index scan or full scan with a range condition
            PhysicalFilter extractedFilter = null;
            if (optimized instanceof PhysicalIndexScan(int indexId, PhysicalNode node, var indexIgnored) &&
                    node instanceof PhysicalFilter indexFilter &&
                    isRangeOperator(indexFilter.op())) {
                extractedFilter = indexFilter;
            } else if (optimized instanceof PhysicalFullScan(int scanId, PhysicalNode node) &&
                    node instanceof PhysicalFilter scanFilter &&
                    isRangeOperator(scanFilter.op())) {
                extractedFilter = scanFilter;
            }

            if (extractedFilter != null) {

                // Group range conditions by selector
                rangeConditions.computeIfAbsent(extractedFilter.selector(), k -> new ArrayList<>())
                        .add(new RangeCondition(extractedFilter, getIndexFromMetadata(metadata, extractedFilter.selector())));
            } else {
                optimizedChildren.add(optimized);
            }
        }

        // Consolidate range conditions
        for (Map.Entry<String, List<RangeCondition>> entry : rangeConditions.entrySet()) {
            String selector = entry.getKey();
            List<RangeCondition> conditions = entry.getValue();

            if (conditions.size() > 1) {
                // Try to consolidate into a range scan
                PhysicalNode consolidated = consolidateRangeConditions(selector, conditions, context);
                if (consolidated != null) {
                    optimizedChildren.add(consolidated);
                } else {
                    // If consolidation failed, add all original conditions
                    for (RangeCondition condition : conditions) {
                        if (condition.index != null) {
                            optimizedChildren.add(new PhysicalIndexScan(context.nextId(), condition.filter, condition.index));
                        } else {
                            optimizedChildren.add(new PhysicalFullScan(context.nextId(), condition.filter));
                        }
                    }
                }
            } else {
                // Single condition, just add it back
                RangeCondition condition = conditions.get(0);
                if (condition.index != null) {
                    optimizedChildren.add(new PhysicalIndexScan(context.nextId(), condition.filter, condition.index));
                } else {
                    optimizedChildren.add(new PhysicalFullScan(context.nextId(), condition.filter));
                }
            }
        }

        // Return simplified structure
        if (optimizedChildren.size() == 1) {
            return optimizedChildren.get(0);
        } else if (optimizedChildren.size() != and.children().size()) {
            return new PhysicalAnd(context.nextId(), optimizedChildren);
        } else {
            return and;
        }
    }

    private PhysicalNode consolidateRangeConditions(String selector, List<RangeCondition> conditions, PlannerContext context) {
        Object lowerBound = null;
        Object upperBound = null;
        boolean includeLower = false;
        boolean includeUpper = false;
        IndexDefinition index = conditions.get(0).index; // Assume same index for same selector

        // Note: index can be null for full scan range consolidation

        // Extract bounds from conditions
        for (RangeCondition condition : conditions) {
            PhysicalFilter filter = condition.filter;

            switch (filter.op()) {
                case GT -> {
                    if (lowerBound == null || isGreater(filter.operand(), lowerBound)) {
                        lowerBound = filter.operand();
                        includeLower = false;
                    }
                }
                case GTE -> {
                    if (lowerBound == null || isGreaterOrEqual(filter.operand(), lowerBound)) {
                        lowerBound = filter.operand();
                        includeLower = true;
                    }
                }
                case LT -> {
                    if (upperBound == null || isLess(filter.operand(), upperBound)) {
                        upperBound = filter.operand();
                        includeUpper = false;
                    }
                }
                case LTE -> {
                    if (upperBound == null || isLessOrEqual(filter.operand(), upperBound)) {
                        upperBound = filter.operand();
                        includeUpper = true;
                    }
                }
            }
        }

        // Only create range scan if we have both bounds
        if (lowerBound != null && upperBound != null) {
            return new PhysicalRangeScan(context.nextId(), selector, lowerBound, upperBound, includeLower, includeUpper, index);
        }

        return null;
    }

    private boolean isRangeOperator(Operator op) {
        return op == Operator.GT || op == Operator.GTE || op == Operator.LT || op == Operator.LTE;
    }

    private IndexDefinition getIndexFromMetadata(BucketMetadata metadata, String selector) {
        Index index = metadata.indexes().getIndex(selector, IndexSelectionPolicy.READONLY);
        if (index == null) {
            return null;
        }
        return index.definition();
    }

    // Simple comparison helpers - in reality, these would need proper type handling
    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean isGreater(Object a, Object b) {
        if (a instanceof Comparable && b instanceof Comparable) {
            return ((Comparable) a).compareTo(b) > 0;
        }
        return false;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean isGreaterOrEqual(Object a, Object b) {
        if (a instanceof Comparable && b instanceof Comparable) {
            return ((Comparable) a).compareTo(b) >= 0;
        }
        return false;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean isLess(Object a, Object b) {
        if (a instanceof Comparable && b instanceof Comparable) {
            return ((Comparable) a).compareTo(b) < 0;
        }
        return false;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean isLessOrEqual(Object a, Object b) {
        if (a instanceof Comparable && b instanceof Comparable) {
            return ((Comparable) a).compareTo(b) <= 0;
        }
        return false;
    }

    @Override
    public String getName() {
        return "RangeScanConsolidation";
    }

    @Override
    public int getPriority() {
        return 90; // High priority, but after redundant scan elimination
    }

    @Override
    public boolean canApply(PhysicalNode node) {
        return switch (node) {
            case PhysicalAnd and -> and.children().size() > 1 && hasIndexScansWithRangeOperators(and);
            case PhysicalNot not -> true;
            case PhysicalElemMatch elemMatch -> true;
            case PhysicalOr or -> true;
            default -> false;
        };
    }

    private boolean hasIndexScansWithRangeOperators(PhysicalAnd and) {
        return and.children().stream()
                .anyMatch(child ->
                        (child instanceof PhysicalIndexScan(int indexId, PhysicalNode indexNode, var indexIgnored) &&
                                indexNode instanceof PhysicalFilter indexFilter &&
                                isRangeOperator(indexFilter.op())) ||
                                (child instanceof PhysicalFullScan(int scanId, PhysicalNode scanNode) &&
                                        scanNode instanceof PhysicalFilter scanFilter &&
                                        isRangeOperator(scanFilter.op()))
                );
    }

    /**
     * Helper record to group range conditions by selector
     */
    private record RangeCondition(PhysicalFilter filter, IndexDefinition index) {
    }
}