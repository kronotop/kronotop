/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
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
    public PhysicalNode apply(PlannerContext context, PhysicalNode node) {
        return switch (node) {
            case PhysicalAnd and -> optimizeAnd(context, and);
            case PhysicalNot not -> new PhysicalNot(context.nextId(), apply(context, not.child()));
            case PhysicalElemMatch elemMatch -> new PhysicalElemMatch(
                    context.nextId(),
                    elemMatch.selector(),
                    apply(context, elemMatch.subPlan())
            );
            case PhysicalOr or -> new PhysicalOr(
                    context.nextId(),
                    or.children().stream()
                            .map(child -> apply(context, child))
                            .toList()
            );
            default -> node; // No optimization for other nodes
        };
    }

    private PhysicalNode optimizeAnd(PlannerContext context, PhysicalAnd and) {
        List<PhysicalNode> optimizedChildren = new ArrayList<>();
        Map<String, List<RangeCondition>> rangeConditions = new HashMap<>();

        // First, recursively optimize children
        for (PhysicalNode child : and.children()) {
            PhysicalNode optimized = apply(context, child);

            // Check if this is an index scan or full scan with a range condition
            PhysicalFilter extractedFilter = null;
            SingleFieldIndexDefinition existingIndex = null;
            if (optimized instanceof PhysicalIndexScan(
                    int ignoredIndexId, PhysicalNode node, SingleFieldIndexDefinition index
            ) &&
                    node instanceof PhysicalFilter indexFilter &&
                    isRangeOperator(indexFilter.op())) {
                extractedFilter = indexFilter;
                // Preserve the index from the PhysicalIndexScan (important for $elemMatch scalar arrays
                // where the filter selector is empty but the index is on the parent field)
                existingIndex = index;
            } else if (optimized instanceof PhysicalFullScan(int ignoredScanId, PhysicalNode node) &&
                    node instanceof PhysicalFilter scanFilter &&
                    isRangeOperator(scanFilter.op())) {
                extractedFilter = scanFilter;
            }

            if (extractedFilter != null) {
                // Use the existing index if available, otherwise look it up from metadata
                SingleFieldIndexDefinition indexToUse = existingIndex != null ?
                        existingIndex : getIndexFromMetadata(context.getMetadata(), extractedFilter.selector());

                // Group range conditions by selector (use index selector if available for a scalar array $elemMatch)
                String groupKey = indexToUse != null ? indexToUse.selector() : extractedFilter.selector();
                rangeConditions.computeIfAbsent(groupKey, k -> new ArrayList<>())
                        .add(new RangeCondition(extractedFilter, indexToUse));
            } else {
                optimizedChildren.add(optimized);
            }
        }

        // Consolidate range conditions
        for (Map.Entry<String, List<RangeCondition>> entry : rangeConditions.entrySet()) {
            List<RangeCondition> conditions = entry.getValue();

            if (conditions.size() > 1) {
                // Try to consolidate into a range scan
                PhysicalNode consolidated = consolidateRangeConditions(context, conditions);
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
                RangeCondition condition = conditions.getFirst();
                if (condition.index != null) {
                    optimizedChildren.add(new PhysicalIndexScan(context.nextId(), condition.filter, condition.index));
                } else {
                    optimizedChildren.add(new PhysicalFullScan(context.nextId(), condition.filter));
                }
            }
        }

        // Return simplified structure
        if (optimizedChildren.size() == 1) {
            return optimizedChildren.getFirst();
        } else if (optimizedChildren.size() != and.children().size()) {
            return new PhysicalAnd(context.nextId(), optimizedChildren);
        } else {
            return and;
        }
    }

    private PhysicalNode consolidateRangeConditions(PlannerContext context, List<RangeCondition> conditions) {
        Object lowerBound = null;
        Object upperBound = null;
        boolean includeLower = false;
        boolean includeUpper = false;
        SingleFieldIndexDefinition index = conditions.getFirst().index; // Assume same index for same selector
        // Use the original filter selector for residual predicate evaluation
        // (important for a scalar array $elemMatch where the filter selector is empty)
        String filterSelector = conditions.getFirst().filter.selector();

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
        // Use filterSelector for residual evaluation (empty for scalar arrays), but index is used for actual scanning
        if (lowerBound != null && upperBound != null) {
            return new PhysicalRangeScan(context.nextId(), filterSelector, lowerBound, upperBound, includeLower, includeUpper, index);
        }

        return null;
    }

    private boolean isRangeOperator(Operator op) {
        return op == Operator.GT || op == Operator.GTE || op == Operator.LT || op == Operator.LTE;
    }

    private SingleFieldIndexDefinition getIndexFromMetadata(BucketMetadata metadata, String selector) {
        Index index = metadata.indexes().getIndex(selector, IndexSelectionPolicy.READ);
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
            case PhysicalNot ignoredNot -> true;
            case PhysicalElemMatch ignoredElemMatch -> true;
            case PhysicalOr ignoredOr -> true;
            default -> false;
        };
    }

    private boolean hasIndexScansWithRangeOperators(PhysicalAnd and) {
        return and.children().stream()
                .anyMatch(child ->
                        (child instanceof PhysicalIndexScan(
                                int ignoredIndexId, PhysicalNode indexNode, var ignoredIndex
                        ) &&
                                indexNode instanceof PhysicalFilter indexFilter &&
                                isRangeOperator(indexFilter.op())) ||
                                (child instanceof PhysicalFullScan(int ignoredScanId, PhysicalNode scanNode) &&
                                        scanNode instanceof PhysicalFilter scanFilter &&
                                        isRangeOperator(scanFilter.op()))
                );
    }

    /**
     * Helper record to group range conditions by selector
     */
    private record RangeCondition(PhysicalFilter filter, SingleFieldIndexDefinition index) {
    }
}