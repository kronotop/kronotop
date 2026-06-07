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

import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Optimization rule that combines multiple indexed conditions into an optimized intersection scan.
 * <p>
 * This rule identifies AND operations with multiple indexed EQ conditions on different fields
 * and converts them to a single PhysicalIndexIntersection node that can efficiently find
 * documents matching all conditions by intersecting index results.
 * <p>
 * Examples:
 * - AND(PhysicalIndexScan(name="john"), PhysicalIndexScan(age=25))
 * → PhysicalIndexIntersection([name_index, age_index], [filters])
 */
public class IndexIntersectionRule implements PhysicalOptimizationRule {

    private static final int MIN_INDEXES_FOR_INTERSECTION = 2;

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
        List<PhysicalNode> nonIndexedChildren = new ArrayList<>();
        List<IndexScanCandidate> indexCandidates = new ArrayList<>();

        // First, recursively optimize children and categorize them
        for (PhysicalNode child : and.children()) {
            PhysicalNode optimized = apply(context, child);

            // Check if this is an index scan with an equality condition
            if (optimized instanceof PhysicalIndexScan(int ignoredId, PhysicalNode node, var ignoredIndex) &&
                    node instanceof PhysicalFilter filter &&
                    isEqualityOperator(filter.op())) {

                Index index = context.getMetadata().indexes().getIndex(filter.selector(), IndexSelectionPolicy.READ);
                if (index != null) {
                    indexCandidates.add(new IndexScanCandidate(filter, index.definition()));
                } else {
                    nonIndexedChildren.add(optimized);
                }
            } else {
                nonIndexedChildren.add(optimized);
            }
        }

        // Only create intersection if we have multiple indexed conditions
        if (indexCandidates.size() >= MIN_INDEXES_FOR_INTERSECTION) {
            List<SingleFieldIndexDefinition> indexes = indexCandidates.stream()
                    .map(candidate -> candidate.index)
                    .toList();

            List<PhysicalFilter> filters = indexCandidates.stream()
                    .map(candidate -> candidate.filter)
                    .toList();

            PhysicalIndexIntersection intersection = new PhysicalIndexIntersection(context.nextId(), indexes, filters);
            nonIndexedChildren.add(intersection);
        } else {
            // Not enough indexed conditions, add them back as regular index scans
            for (IndexScanCandidate candidate : indexCandidates) {
                nonIndexedChildren.add(new PhysicalIndexScan(context.nextId(), candidate.filter, candidate.index));
            }
        }

        // Return simplified structure
        if (nonIndexedChildren.size() == 1) {
            return nonIndexedChildren.getFirst();
        } else if (nonIndexedChildren.size() < and.children().size()) {
            return new PhysicalAnd(context.nextId(), nonIndexedChildren);
        } else {
            // Even if no structural changes, children might have been optimized
            return new PhysicalAnd(context.nextId(), nonIndexedChildren);
        }
    }

    private boolean isEqualityOperator(Operator op) {
        // For now, only optimize EQ operators for intersection
        // Could be extended to include IN with small value sets
        return op == Operator.EQ;
    }

    @Override
    public String getName() {
        return "IndexIntersection";
    }

    @Override
    public int getPriority() {
        return 80; // High priority, but after simpler optimizations
    }

    @Override
    public boolean canApply(PhysicalNode node) {
        return switch (node) {
            case PhysicalAnd and -> !and.children().isEmpty(); // Apply to all ANDs to handle nested structures
            case PhysicalNot ignoredNot -> true;
            case PhysicalElemMatch ignoredElemMatch -> true;
            case PhysicalOr ignoredOr -> true;
            default -> false;
        };
    }

    /**
     * Helper record to group index scan candidates
     */
    private record IndexScanCandidate(PhysicalFilter filter, SingleFieldIndexDefinition index) {
    }
}