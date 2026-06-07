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

import com.kronotop.bucket.planner.physical.*;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Optimization rule that eliminates redundant scans in AND/OR operations.
 * <p>
 * Examples:
 * - OR(PhysicalIndexScan(A), PhysicalIndexScan(A)) → PhysicalIndexScan(A)
 * - AND(PhysicalFullScan(A), PhysicalFullScan(A)) → PhysicalFullScan(A)
 */
public class RedundantScanEliminationRule implements PhysicalOptimizationRule {

    @Override
    public PhysicalNode apply(PlannerContext context, PhysicalNode node) {
        return switch (node) {
            case PhysicalAnd and -> optimizeAnd(context, and);
            case PhysicalOr or -> optimizeOr(context, or);
            case PhysicalNot not -> new PhysicalNot(context.nextId(), apply(context, not.child()));
            case PhysicalElemMatch elemMatch -> new PhysicalElemMatch(
                    context.nextId(),
                    elemMatch.selector(),
                    apply(context, elemMatch.subPlan())
            );
            default -> node; // No optimization for leaf nodes
        };
    }

    private PhysicalNode optimizeAnd(PlannerContext context, PhysicalAnd and) {
        List<PhysicalNode> optimizedChildren = new ArrayList<>();

        // First, recursively optimize children
        for (PhysicalNode child : and.children()) {
            optimizedChildren.add(apply(context, child));
        }

        // Remove duplicates while preserving order using LinkedHashSet
        // Since PhysicalNode records now have proper equals/hashCode that ignore IDs
        LinkedHashSet<PhysicalNode> uniqueChildren = new LinkedHashSet<>(optimizedChildren);
        List<PhysicalNode> deduplicatedChildren = new ArrayList<>(uniqueChildren);

        // Return simplified structure
        if (deduplicatedChildren.size() == 1) {
            return deduplicatedChildren.getFirst();
        } else if (deduplicatedChildren.size() < and.children().size()) {
            return new PhysicalAnd(context.nextId(), deduplicatedChildren);
        } else {
            // Even if no deduplication happened, children might have been optimized
            return new PhysicalAnd(context.nextId(), deduplicatedChildren);
        }
    }

    private PhysicalNode optimizeOr(PlannerContext context, PhysicalOr or) {
        List<PhysicalNode> optimizedChildren = new ArrayList<>();

        // First, recursively optimize children
        for (PhysicalNode child : or.children()) {
            optimizedChildren.add(apply(context, child));
        }

        // Remove duplicates while preserving order using LinkedHashSet
        // Since PhysicalNode records now have proper equals/hashCode that ignore IDs
        LinkedHashSet<PhysicalNode> uniqueChildren = new LinkedHashSet<>(optimizedChildren);
        List<PhysicalNode> deduplicatedChildren = new ArrayList<>(uniqueChildren);

        // Return simplified structure
        if (deduplicatedChildren.size() == 1) {
            return deduplicatedChildren.getFirst();
        } else if (deduplicatedChildren.size() < or.children().size()) {
            return new PhysicalOr(context.nextId(), deduplicatedChildren);
        } else {
            // Even if no deduplication happened, children might have been optimized
            return new PhysicalOr(context.nextId(), deduplicatedChildren);
        }
    }

    @Override
    public String getName() {
        return "RedundantScanElimination";
    }

    @Override
    public int getPriority() {
        return 100; // High priority - run early to simplify other optimizations
    }

    @Override
    public boolean canApply(PhysicalNode node) {
        return switch (node) {
            case PhysicalAnd and -> !and.children().isEmpty(); // Apply to single child too
            case PhysicalOr or -> !or.children().isEmpty(); // Apply to single child too
            case PhysicalNot not -> true;
            case PhysicalElemMatch elemMatch -> true;
            default -> false;
        };
    }
}