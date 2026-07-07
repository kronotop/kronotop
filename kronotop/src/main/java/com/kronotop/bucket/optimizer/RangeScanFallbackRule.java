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

import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites a PhysicalRangeScan that has no index into a full scan with filters.
 * <p>
 * When no index covers the range, the scan cannot run as-is. This rule turns each
 * bound into a filter over a full bucket scan. A single bound becomes one
 * PhysicalFullScan; two bounds become a PhysicalAnd of two full scans.
 * <p>
 * Examples:
 * - PhysicalRangeScan("age", 18, null, true, false, null)
 * -> PhysicalFullScan(age >= 18)
 * - PhysicalRangeScan("age", 18, 65, true, false, null)
 * -> PhysicalAnd([PhysicalFullScan(age >= 18), PhysicalFullScan(age < 65)])
 */
public class RangeScanFallbackRule implements PhysicalOptimizationRule {

    @Override
    public PhysicalNode apply(PlannerContext context, PhysicalNode node) {
        return switch (node) {
            case PhysicalRangeScan rangeScan when rangeScan.index() == null -> convertToFullScan(rangeScan, context);
            case PhysicalAnd and -> new PhysicalAnd(
                    context.nextId(),
                    and.children().stream()
                            .map(child -> apply(context, child))
                            .toList()
            );
            case PhysicalOr or -> new PhysicalOr(
                    context.nextId(),
                    or.children().stream()
                            .map(child -> apply(context, child))
                            .toList()
            );
            case PhysicalNot not -> new PhysicalNot(
                    context.nextId(),
                    apply(context, not.child())
            );
            case PhysicalElemMatch elemMatch -> new PhysicalElemMatch(
                    context.nextId(),
                    elemMatch.selector(),
                    apply(context, elemMatch.subPlan())
            );
            default -> node; // No optimization for other nodes
        };
    }

    private PhysicalNode convertToFullScan(PhysicalRangeScan rangeScan, PlannerContext context) {
        // Create filters for the range conditions
        List<PhysicalNode> filters = new ArrayList<>();

        if (rangeScan.lowerBound() != null) {
            Operator lowerOp = rangeScan.includeLower() ? Operator.GTE : Operator.GT;
            filters.add(new PhysicalFilter(
                    context.nextId(),
                    rangeScan.selector(),
                    lowerOp,
                    rangeScan.lowerBound()
            ));
        }

        if (rangeScan.upperBound() != null) {
            Operator upperOp = rangeScan.includeUpper() ? Operator.LTE : Operator.LT;
            filters.add(new PhysicalFilter(
                    context.nextId(),
                    rangeScan.selector(),
                    upperOp,
                    rangeScan.upperBound()
            ));
        }

        if (filters.isEmpty()) {
            throw new IllegalStateException("Range scan must have at least one bound");
        }

        if (filters.size() == 1) {
            return new PhysicalFullScan(context.nextId(), filters.getFirst());
        }

        filters.replaceAll(physicalNode -> new PhysicalFullScan(context.nextId(), physicalNode));
        return new PhysicalAnd(context.nextId(), filters);
    }

    @Override
    public String getName() {
        return "RangeScanFallback";
    }

    @Override
    public int getPriority() {
        return 80; // Medium priority - should run after index-based optimizations
    }

    @Override
    public boolean canApply(PhysicalNode node) {
        return switch (node) {
            case PhysicalRangeScan rangeScan -> rangeScan.index() == null;
            case PhysicalAnd and -> hasRangeScanWithNullIndex(and);
            case PhysicalOr or -> hasRangeScanWithNullIndex(or);
            case PhysicalNot not -> true; // Recursively check
            case PhysicalElemMatch elemMatch -> true; // Recursively check
            default -> false;
        };
    }

    private boolean hasRangeScanWithNullIndex(PhysicalAnd and) {
        return and.children().stream()
                .anyMatch(child -> child instanceof PhysicalRangeScan rangeScan && rangeScan.index() == null);
    }

    private boolean hasRangeScanWithNullIndex(PhysicalOr or) {
        return or.children().stream()
                .anyMatch(child -> child instanceof PhysicalRangeScan rangeScan && rangeScan.index() == null);
    }
}