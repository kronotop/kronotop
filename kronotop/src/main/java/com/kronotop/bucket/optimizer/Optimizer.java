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
import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.bucket.planner.physical.PlannerContext;

import java.util.ArrayList;
import java.util.List;

/*
 * As you start to walk on the way, the way appears.
 *
 * — Attributed to Rumi
 */

/**
 * Physical plan optimizer that applies rule-based optimizations to PhysicalNode trees.
 * <p>
 * The optimizer runs after the PhysicalPlanner and before the PlanExecutor,
 * applying a series of optimization rules to improve query execution performance.
 */
public class Optimizer {
    private final List<PhysicalOptimizationRule> rules;
    private final int maxOptimizationPasses;

    public Optimizer() {
        this.maxOptimizationPasses = 5; // Default max iterations
        this.rules = initializeRules();
    }

    /**
     * Initialize the optimization rules in priority order.
     * Higher priority rules run first.
     */
    private List<PhysicalOptimizationRule> initializeRules() {
        List<PhysicalOptimizationRule> rulesList = new ArrayList<>();

        // Phase 1: Index Optimization Rules (High Priority)
        rulesList.add(new RedundantScanEliminationRule());
        rulesList.add(new RangeScanConsolidationRule());
        rulesList.add(new IndexIntersectionRule());

        // Phase 2: Fallback Rules (Medium Priority)
        rulesList.add(new RangeScanFallbackRule());

        // Phase 3: Execution Order Optimization Rules (Medium Priority)
        rulesList.add(new SelectivityBasedOrderingRule());

        // Sort by priority (higher priority first)
        rulesList.sort((r1, r2) -> Integer.compare(r2.getPriority(), r1.getPriority()));

        return rulesList;
    }

    /**
     * Optimize a physical plan by applying optimization rules.
     *
     * @param metadata bucket metadata containing index information
     * @param plan     the physical plan to optimize
     * @param context  physical plan context for generating node IDs
     * @return optimized physical plan
     */
    public PhysicalNode optimize(BucketMetadata metadata, PhysicalNode plan, PlannerContext context) {
        PhysicalNode current = plan;
        boolean changed;
        int iterations = 0;

        do {
            changed = false;

            for (PhysicalOptimizationRule rule : rules) {
                if (rule.canApply(current)) {
                    PhysicalNode optimized = rule.apply(current, metadata, context);
                    if (!optimized.equals(current)) {
                        current = optimized;
                        changed = true;
                        // Note: We could add metrics tracking here
                    }
                }
            }

            iterations++;
        } while (changed && iterations < maxOptimizationPasses);

        return current;
    }
}
