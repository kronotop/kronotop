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

import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.bucket.planner.physical.PlannerContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Applies rule-based optimizations to a PhysicalNode tree.
 * <p>
 * Runs after the PhysicalPlanner and before the PipelineExecutor. Each rule
 * rewrites the plan to make query execution cheaper. Rules run in priority order,
 * highest first, and the whole list repeats until a pass changes nothing, at most
 * 5 passes.
 */
public class Optimizer {
    private final List<PhysicalOptimizationRule> rules;
    private final int maxOptimizationPasses;

    public Optimizer() {
        this.maxOptimizationPasses = 5;
        this.rules = initializeRules();
    }

    /**
     * Returns the rules sorted by priority, highest first. Rules with the same
     * priority keep the order in which they are added.
     */
    private List<PhysicalOptimizationRule> initializeRules() {
        List<PhysicalOptimizationRule> rulesList = new ArrayList<>();

        rulesList.add(new RedundantScanEliminationRule());
        rulesList.add(new RangeScanConsolidationRule());
        rulesList.add(new IndexIntersectionRule());
        rulesList.add(new RangeScanFallbackRule());
        rulesList.add(new SelectivityBasedOrderingRule());

        // Stable sort, higher priority first
        rulesList.sort((r1, r2) -> Integer.compare(r2.getPriority(), r1.getPriority()));

        return rulesList;
    }

    /**
     * Applies the optimization rules to a physical plan.
     *
     * @param context planner context
     * @param plan    the physical plan to optimize
     * @return optimized physical plan
     */
    public PhysicalNode optimize(PlannerContext context, PhysicalNode plan) {
        PhysicalNode current = plan;
        boolean changed;
        int iterations = 0;

        do {
            changed = false;

            for (PhysicalOptimizationRule rule : rules) {
                if (rule.canApply(current)) {
                    PhysicalNode optimized = rule.apply(context, current);
                    if (!optimized.equals(current)) {
                        current = optimized;
                        changed = true;
                    }
                }
            }

            iterations++;
        } while (changed && iterations < maxOptimizationPasses);

        return current;
    }
}
