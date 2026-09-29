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

/**
 * A rule that rewrites a PhysicalNode tree into a cheaper equivalent.
 */
public interface PhysicalOptimizationRule {

    /**
     * Applies this rule to a physical node tree.
     *
     * @param context planner context containing metadata and configuration
     * @param node    the physical node to optimize
     * @return optimized physical node (maybe the same instance if no optimization applied)
     */
    PhysicalNode apply(PlannerContext context, PhysicalNode node);

    /**
     * Returns the rule name, used in logs and metrics.
     */
    String getName();

    /**
     * Returns the rule priority. Higher priority rules run first.
     */
    int getPriority();

    /**
     * Returns false if this rule can never apply to the node. Must be cheap.
     *
     * @param node the physical node to check
     * @return true if this rule might be applicable
     */
    boolean canApply(PhysicalNode node);
}