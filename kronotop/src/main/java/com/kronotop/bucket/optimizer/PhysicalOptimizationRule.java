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

/**
 * Interface for physical plan optimization rules.
 * Rules transform PhysicalNode trees to improve query execution performance.
 */
public interface PhysicalOptimizationRule {

    /**
     * Apply this optimization rule to a physical node tree.
     *
     * @param node     the physical node to optimize
     * @param metadata bucket metadata containing index information
     * @param context  physical plan context for generating node IDs
     * @return optimized physical node (may be the same instance if no optimization applied)
     */
    PhysicalNode apply(PhysicalNode node, BucketMetadata metadata, PlannerContext context);

    /**
     * Get the name of this optimization rule.
     *
     * @return rule name for logging and metrics
     */
    String getName();

    /**
     * Get the priority of this rule. Higher priority rules execute first.
     *
     * @return priority value (higher values = higher priority)
     */
    int getPriority();

    /**
     * Quick check if this rule can be applied to the given node.
     * This should be a fast check to avoid expensive rule application attempts.
     *
     * @param node the physical node to check
     * @return true if this rule might be applicable
     */
    boolean canApply(PhysicalNode node);
}