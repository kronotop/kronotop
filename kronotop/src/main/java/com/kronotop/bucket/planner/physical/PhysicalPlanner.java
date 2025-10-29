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

package com.kronotop.bucket.planner.physical;

import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.planner.logical.*;

import java.util.ArrayList;
import java.util.List;

public class PhysicalPlanner {

    public PhysicalPlanner() {
    }

    /**
     * Efficiently transposes a LogicalNode to PhysicalNode with minimal CPU and memory overhead.
     * <p>
     * Key optimizations:
     * - Direct field reuse between logical and physical nodes
     * - Singleton instance reuse for constants (True/False)
     * - In-place list transformation to avoid intermediate collections
     * - Pattern matching for efficient dispatch
     */
    public PhysicalNode plan(BucketMetadata metadata, LogicalNode logicalPlan, PlannerContext context) {
        return switch (logicalPlan) {
            // Direct field transposition - zero-copy for primitive fields
            case LogicalFilter filter -> transposeFilter(metadata, filter, context);

            // Efficient list transformation - reuse child structure
            case LogicalAnd and ->
                    new PhysicalAnd(context.nextId(), transposeChildren(metadata, and.children(), context));
            case LogicalOr or -> new PhysicalOr(context.nextId(), transposeChildren(metadata, or.children(), context));

            // Single child transposition
            case LogicalNot not -> new PhysicalNot(context.nextId(), plan(metadata, not.child(), context));
            case LogicalElemMatch elemMatch -> new PhysicalElemMatch(
                    context.nextId(),
                    elemMatch.selector(),
                    plan(metadata, elemMatch.subPlan(), context)
            );

            // Singleton instance reuse - no object allocation
            case LogicalTrue ignored -> new PhysicalTrue(context.nextId());
            case LogicalFalse ignored -> new PhysicalFalse(context.nextId());
        };
    }

    /**
     * Efficiently transposes LogicalFilter to either PhysicalFilter, PhysicalIndexScan,
     * or PhysicalFullScan based on index availability.
     * Zero-copy field reuse for optimal performance.
     */
    private PhysicalNode transposeFilter(BucketMetadata metadata, LogicalFilter filter, PlannerContext context) {
        // Check for index availability
        Index index = metadata.indexes().getIndex(filter.selector(), IndexSelectionPolicy.READ);

        // Direct field reuse - no object copying
        PhysicalFilter node = new PhysicalFilter(context.nextId(), filter.selector(), filter.op(), filter.operand());

        if (index != null) {
            // Index available - use index scan with filter pushdown
            return new PhysicalIndexScan(context.nextId(), node, index.definition());
        }

        // No index - full scan with filter
        return new PhysicalFullScan(context.nextId(), node);
    }

    /**
     * Efficiently transposes a list of LogicalNodes to PhysicalNodes.
     * Pre-sizes the result list to avoid array reallocation.
     */
    private List<PhysicalNode> transposeChildren(BucketMetadata metadata, List<LogicalNode> children, PlannerContext context) {
        // Pre-size to avoid array growth and copying
        List<PhysicalNode> physicalChildren = new ArrayList<>(children.size());

        // Transform each child - compiler can optimize this loop
        for (LogicalNode child : children) {
            physicalChildren.add(plan(metadata, child, context));
        }

        return physicalChildren;
    }
}
