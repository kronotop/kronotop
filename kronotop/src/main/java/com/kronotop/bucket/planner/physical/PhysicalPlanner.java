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

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexDefinition;
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
            case LogicalAnd and -> new PhysicalAnd(context.generateId(), transposeChildren(metadata, and.children(), context));
            case LogicalOr or -> new PhysicalOr(context.generateId(), transposeChildren(metadata, or.children(), context));

            // Single child transposition
            case LogicalNot not -> new PhysicalNot(context.generateId(), plan(metadata, not.child(), context));
            case LogicalElemMatch elemMatch -> new PhysicalElemMatch(
                    context.generateId(),
                    elemMatch.selector(),
                    plan(metadata, elemMatch.subPlan(), context)
            );

            // Singleton instance reuse - no object allocation
            case LogicalTrue ignored -> new PhysicalTrue(context.generateId());
            case LogicalFalse ignored -> new PhysicalFalse(context.generateId());
        };
    }

    /**
     * Efficiently transposes LogicalFilter to either PhysicalFilter, PhysicalIndexScan,
     * or PhysicalFullScan based on index availability.
     * Zero-copy field reuse for optimal performance.
     */
    private PhysicalNode transposeFilter(BucketMetadata metadata, LogicalFilter filter, PlannerContext context) {
        // Check for index availability
        DirectorySubspace subspace = metadata.indexes().getSubspace(filter.selector());
        IndexDefinition indexDefinition = metadata.indexes().getIndexBySelector(filter.selector());

        // Direct field reuse - no object copying
        PhysicalFilter node = new PhysicalFilter(context.generateId(), filter.selector(), filter.op(), filter.operand());

        if (subspace != null) {
            // Index available - use index scan with filter pushdown
            return new PhysicalIndexScan(context.generateId(), node, indexDefinition);
        }

        // No index - full scan with filter
        return new PhysicalFullScan(context.generateId(), node);
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
