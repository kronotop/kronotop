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

package com.kronotop.bucket.executor;

import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.planner.physical.PhysicalNode;

import java.util.List;
import java.util.Set;

/**
 * Provides context information for query execution to enable context-aware cursor management.
 * This helps the cursor utilities make intelligent decisions about cursor aggregation and selection.
 * Enhanced with node tracking for independent cursor coordination.
 */
public record CompositeQueryContext(
        CompositeQueryType queryType,
        List<IndexDefinition> involvedIndexes,
        Set<Integer> activeNodeIds,
        PhysicalNode physicalPlan,
        boolean isMultiIndex,
        boolean hasMixedIndexedNonIndexed,
        Set<Integer> activeNodes,
        Integer currentNode
) {

    /**
     * Type of query being executed, which affects cursor management strategy.
     */
    public enum CompositeQueryType {
        /** Multiple index intersection (AND) - use the most restrictive cursor */
        MULTI_INDEX_AND,
        /** Multiple index union (OR) - use least restrictive cursor */
        MULTI_INDEX_OR,
        /** Mixed indexed and non-indexed conditions - use aggregated approach */
        MIXED_QUERY,
        /** Full bucket scan - use aggregated cursor for pagination */
        FULL_BUCKET_SCAN,
        /** Range scan on a single index. Use a specific node cursor */
        RANGE_SCAN
    }

    /**
     * Creates a context for multi-index AND operations.
     */
    public static CompositeQueryContext multiIndexAnd(List<IndexDefinition> indexes, Set<Integer> nodeIds, PhysicalNode plan) {
        return new CompositeQueryContext(
                CompositeQueryType.MULTI_INDEX_AND,
                indexes,
                nodeIds,
                plan,
                true,
                false,
                Set.of(),
                null
        );
    }

    /**
     * Creates a context for multi-index OR operations.
     */
    public static CompositeQueryContext multiIndexOr(List<IndexDefinition> indexes, Set<Integer> nodeIds, PhysicalNode plan) {
        return new CompositeQueryContext(
                CompositeQueryType.MULTI_INDEX_OR,
                indexes,
                nodeIds,
                plan,
                true,
                false,
                Set.of(),
                null
        );
    }

    /**
     * Creates a context for mixed indexed and non-indexed queries.
     */
    public static CompositeQueryContext mixedQuery(List<IndexDefinition> indexes, Set<Integer> nodeIds, PhysicalNode plan) {
        return new CompositeQueryContext(
                CompositeQueryType.MIXED_QUERY,
                indexes,
                nodeIds,
                plan,
                !indexes.isEmpty(),
                true,
                Set.of(),
                null
        );
    }

    /**
     * Creates a context for full bucket scans.
     */
    public static CompositeQueryContext fullBucketScan(PhysicalNode plan) {
        return new CompositeQueryContext(
                CompositeQueryType.FULL_BUCKET_SCAN,
                List.of(),
                Set.of(),
                plan,
                false,
                false,
                Set.of(),
                null
        );
    }

    /**
     * Creates a context for range scan operations.
     */
    public static CompositeQueryContext rangeScan(IndexDefinition index, int nodeId, PhysicalNode plan) {
        return new CompositeQueryContext(
                CompositeQueryType.RANGE_SCAN,
                List.of(index),
                Set.of(nodeId),
                plan,
                false,
                false,
                Set.of(),
                null
        );
    }
}