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

package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.CollatorCache;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.handlers.protocol.SortDirection;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;

import java.util.Collections;
import java.util.List;

/**
 * Base context for scan operations providing index subspace, execution state, and sort direction.
 * Subclasses specialize for different scan strategies (index scan, full scan, range scan).
 */
abstract class ScanContext {
    private final int nodeId;
    private final DirectorySubspace indexSubspace;
    private final ExecutionState state;
    private final SortDirection sortDirection;
    private final List<BqlValue> parameters;

    protected ScanContext(int nodeId, DirectorySubspace indexSubspace, ExecutionState state, SortDirection sortDirection) {
        this(nodeId, indexSubspace, state, sortDirection, Collections.emptyList());
    }

    protected ScanContext(int nodeId, DirectorySubspace indexSubspace, ExecutionState state, SortDirection sortDirection, List<BqlValue> parameters) {
        this.nodeId = nodeId;
        this.indexSubspace = indexSubspace;
        this.state = state;
        this.sortDirection = sortDirection;
        this.parameters = parameters;
    }

    /**
     * Returns the FDB directory subspace containing the index entries.
     */
    public DirectorySubspace indexSubspace() {
        return indexSubspace;
    }

    /**
     * Returns the execution state tracking cursor position and bounds.
     */
    public ExecutionState state() {
        return state;
    }

    /**
     * Returns the sort direction (ASC or DESC) for this scan.
     */
    public SortDirection getSortDirection() {
        return sortDirection;
    }

    /**
     * Returns true if scanning in descending order (DESC).
     */
    public boolean isReverse() {
        return sortDirection == SortDirection.DESC;
    }

    /**
     * Returns the pipeline node ID for cursor checkpoint tracking.
     */
    public int nodeId() {
        return nodeId;
    }

    /**
     * Returns the parameter list for resolving Param operands.
     */
    public List<BqlValue> getParameters() {
        return parameters;
    }
}

/**
 * Context for index scan operations with equality or comparison predicates on a single field index.
 */
class IndexScanContext extends ScanContext {
    private final IndexScanPredicate predicate;
    private final SingleFieldIndexDefinition index;
    private final Collation collation;
    private final CollatorCache collatorCache;

    public IndexScanContext(int nodeId, DirectorySubspace subspace, ExecutionState state, SortDirection sortDirection,
                            IndexScanPredicate predicate, SingleFieldIndexDefinition indexDefinition, List<BqlValue> parameters,
                            Collation collation, CollatorCache collatorCache) {
        super(nodeId, subspace, state, sortDirection, parameters);
        this.predicate = predicate;
        this.index = indexDefinition;
        this.collation = collation;
        this.collatorCache = collatorCache;
    }

    public IndexScanPredicate predicate() {
        return predicate;
    }

    public SingleFieldIndexDefinition index() {
        return index;
    }

    public Collation collation() {
        return collation;
    }

    public CollatorCache collatorCache() {
        return collatorCache;
    }
}

/**
 * Context for full scan operations traversing all entries in the primary index.
 */
class FullScanContext extends ScanContext {
    public FullScanContext(int nodeId, DirectorySubspace subspace, ExecutionState state, SortDirection sortDirection) {
        super(nodeId, subspace, state, sortDirection);
    }
}

/**
 * Context for range scan operations with bounded predicates (e.g., $gt/$lt combinations).
 */
class RangeScanContext extends ScanContext {
    private final RangeScanPredicate predicate;
    private final SingleFieldIndexDefinition index;
    private final Collation collation;
    private final CollatorCache collatorCache;

    public RangeScanContext(int nodeId, DirectorySubspace subspace, ExecutionState state, SortDirection sortDirection,
                            RangeScanPredicate predicate, SingleFieldIndexDefinition index, List<BqlValue> parameters,
                            Collation collation, CollatorCache collatorCache) {
        super(nodeId, subspace, state, sortDirection, parameters);
        this.predicate = predicate;
        this.index = index;
        this.collation = collation;
        this.collatorCache = collatorCache;
    }

    public RangeScanPredicate predicate() {
        return predicate;
    }

    public SingleFieldIndexDefinition index() {
        return index;
    }

    public Collation collation() {
        return collation;
    }

    public CollatorCache collatorCache() {
        return collatorCache;
    }
}