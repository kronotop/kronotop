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

package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.index.IndexDefinition;

abstract class ScanContext {
    private final int nodeId;
    private final DirectorySubspace indexSubspace;
    private final ExecutionState state;
    private final boolean isReverse;

    protected ScanContext(int nodeId, DirectorySubspace indexSubspace, ExecutionState state, boolean isReverse) {
        this.nodeId = nodeId;
        this.indexSubspace = indexSubspace;
        this.state = state;
        this.isReverse = isReverse;
    }

    public DirectorySubspace indexSubspace() {
        return indexSubspace;
    }

    public ExecutionState state() {
        return state;
    }

    public boolean isReverse() {
        return isReverse;
    }

    public int nodeId() {
        return nodeId;
    }
}

class IndexScanContext extends ScanContext {
    private final IndexScanPredicate predicate;
    private final IndexDefinition index;

    public IndexScanContext(int nodeId, DirectorySubspace subspace, ExecutionState state, boolean isReverse, IndexScanPredicate predicate, IndexDefinition indexDefinition) {
        super(nodeId, subspace, state, isReverse);
        this.predicate = predicate;
        this.index = indexDefinition;
    }

    public IndexScanPredicate predicate() {
        return predicate;
    }

    public IndexDefinition index() {
        return index;
    }
}

class FullScanContext extends ScanContext {
    public FullScanContext(int nodeId, DirectorySubspace subspace, ExecutionState state, boolean isReverse) {
        super(nodeId, subspace, state, isReverse);
    }
}

class RangeScanContext extends ScanContext {
    private final RangeScanPredicate predicate;

    public RangeScanContext(int nodeId, DirectorySubspace subspace, ExecutionState state, boolean isReverse, RangeScanPredicate predicate) {
        super(nodeId, subspace, state, isReverse);
        this.predicate = predicate;
    }

    public RangeScanPredicate predicate() {
        return predicate;
    }
}