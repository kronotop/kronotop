package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.DefaultIndexDefinition;
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