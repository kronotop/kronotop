package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.index.IndexDefinition;

abstract class ScanContext {
    protected final int nodeId;
    protected final DirectorySubspace indexSubspace;
    protected final ExecutionState state;
    protected final boolean isReverse;

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

class SecondaryIndexScanContext extends ScanContext {
    private final IndexScanPredicate predicate;
    private final IndexDefinition index;

    public SecondaryIndexScanContext(int nodeId, DirectorySubspace indexSubspace, ExecutionState state, boolean isReverse, IndexScanPredicate predicate, IndexDefinition indexDefinition) {
        super(nodeId, indexSubspace, state, isReverse);
        this.predicate = predicate;
        this.index = indexDefinition;
    }

    public IndexScanPredicate predicate() {
        return predicate;
    }

    public IndexDefinition indexDefinition() {
        return index;
    }
}

class PrimaryIndexScanContext extends ScanContext {
    private final FullScanPredicate predicate;
    private final IndexDefinition index;

    public PrimaryIndexScanContext(int nodeId, DirectorySubspace indexSubspace, ExecutionState state, boolean isReverse, FullScanPredicate predicate) {
        super(nodeId, indexSubspace, state, isReverse);
        this.predicate = predicate;
        this.index = DefaultIndexDefinition.ID;
    }

    public FullScanPredicate predicate() {
        return predicate;
    }

    public IndexDefinition indexDefinition() {
        return index;
    }
}