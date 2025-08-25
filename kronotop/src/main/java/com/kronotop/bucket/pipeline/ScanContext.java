package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.index.IndexDefinition;

abstract class ScanContext {
    protected final int nodeId;
    protected final DirectorySubspace indexSubspace;
    protected final Cursor cursor;
    protected final boolean isReverse;

    protected ScanContext(int nodeId, DirectorySubspace indexSubspace, Cursor cursor, boolean isReverse) {
        this.nodeId = nodeId;
        this.indexSubspace = indexSubspace;
        this.cursor = cursor;
        this.isReverse = isReverse;
    }

    public DirectorySubspace indexSubspace() {
        return indexSubspace;
    }

    public Cursor cursor() {
        return cursor;
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

    public IndexScanContext(int nodeId, DirectorySubspace indexSubspace, Cursor cursor, boolean isReverse, IndexScanPredicate predicate, IndexDefinition indexDefinition) {
        super(nodeId, indexSubspace, cursor, isReverse);
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