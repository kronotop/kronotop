package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.index.IndexDefinition;

abstract class ScanContext {
    protected final DirectorySubspace indexSubspace;
    protected final Cursor cursor;
    protected final boolean isReverse;

    protected ScanContext(DirectorySubspace indexSubspace, Cursor cursor, boolean isReverse) {
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
}

class IndexScanContext extends ScanContext {
    private final Predicate predicate;
    private final IndexDefinition index;

    public IndexScanContext(DirectorySubspace indexSubspace, Cursor cursor, boolean isReverse, Predicate predicate, IndexDefinition indexDefinition) {
        super(indexSubspace, cursor, isReverse);
        this.predicate = predicate;
        this.index = indexDefinition;
    }

    public Predicate predicate() {
        return predicate;
    }

    public IndexDefinition indexDefinition() {
        return index;
    }
}