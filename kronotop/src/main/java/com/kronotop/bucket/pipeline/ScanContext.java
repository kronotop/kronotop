package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.index.IndexDefinition;

abstract class ScanContext {
    protected final DirectorySubspace indexSubspace;
    protected final Cursor cursor;

    protected ScanContext(DirectorySubspace indexSubspace, Cursor cursor) {
        this.indexSubspace = indexSubspace;
        this.cursor = cursor;
    }

    public DirectorySubspace indexSubspace() {
        return indexSubspace;
    }

    public Cursor cursor() {
        return cursor;
    }
}

class IndexScanContext extends ScanContext {
    private final Predicate predicate;
    private final IndexDefinition index;

    public IndexScanContext(DirectorySubspace indexSubspace, Cursor cursor, Predicate predicate, IndexDefinition indexDefinition) {
        super(indexSubspace, cursor);
        this.predicate = predicate;
        this.index = indexDefinition;
    }

    public Predicate filter() {
        return predicate;
    }

    public IndexDefinition index() {
        return index;
    }
}