package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.index.IndexDefinition;

import java.util.List;

public abstract class AbstractScanNode extends AbstractPipelineNode implements ScanNode {
    private final List<IndexScanPredicate> children;
    private final IndexDefinition index;

    protected AbstractScanNode(int id, IndexDefinition index, List<IndexScanPredicate> children) {
        super(id);
        this.index = index;
        this.children = children;
    }

    @Override
    public List<IndexScanPredicate> predicates() {
        return children;
    }

    @Override
    public IndexDefinition index() {
        return index;
    }
}