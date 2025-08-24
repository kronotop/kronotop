package com.kronotop.bucket.executor;

import com.kronotop.bucket.index.IndexDefinition;

import java.util.List;

public abstract class AbstractScanNode extends AbstractPipelineNode implements ScanNode {
    private final List<Predicate> children;
    private final IndexDefinition index;

    protected AbstractScanNode(int id, IndexDefinition index, List<Predicate> children) {
        super(id);
        this.index = index;
        this.children = children;
    }

    @Override
    public List<Predicate> predicates() {
        return children;
    }

    @Override
    public IndexDefinition index() {
        return index;
    }
}