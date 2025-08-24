package com.kronotop.bucket.executor;

import java.util.List;

public abstract class AbstractScanNode extends AbstractPipelineNode implements ScanNode {
    private final List<Predicate> children;

    protected AbstractScanNode(int id, List<Predicate> children) {
        super(id);
        this.children = children;
    }

    @Override
    public List<Predicate> predicates() {
        return children;
    }
}