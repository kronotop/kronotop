package com.kronotop.bucket.executor;

import java.util.List;

public abstract class AbstractScanNode implements ScanNode {
    private final List<PredicateNode> children;

    protected AbstractScanNode(List<PredicateNode> children) {
        this.children = children;
    }

    @Override
    public List<PredicateNode> predicates() {
        return children;
    }
}