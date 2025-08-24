package com.kronotop.bucket.executor;


import java.util.ArrayList;
import java.util.List;

public abstract class AbstractScanNode extends AbstractDataFlowNode implements ScanNode {
    private final List<PredicateNode> predicates = new ArrayList<>();

    protected AbstractScanNode(List<PipelineNode> children) {
        super(children);
    }

    @Override
    public List<PredicateNode> predicates() {
        return predicates;
    }
}