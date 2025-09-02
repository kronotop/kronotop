package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;

import java.util.List;

public class IntersectionNode extends AbstractLogicalNode implements LogicalNode {
    public IntersectionNode(int id, ExecutionStrategy strategy, List<PipelineNode> children) {
        super(id, strategy, children);
    }

    @Override
    public void execute(QueryContext ctx, Transaction tr) {
    }
}