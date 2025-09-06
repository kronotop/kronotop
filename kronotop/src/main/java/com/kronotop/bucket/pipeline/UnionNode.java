package com.kronotop.bucket.pipeline;

import java.util.List;

public class UnionNode extends AbstractLogicalNode implements LogicalNode {
    public UnionNode(int id, ExecutionStrategy strategy, List<PipelineNode> children) {
        super(id, strategy, children);
    }

    @Override
    public void execute(QueryContext ctx) {
    }
}
