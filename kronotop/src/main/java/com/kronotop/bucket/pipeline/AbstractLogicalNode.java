package com.kronotop.bucket.pipeline;

import java.util.List;

public abstract class AbstractLogicalNode extends AbstractPipelineNode implements LogicalNode {
    private final List<PipelineNode> children;
    private final ExecutionStrategy strategy;

    public AbstractLogicalNode(int id, ExecutionStrategy strategy, List<PipelineNode> children) {
        super(id);
        this.children = children;
        this.strategy = strategy;
    }

    @Override
    public ExecutionStrategy strategy() {
        return strategy;
    }

    @Override
    public List<PipelineNode> children() {
        return children;
    }
}
