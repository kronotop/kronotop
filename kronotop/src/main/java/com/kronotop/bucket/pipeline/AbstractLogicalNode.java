package com.kronotop.bucket.pipeline;

import java.util.List;

public abstract class AbstractLogicalNode extends AbstractPipelineNode implements LogicalNode {
    private final List<PipelineNode> children;

    public AbstractLogicalNode(int id, List<PipelineNode> children) {
        super(id);
        this.children = children;
    }

    @Override
    public List<PipelineNode> children() {
        return children;
    }
}
