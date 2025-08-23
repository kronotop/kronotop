package com.kronotop.bucket.executor;

import java.util.List;

public abstract class AbstractPipelineNode implements PipelineNode {
    private final List<PipelineNode> children;

    protected AbstractPipelineNode(List<PipelineNode> children) {
        this.children = children;
    }

    @Override
    public List<PipelineNode> children() {
        return children;
    }

    @Override
    public final void execute(PipelineContext ctx) {
        run(ctx);
    }

    protected abstract void run(PipelineContext ctx);
}
