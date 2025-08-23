package com.kronotop.bucket.executor;

import com.apple.foundationdb.Transaction;

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
    public final void execute(Transaction tr, PipelineContext ctx) {
        run(tr, ctx);
    }

    protected abstract void run(Transaction tr, PipelineContext ctx);
}
