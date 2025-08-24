package com.kronotop.bucket.executor;

import com.apple.foundationdb.Transaction;

public class PipelineExecutor {
    private final PipelineNode root;

    public PipelineExecutor(PipelineNode root) {
        this.root = root;
    }

    public void run(Transaction tr, PipelineContext ctx) {
        executeNode(tr, ctx, root);
    }

    private void executeNode(Transaction tr, PipelineContext ctx, PipelineNode node) {
        for (PipelineNode child : node.children()) {
            executeNode(tr, ctx, child);
        }
        node.execute(tr, ctx);
    }
}
