package com.kronotop.bucket.pipeline;

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
        switch (node) {
            case ScanNode scanNode -> {
                scanNode.execute(ctx, tr);
            }
            case LogicalNode logicalNode -> {
                for (PipelineNode child : logicalNode.children()) {
                    executeNode(tr, ctx, child);
                }
                logicalNode.execute(ctx);
            }
            default -> throw new IllegalStateException("Unexpected PipelineNode: " + node);
        }
    }
}
