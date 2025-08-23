package com.kronotop.bucket.executor;

public class PipelineExecutor {
    private final PipelineNode root;

    public PipelineExecutor(PipelineNode root) {
        this.root = root;
    }

    public void run(PipelineContext ctx) {
        executeNode(root, ctx);
    }

    private void executeNode(PipelineNode node, PipelineContext ctx) {
        for (PipelineNode child : node.children()) {
            executeNode(child, ctx);
        }
        System.out.println("running " + node);
        node.execute(ctx);
    }
}
