package com.kronotop.bucket.pipeline;

public abstract class AbstractPipelineNode {
    private final int id;
    private volatile PipelineNode next;

    public AbstractPipelineNode(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public PipelineNode next() {
        return next;
    }

    public void connectNext(PipelineNode node) {
        if (next != null) {
            throw new IllegalStateException("next node has already been set for id=" + id);
        }
        next = node;
    }
}
