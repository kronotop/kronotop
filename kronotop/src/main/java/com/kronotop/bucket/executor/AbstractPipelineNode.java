package com.kronotop.bucket.executor;

public abstract class AbstractPipelineNode {
    private final int id;

    public AbstractPipelineNode(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }
}
