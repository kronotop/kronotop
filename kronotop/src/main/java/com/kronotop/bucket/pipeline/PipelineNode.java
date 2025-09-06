package com.kronotop.bucket.pipeline;

public interface PipelineNode {
    int id();
    PipelineNode next();
    void connectNext(PipelineNode node);
}
