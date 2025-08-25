package com.kronotop.bucket.pipeline;

public interface DataFetchNode extends PipelineNode {
    void execute(PipelineContext ctx);
}
