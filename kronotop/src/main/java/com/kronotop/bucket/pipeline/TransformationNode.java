package com.kronotop.bucket.pipeline;

public interface TransformationNode extends PipelineNode {
    void transform(QueryContext ctx);
}
