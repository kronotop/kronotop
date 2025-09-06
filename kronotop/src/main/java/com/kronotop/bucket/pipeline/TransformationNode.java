package com.kronotop.bucket.pipeline;

public interface TransformationNode extends ScanNode {
    void transform(QueryContext ctx);
}
