package com.kronotop.bucket.pipeline;

import java.util.List;

public interface LogicalNode extends PipelineNode {
    ExecutionStrategy strategy();
    List<PipelineNode> children();
    void execute(QueryContext ctx);
}
