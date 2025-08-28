package com.kronotop.bucket.pipeline;

import java.util.List;

public interface LogicalNode extends TransactionAwareNode {
    ExecutionStrategy strategy();

    List<PipelineNode> children();
}
