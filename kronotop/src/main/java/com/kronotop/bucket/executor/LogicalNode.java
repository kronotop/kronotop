package com.kronotop.bucket.executor;

import java.util.List;

public interface LogicalNode extends PipelineNode {
    List<PipelineNode> children();
}
