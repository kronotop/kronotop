package com.kronotop.bucket.pipeline;

import java.util.List;

public interface LogicalNode extends PipelineNode {
    List<PipelineNode> children();
}
