package com.kronotop.bucket.executor;

import java.util.List;

public interface PipelineNode {
    void execute(PipelineContext ctx);
    List<PipelineNode> children();
}
