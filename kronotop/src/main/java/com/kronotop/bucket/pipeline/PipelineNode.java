package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;

public interface PipelineNode {
    int id();
    void execute(PipelineContext ctx, Transaction tr);
}
