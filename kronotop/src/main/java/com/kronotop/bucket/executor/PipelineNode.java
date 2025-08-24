package com.kronotop.bucket.executor;

import com.apple.foundationdb.Transaction;

public interface PipelineNode {
    int id();
    void execute(PipelineContext ctx, Transaction tr);
}
