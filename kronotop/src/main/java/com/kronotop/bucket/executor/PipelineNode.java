package com.kronotop.bucket.executor;

import com.apple.foundationdb.Transaction;

public interface PipelineNode {
    void execute(PipelineContext ctx, Transaction tr);
}
