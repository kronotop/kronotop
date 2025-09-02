package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;

public interface TransactionAwareNode extends PipelineNode {
    void execute(QueryContext ctx, Transaction tr);
}
