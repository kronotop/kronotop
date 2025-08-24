package com.kronotop.bucket.executor;

import com.apple.foundationdb.Transaction;

import java.util.List;

public final class IndexScanNode extends AbstractScanNode {
    public IndexScanNode(List<PredicateNode> children) {
        super(children);
    }

    @Override
    public void execute(PipelineContext ctx, Transaction tr) {
    }
}