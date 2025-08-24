package com.kronotop.bucket.executor;

import com.apple.foundationdb.Transaction;

import java.util.List;

public final class IndexScanNode extends AbstractScanNode {
    public IndexScanNode(int id, List<Predicate> children) {
        super(id, children);
    }

    @Override
    public void execute(PipelineContext ctx, Transaction tr) {
        System.out.println("Running IndexScanNode[id=" + id() +"] with predicates " + predicates());
    }
}