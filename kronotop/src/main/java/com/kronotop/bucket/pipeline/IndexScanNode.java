package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.index.IndexDefinition;

import java.util.List;

public final class IndexScanNode extends AbstractScanNode {
    public IndexScanNode(int id, IndexDefinition index, List<Predicate> predicates) {
        super(id, index, predicates);
    }

    @Override
    public void execute(PipelineContext ctx, Transaction tr) {
        System.out.printf("IndexScanNode ==> %d, %s, %s%n", id(), index(), predicates());
    }
}