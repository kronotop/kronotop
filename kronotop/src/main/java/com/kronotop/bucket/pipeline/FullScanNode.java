package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.index.IndexDefinition;

public class FullScanNode extends AbstractTransactionAwareNode implements ScanNode<FullScanPredicate> {
    private final IndexDefinition index = DefaultIndexDefinition.ID;
    private final FullScanPredicate predicates;

    protected FullScanNode(int id, FullScanPredicate predicates) {
        super(id);
        this.predicates = predicates;
    }

    @Override
    public IndexDefinition index() {
        return index;
    }

    @Override
    public FullScanPredicate predicate() {
        return predicates;
    }

    @Override
    public void execute(PipelineContext ctx, Transaction tr) {

    }
}
