package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.index.IndexDefinition;

import java.util.List;

public class FullScanNode extends AbstractTransactionAwareNode implements ScanNode<FullScanPredicate> {
    private final IndexDefinition index = DefaultIndexDefinition.ID;
    private final List<FullScanPredicate> predicates;

    protected FullScanNode(int id, List<FullScanPredicate> predicates) {
        super(id);
        this.predicates = predicates;
    }

    @Override
    public IndexDefinition index() {
        return index;
    }

    @Override
    public List<FullScanPredicate> predicates() {
        return predicates;
    }

    @Override
    public void execute(PipelineContext ctx, Transaction tr) {

    }
}
