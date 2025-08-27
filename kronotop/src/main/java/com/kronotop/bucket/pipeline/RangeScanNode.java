package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.index.IndexDefinition;

public class RangeScanNode extends AbstractTransactionAwareNode implements ScanNode {
    private final RangeScanPredicate predicate;

    protected RangeScanNode(int id, RangeScanPredicate predicate) {
        super(id);
        this.predicate = predicate;
    }

    @Override
    public IndexDefinition index() {
        return null;
    }

    @Override
    public void execute(PipelineContext ctx, Transaction tr) {

    }
}
