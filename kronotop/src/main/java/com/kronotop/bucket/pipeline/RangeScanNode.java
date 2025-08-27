package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.index.IndexDefinition;

public class RangeScanNode implements ScanNode {
    @Override
    public IndexDefinition index() {
        return null;
    }

    @Override
    public void execute(PipelineContext ctx, Transaction tr) {

    }

    @Override
    public int id() {
        return 0;
    }
}
