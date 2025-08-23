package com.kronotop.bucket.executor;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.planner.physical.PhysicalFilter;

import java.util.List;

public class FilterNode extends AbstractPipelineNode{
    public FilterNode(PhysicalFilter physicalFilter, List<PipelineNode> children) {
        super(children);
    }

    @Override
    protected void run(Transaction tr, PipelineContext ctx) {

    }
}
