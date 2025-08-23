package com.kronotop.bucket.executor;

import com.kronotop.bucket.planner.physical.PhysicalIndexScan;

import java.util.List;

public final class IndexScanNode extends AbstractPipelineNode {
    private final PhysicalIndexScan indexScan;

    public IndexScanNode(PhysicalIndexScan indexScan, List<PipelineNode> children) {
        super(children);
        this.indexScan = indexScan;
    }

    @Override
    protected void run(PipelineContext ctx) {
    }
}