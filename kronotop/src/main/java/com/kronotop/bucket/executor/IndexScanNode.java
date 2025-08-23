package com.kronotop.bucket.executor;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.planner.physical.PhysicalIndexScan;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;

public final class IndexScanNode extends AbstractPipelineNode {
    private final PhysicalIndexScan indexScan;

    public IndexScanNode(PhysicalIndexScan indexScan, List<PipelineNode> children) {
        super(children);
        this.indexScan = indexScan;
    }

    @Override
    protected void run(Transaction tr, PipelineContext ctx) {
        LinkedHashMap<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();
    }
}