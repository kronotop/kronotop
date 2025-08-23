package com.kronotop.bucket.executor;

import com.kronotop.bucket.planner.physical.PhysicalFilter;
import com.kronotop.bucket.planner.physical.PhysicalIndexScan;
import com.kronotop.bucket.planner.physical.PhysicalNode;

import java.util.List;

public class PipelineRewriter {
    public static PipelineNode rewrite(PhysicalNode plan) {
        switch (plan){
            case PhysicalIndexScan physicalIndexScan -> {
                PipelineNode child = rewrite(physicalIndexScan.node());
                return new IndexScanNode(physicalIndexScan, List.of(child));
            }
            case PhysicalFilter physicalFilter -> {
                return new FilterNode(physicalFilter, List.of());
            }
            default -> throw new IllegalStateException("Unexpected PhysicalNode: " + plan);
        }
    }
}
