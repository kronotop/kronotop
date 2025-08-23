package com.kronotop.bucket.executor;

import com.kronotop.bucket.planner.physical.PhysicalFilter;
import com.kronotop.bucket.planner.physical.PhysicalIndexScan;
import com.kronotop.bucket.planner.physical.PhysicalNode;

import java.util.List;

public class PipelineRewriter {
    public static PipelineNode rewrite(PhysicalNode plan) {
        switch (plan) {
            case PhysicalIndexScan physicalIndexScan -> {
                PipelineNode child = rewrite(physicalIndexScan.node());
                List<PipelineNode> children = child != null ? List.of(child) : List.of();
                return new IndexScanNode(physicalIndexScan, children);
            }
            case PhysicalFilter ignore -> {
                // Leaf node
                return null;
            }
            default -> throw new IllegalStateException("Unexpected PhysicalNode: " + plan);
        }
    }
}
