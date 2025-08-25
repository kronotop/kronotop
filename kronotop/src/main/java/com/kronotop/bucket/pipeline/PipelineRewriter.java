package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.PhysicalAnd;
import com.kronotop.bucket.planner.physical.PhysicalFilter;
import com.kronotop.bucket.planner.physical.PhysicalIndexScan;
import com.kronotop.bucket.planner.physical.PhysicalNode;

import java.util.ArrayList;
import java.util.List;

public class PipelineRewriter {
    public static PipelineNode rewrite(PhysicalNode plan) {
        return switch (plan) {
            case PhysicalAnd physicalAnd -> {
                List<PipelineNode> children = new ArrayList<>();
                for (PhysicalNode child : physicalAnd.children()) {
                    children.add(rewrite(child));
                }
                yield new IntersectionNode(physicalAnd.id(), children);
            }
            case PhysicalIndexScan indexScan -> {
                PhysicalNode physicalNode = indexScan.node();
                if (!(physicalNode instanceof PhysicalFilter(
                        int id, String selector, Operator op, Object operand
                ))) {
                    throw new IllegalStateException("PhysicalNode must be a PhysicalFilter instance");
                }
                IndexScanPredicate predicate = new IndexScanPredicate(id, selector, op, operand);
                yield new IndexScanNode(indexScan.id(), indexScan.index(), List.of(predicate));
            }
            default -> throw new IllegalStateException("Unexpected PhysicalNode: " + plan);
        };
    }
}
