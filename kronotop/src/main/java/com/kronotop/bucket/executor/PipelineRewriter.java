package com.kronotop.bucket.executor;

import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.PhysicalFilter;
import com.kronotop.bucket.planner.physical.PhysicalIndexScan;
import com.kronotop.bucket.planner.physical.PhysicalNode;

import java.util.List;

public class PipelineRewriter {
    public static PipelineNode rewrite(PhysicalNode plan) {
        return switch (plan) {
            case PhysicalIndexScan indexScan -> {
                PhysicalNode physicalNode = indexScan.node();
                if (!(physicalNode instanceof PhysicalFilter(
                        int id, String selector, Operator op, Object operand
                ))) {
                    throw new IllegalStateException("PhysicalNode must be a PhysicalFilter instance");
                }
                Predicate predicate = new Predicate(id, selector, op, operand);
                yield new IndexScanNode(indexScan.id(), List.of(predicate));
            }
            default -> throw new IllegalStateException("Unexpected PhysicalNode: " + plan);
        };
    }
}
