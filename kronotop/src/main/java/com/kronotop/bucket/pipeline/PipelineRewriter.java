package com.kronotop.bucket.pipeline;

import com.kronotop.KronotopException;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.*;

import java.util.ArrayList;
import java.util.List;

public class PipelineRewriter {
    public static PipelineNode rewrite(PhysicalNode plan) {
        return switch (plan) {
            case PhysicalAnd physicalAnd -> {
                int indexScan = 0;
                int fullScan = 0;
                List<PipelineNode> children = new ArrayList<>();
                for (PhysicalNode child : physicalAnd.children()) {
                    if (child instanceof PhysicalFullScan) {
                        fullScan++;
                    } else if (child instanceof PhysicalIndexScan) {
                        indexScan++;
                    }
                    children.add(rewrite(child));
                }
                ExecutionStrategy strategy = fullScan == 0 ? ExecutionStrategy.INDEX_SCAN :
                        indexScan == 0 ? ExecutionStrategy.FULL_SCAN : ExecutionStrategy.MIXED_SCAN;
                if (strategy.equals(ExecutionStrategy.FULL_SCAN)) {
                    List<ResidualPredicate> predicates = new ArrayList<>();
                    for (PipelineNode child : children) {
                        if (child instanceof FullScanNode fullScanNode) {
                            predicates.addAll(fullScanNode.predicates());
                        } else {
                            throw new KronotopException("Fail...");
                        }
                    }
                    yield new FullScanNode(physicalAnd.id(), predicates);
                }
                yield new IntersectionNode(physicalAnd.id(), strategy, children);
            }
            case PhysicalIndexScan indexScan -> {
                PhysicalNode physicalNode = indexScan.node();
                if (!(physicalNode instanceof PhysicalFilter(
                        int id, String selector, Operator op, Object operand
                ))) {
                    throw new IllegalStateException("PhysicalNode must be a PhysicalFilter instance");
                }
                IndexScanPredicate predicate = new IndexScanPredicate(id, selector, op, operand);
                yield new IndexScanNode(indexScan.id(), indexScan.index(), predicate);
            }
            case PhysicalFullScan fullScan -> {
                PhysicalNode physicalNode = fullScan.node();
                if (!(physicalNode instanceof PhysicalFilter(
                        int id, String selector, Operator op, Object operand
                ))) {
                    throw new IllegalStateException("PhysicalNode must be a PhysicalFilter instance");
                }
                ResidualPredicate predicate = new ResidualPredicate(id, selector, op, operand);
                yield new FullScanNode(id, List.of(predicate));
            }
            case PhysicalRangeScan rangeScan -> {
                RangeScanPredicate predicate = new RangeScanPredicate(
                        rangeScan.selector(),
                        rangeScan.lowerBound(),
                        rangeScan.upperBound(),
                        rangeScan.includeLower(),
                        rangeScan.includeUpper()
                );
                yield new RangeScanNode(rangeScan.id(), rangeScan.index(), predicate);
            }
            case PhysicalFalse ignored -> null; // this query makes no sense.
            default -> throw new IllegalStateException("Unexpected PhysicalNode: " + plan);
        };
    }
}
