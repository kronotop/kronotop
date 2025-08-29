package com.kronotop.bucket.pipeline;

import com.kronotop.KronotopException;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The PipelineRewriter class is responsible for transforming a physical execution plan,
 * represented as a set of {@link PhysicalNode} instances, into an optimized logical
 * execution plan, represented as a set of {@link PipelineNode} instances. This includes
 * analyzing the execution strategy, consolidating predicates, and creating specific pipeline nodes
 * for execution.
 * <p>
 * It supports operations such as:
 * - Traversing child {@link PhysicalNode} objects to determine the overall execution strategy
 * ({@link ExecutionStrategy}).
 * - Converting groups of nodes to consolidated {@link FullScanNode} or specific node types.
 * - Rewriting complex physical plans ({@link PhysicalAnd}, {@link PhysicalOr}, etc.) into their
 * corresponding optimized pipeline representations.
 */
public class PipelineRewriter {

    /**
     * Traverses through a list of {@link PhysicalNode} children and processes each child
     * to determine the appropriate {@link ExecutionStrategy} and generate a list of rewritten
     * {@link PipelineNode} instances.
     * <p>
     * The method analyzes the types of {@link PhysicalNode} (e.g., {@link PhysicalFullScan}
     * or {@link PhysicalIndexScan}) to establish whether the execution strategy should
     * be {@link ExecutionStrategy#INDEX_SCAN}, {@link ExecutionStrategy#FULL_SCAN}, or
     * {@link ExecutionStrategy#MIXED_SCAN}. It then rewrites each child node into a corresponding
     * {@link PipelineNode}.
     *
     * @param children the list of {@link PhysicalNode} representing the children of the current node
     *                 in the physical execution plan
     * @return a {@link SubPlan} object containing the determined {@link ExecutionStrategy} and
     * the rewritten list of {@link PipelineNode} instances
     */
    private static SubPlan traverseChildren(List<PhysicalNode> children) {
        int indexScan = 0;
        int fullScan = 0;
        boolean nested = false;
        List<PipelineNode> traversedChildren = new ArrayList<>();
        for (PhysicalNode child : children) {
            if (child instanceof PhysicalFullScan) {
                fullScan++;
            } else if (child instanceof PhysicalIndexScan) {
                indexScan++;
            } else {
                nested = true;
            }
            traversedChildren.add(rewrite(child));
        }
        ExecutionStrategy strategy = nested ? ExecutionStrategy.NESTED :
                fullScan == 0 ? ExecutionStrategy.INDEX_SCAN :
                indexScan == 0 ? ExecutionStrategy.FULL_SCAN : ExecutionStrategy.MIXED_SCAN;
        return new SubPlan(strategy, traversedChildren);
    }

    private static FullScanNode convertToFullScanNode(int id, SubPlan subplan, PredicateEvalStrategy strategy) {
        List<ResidualPredicateNode> children = new ArrayList<>();
        for (PipelineNode child : subplan.children()) {
            if (child instanceof FullScanNode fullScanNode) {
                children.add(fullScanNode.predicate());
            } else {
                throw new KronotopException("Child plan must be a FullScanNode instance but " + child.getClass().getSimpleName());
            }
        }
        ResidualPredicateNode predicate;
        if (Objects.equals(strategy, PredicateEvalStrategy.AND)) {
            predicate = new ResidualAndNode(children);
        } else if (Objects.equals(strategy, PredicateEvalStrategy.OR)) {
            predicate = new ResidualOrNode(children);
        } else {
            throw new IllegalArgumentException("Unknown predicate rule");
        }
        return new FullScanNode(id, predicate);
    }

    /**
     * Rewrites a given {@link PhysicalNode} into an optimized {@link PipelineNode} representation
     * based on the provided execution strategy and constraints.
     *
     * @param plan the input physical execution plan, represented as a {@link PhysicalNode}
     * @return the optimized logical pipeline node, or {@code null} for invalid queries
     * @throws IllegalStateException if a {@link PhysicalNode} is encountered with an unexpected or invalid state
     */
    public static PipelineNode rewrite(PhysicalNode plan) {
        return switch (plan) {
            case PhysicalAnd physicalAnd -> {
                SubPlan subplan = traverseChildren(physicalAnd.children());
                if (subplan.strategy().equals(ExecutionStrategy.FULL_SCAN)) {
                    yield convertToFullScanNode(physicalAnd.id(), subplan, PredicateEvalStrategy.AND);
                } else if (subplan.strategy().equals(ExecutionStrategy.NESTED)) {
                    yield convertToFullScanNode(physicalAnd.id(), subplan, PredicateEvalStrategy.AND);
                }
                yield new IntersectionNode(physicalAnd.id(), subplan.strategy(), subplan.children());
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
                yield new FullScanNode(id, predicate);
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
            case PhysicalOr physicalOr -> {
                SubPlan subplan = traverseChildren(physicalOr.children());
                if (subplan.strategy().equals(ExecutionStrategy.FULL_SCAN)) {
                    yield convertToFullScanNode(physicalOr.id(), subplan, PredicateEvalStrategy.OR);
                } else if (subplan.strategy().equals(ExecutionStrategy.NESTED)) {
                    yield convertToFullScanNode(physicalOr.id(), subplan, PredicateEvalStrategy.OR);
                }
                yield new UnionNode(physicalOr.id(), subplan.strategy(), subplan.children());
            }
            default -> throw new IllegalStateException("Unexpected PhysicalNode: " + plan);
        };
    }
}

record SubPlan(ExecutionStrategy strategy, List<PipelineNode> children) {
}