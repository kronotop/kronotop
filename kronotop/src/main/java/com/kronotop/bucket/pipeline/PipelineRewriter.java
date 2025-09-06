package com.kronotop.bucket.pipeline;

import com.kronotop.KronotopException;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.*;

import java.util.ArrayList;
import java.util.List;

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
     * Determines the execution strategy based on the types of child nodes.
     *
     * @param children the list of {@link PhysicalNode} representing the children
     * @return the appropriate {@link ExecutionStrategy}
     */
    private static ExecutionStrategy determineStrategy(List<PhysicalNode> children) {
        int indexScan = 0;
        int fullScan = 0;
        boolean hasLogicalChildren = false;

        for (PhysicalNode child : children) {
            if (child instanceof PhysicalFullScan) {
                fullScan++;
            } else if (child instanceof PhysicalIndexScan) {
                indexScan++;
            } else if (child instanceof PhysicalAnd || child instanceof PhysicalOr) {
                hasLogicalChildren = true;
            }
        }

        if (hasLogicalChildren) {
            return ExecutionStrategy.NESTED;
        }

        if (indexScan == 0) {
            return ExecutionStrategy.FULL_SCAN;
        }

        if (indexScan == 1 && fullScan == 0) {
            return ExecutionStrategy.INDEX_SCAN;
        }

        // More than one index, the later phases will pick
        // the most selective index and rewrite the pipeline
        return ExecutionStrategy.MIXED_SCAN;
    }

    /**
     * Rewrites a list of physical nodes into pipeline nodes.
     *
     * @param children the list of {@link PhysicalNode} to rewrite
     * @return the list of rewritten {@link PipelineNode} instances
     */
    private static List<PipelineNode> rewriteChildren(PlannerContext ctx, List<PhysicalNode> children) {
        return children.stream().map((node) -> PipelineRewriter.rewrite(ctx, node)).toList();
    }

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
     * @return a {@link IntermediatePlan} object containing the determined {@link ExecutionStrategy} and
     * the rewritten list of {@link PipelineNode} instances
     */
    private static IntermediatePlan traverseChildren(PlannerContext ctx, List<PhysicalNode> children) {
        ExecutionStrategy strategy = determineStrategy(children);
        List<PipelineNode> rewritten = rewriteChildren(ctx, children);
        return new IntermediatePlan(strategy, rewritten);
    }

    /**
     * Combines residual predicates from multiple FullScanNode children into a single predicate.
     *
     * @param children the list of pipeline nodes (must be FullScanNode instances)
     * @param strategy the strategy for combining predicates (AND or OR)
     * @return the combined residual predicate node
     */
    private static ResidualPredicateNode combineResiduals(
            List<PipelineNode> children,
            PredicateEvalStrategy strategy) {

        List<ResidualPredicateNode> predicates = children.stream()
                .map(child -> {
                    if (!(child instanceof FullScanNode fullScanNode)) {
                        throw new KronotopException("Child plan must be a FullScanNode instance but " + child.getClass().getSimpleName());
                    }
                    return fullScanNode.predicate();
                })
                .toList();

        return strategy == PredicateEvalStrategy.AND
                ? new ResidualAndNode(predicates)
                : new ResidualOrNode(predicates);
    }

    /**
     * Rewrites logical operators (AND/OR) into appropriate pipeline nodes based on execution strategy.
     *
     * @param id                the node identifier
     * @param children          the list of child physical nodes
     * @param predicateStrategy the predicate evaluation strategy (AND or OR)
     * @param nodeFactory       factory function to create intersection/union nodes
     * @return the rewritten pipeline node
     */
    private static PipelineNode rewriteLogicalOperator(
            PlannerContext ctx,
            int id,
            List<PhysicalNode> children,
            PredicateEvalStrategy predicateStrategy,
            NodeFactory nodeFactory) {

        IntermediatePlan intermediatePlan = traverseChildren(ctx, children);

        return switch (intermediatePlan.strategy()) {
            case FULL_SCAN, NESTED -> convertToFullScanNode(id, intermediatePlan, predicateStrategy);
            case MIXED_SCAN -> optimizeIndexScanWithResidual(ctx, intermediatePlan, predicateStrategy);
            default -> nodeFactory.create(id, intermediatePlan.strategy(), intermediatePlan.children());
        };
    }

    private static PipelineNode optimizeIndexScanWithResidual(PlannerContext ctx, IntermediatePlan intermediatePlan, PredicateEvalStrategy predicateStrategy) {
        PipelineNode mostSelectiveIndexScan = selectMostSelectiveIndexScan(intermediatePlan.children());

        List<PipelineNode> otherNodes = new ArrayList<>();
        for (PipelineNode node : intermediatePlan.children()) {
            if (node.id() != mostSelectiveIndexScan.id()) {
                otherNodes.add(node);
            }
        }

        ResidualPredicateNode predicate = combineResiduals(otherNodes, predicateStrategy);
        TransformWithResidualPredicateNode nextNode = new TransformWithResidualPredicateNode(ctx.nextId(), predicate);
        mostSelectiveIndexScan.connectNext(nextNode);
        return mostSelectiveIndexScan;
    }

    private static PipelineNode selectMostSelectiveIndexScan(List<PipelineNode> children) {
        for (PipelineNode node : children) {
            if (node instanceof IndexScanNode) {
                return node;
            }
        }
        throw new IllegalStateException("No IndexScanNode found");
    }

    private static FullScanNode convertToFullScanNode(int id, IntermediatePlan subplan, PredicateEvalStrategy strategy) {
        ResidualPredicateNode predicate = combineResiduals(subplan.children(), strategy);
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
    public static PipelineNode rewrite(PlannerContext ctx, PhysicalNode plan) {
        return switch (plan) {
            case PhysicalAnd physicalAnd -> rewriteLogicalOperator(
                    ctx,
                    physicalAnd.id(),
                    physicalAnd.children(),
                    PredicateEvalStrategy.AND,
                    IntersectionNode::new
            );
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
            case PhysicalOr physicalOr -> rewriteLogicalOperator(
                    ctx,
                    physicalOr.id(),
                    physicalOr.children(),
                    PredicateEvalStrategy.OR,
                    UnionNode::new
            );
            default -> throw new IllegalStateException("Unexpected PhysicalNode: " + plan);
        };
    }

    @FunctionalInterface
    private interface NodeFactory {
        PipelineNode create(int id, ExecutionStrategy strategy, List<PipelineNode> children);
    }
}

record IntermediatePlan(ExecutionStrategy strategy, List<PipelineNode> children) {
}