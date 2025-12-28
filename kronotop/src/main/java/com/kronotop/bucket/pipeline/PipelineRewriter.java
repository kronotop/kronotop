/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
            } else if (child instanceof PhysicalIndexScan || child instanceof PhysicalRangeScan) {
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

    private static ResidualPredicateNode transformToResidualPredicate(
            PlannerContext ctx,
            List<PipelineNode> children,
            PredicateEvalStrategy strategy) {

        List<ResidualPredicateNode> predicates = children.stream()
                .map(child -> {
                    switch (child) {
                        case FullScanNode fullScanNode -> {
                            return fullScanNode.predicate();
                        }
                        case IndexScanNode indexScanNode -> {
                            IndexScanPredicate predicate = indexScanNode.predicate();
                            return new ResidualPredicate(predicate.id(), predicate.selector(), predicate.op(), predicate.operand());
                        }
                        case RangeScanNode rangeScanNode -> {
                            return rangeScanPredicateToResidualAndNode(ctx, rangeScanNode.predicate());
                        }
                        default -> throw new KronotopException("Cannot transform " + child.getClass().getSimpleName()
                                + " to " + ResidualPredicateNode.class.getSimpleName());
                    }
                })
                .toList();

        return strategy == PredicateEvalStrategy.AND
                ? new ResidualAndNode(predicates)
                : new ResidualOrNode(predicates);
    }

    private static ResidualPredicateNode rangeScanPredicateToResidualAndNode(PlannerContext ctx, RangeScanPredicate predicate) {
        List<ResidualPredicateNode> children = new ArrayList<>();

        Operator lowerBoundOp = Operator.GT;
        if (predicate.includeLower()) {
            lowerBoundOp = Operator.GTE;
        }
        children.add(new ResidualPredicate(ctx.nextId(), predicate.selector(), lowerBoundOp, predicate.lowerBound()));


        Operator upperBoundOp = Operator.LT;
        if (predicate.includeUpper()) {
            upperBoundOp = Operator.LTE;
        }
        children.add(new ResidualPredicate(ctx.nextId(), predicate.selector(), upperBoundOp, predicate.upperBound()));

        return new ResidualAndNode(children);
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
            case FULL_SCAN, NESTED -> convertToFullScanNode(ctx, id, intermediatePlan.children(), predicateStrategy);
            case MIXED_SCAN -> {
                if (PredicateEvalStrategy.AND.equals(predicateStrategy)) {
                    yield convertToIndexScanNode(ctx, intermediatePlan.children(), predicateStrategy);
                }
                yield convertToUnionNode(ctx, intermediatePlan.children());
            }
            default -> nodeFactory.create(id, intermediatePlan.children());
        };
    }

    /**
     * Determines if the given list of {@link PipelineNode} instances contains more than one node
     * of type {@link FullScanNode}.
     *
     * @param children the list of {@link PipelineNode} instances to analyze
     * @return {@code true} if there are more than one {@link FullScanNode} in the list,
     * otherwise {@code false}
     */
    private static boolean hasManyFullScanNodes(List<PipelineNode> children) {
        int numberOfFullScans = 0;
        for (PipelineNode child : children) {
            if (child instanceof FullScanNode) {
                numberOfFullScans++;
                if (numberOfFullScans > 1) {
                    break;
                }
            }
        }
        return numberOfFullScans > 1;
    }

    /**
     * Transforms a list of child {@link PipelineNode} instances into a {@link UnionNode}.
     * If the child nodes include multiple {@link FullScanNode} instances,
     * they are consolidated into a single {@link FullScanNode} containing a residual predicate.
     *
     * @param ctx      the {@link PlannerContext} providing context information and utilities
     * @param children the list of child {@link PipelineNode} instances to be processed
     * @return a {@link UnionNode} containing the transformed child nodes
     */
    private static PipelineNode convertToUnionNode(PlannerContext ctx, List<PipelineNode> children) {
        if (!hasManyFullScanNodes(children)) {
            return new UnionNode(ctx.nextId(), children);
        }

        List<PipelineNode> otherNodes = new ArrayList<>();
        List<PipelineNode> fullScanNodes = new ArrayList<>();
        for (PipelineNode child : children) {
            if (child instanceof FullScanNode) {
                fullScanNodes.add(child);
            } else {
                otherNodes.add(child);
            }
        }
        ResidualPredicateNode predicate = transformToResidualPredicate(ctx, fullScanNodes, PredicateEvalStrategy.OR);
        otherNodes.add(new FullScanNode(ctx.nextId(), predicate));
        return new UnionNode(ctx.nextId(), otherNodes);
    }

    /**
     * Optimizes a mixed scan scenario by selecting the most selective index and converting
     * remaining predicates to residual filters.
     * <p>
     * Uses {@link SelectivityEstimator} to identify which index will produce the smallest
     * result set, then chains the other predicates as a {@link TransformWithResidualPredicateNode}
     * for post-retrieval filtering.
     *
     * @param ctx               the planner context for ID generation and metadata access
     * @param children          the list of scan nodes to optimize
     * @param predicateStrategy how to combine residual predicates (AND/OR)
     * @return the most selective scan node with residual predicates connected as next step
     */
    private static PipelineNode convertToIndexScanNode(PlannerContext ctx, List<PipelineNode> children, PredicateEvalStrategy predicateStrategy) {
        PipelineNode mostSelectiveIndexScan = SelectivityEstimator.estimate(ctx, children);

        List<PipelineNode> otherNodes = new ArrayList<>();
        for (PipelineNode node : children) {
            if (node.id() != mostSelectiveIndexScan.id()) {
                otherNodes.add(node);
            }
        }

        ResidualPredicateNode predicate = transformToResidualPredicate(ctx, otherNodes, predicateStrategy);
        TransformWithResidualPredicateNode nextNode = new TransformWithResidualPredicateNode(ctx.nextId(), predicate);
        mostSelectiveIndexScan.connectNext(nextNode);
        return mostSelectiveIndexScan;
    }

    /**
     * Falls back to a full table scan when no suitable indexes are available.
     * <p>
     * Combines all child predicates into a single composite residual predicate
     * that filters documents during the sequential scan.
     *
     * @param ctx      the planner context for ID generation
     * @param id       the node identifier for the resulting full scan node
     * @param children the list of child nodes whose predicates will be combined
     * @param strategy how to combine predicates (AND/OR)
     * @return a full scan node with the combined residual predicate
     */
    private static FullScanNode convertToFullScanNode(PlannerContext ctx, int id, List<PipelineNode> children, PredicateEvalStrategy strategy) {
        ResidualPredicateNode predicate = transformToResidualPredicate(ctx, children, strategy);
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
        assert ctx.getMetadata() != null : "Bucket metadata must be provided for query planning";
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
            case PhysicalTrue catchAll -> new FullScanNode(catchAll.id(), new AlwaysTruePredicate());
            case PhysicalIndexIntersection intersection -> {
                List<PipelineNode> children = new ArrayList<>();
                for (int i = 0; i < intersection.filters().size(); i++) {
                    PhysicalFilter filter = intersection.filters().get(i);
                    IndexScanPredicate predicate = new IndexScanPredicate(
                            filter.id(), filter.selector(), filter.op(), filter.operand()
                    );
                    children.add(new IndexScanNode(ctx.nextId(), intersection.indexes().get(i), predicate));
                }
                yield convertToIndexScanNode(ctx, children, PredicateEvalStrategy.AND);
            }
            default -> throw new IllegalStateException("Unexpected PhysicalNode: " + plan);
        };
    }

    @FunctionalInterface
    private interface NodeFactory {
        PipelineNode create(int id, List<PipelineNode> children);
    }
}

record IntermediatePlan(ExecutionStrategy strategy, List<PipelineNode> children) {
}