/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.TypeBracketComparator;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.*;
import org.bson.BsonType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Transforms physical execution plans into optimized pipeline execution plans.
 * <p>
 * This class bridges the gap between the physical planner output ({@link PhysicalNode} tree)
 * and the executable pipeline representation ({@link PipelineNode} tree). The transformation
 * involves:
 * <ul>
 *   <li>Analyzing child nodes to determine the optimal {@link ExecutionStrategy}</li>
 *   <li>Converting index scans, full scans, and range scans to their pipeline equivalents</li>
 *   <li>Handling nested logical operators (AND/OR) with mixed index availability</li>
 *   <li>Consolidating multiple predicates into residual filters for post-retrieval evaluation</li>
 *   <li>Optimizing {@code $in} queries that were expanded to OR with multiple index scans</li>
 * </ul>
 * <p>
 * Execution strategies:
 * <ul>
 *   <li>{@link ExecutionStrategy#INDEX_SCAN} - Single index available, use it directly</li>
 *   <li>{@link ExecutionStrategy#FULL_SCAN} - No indexes, scan all documents</li>
 *   <li>{@link ExecutionStrategy#MIXED_SCAN} - Multiple indexes or mix of indexed/non-indexed</li>
 *   <li>{@link ExecutionStrategy#NESTED} - Contains nested AND/OR structures (e.g., from {@code $in})</li>
 * </ul>
 *
 * @see PhysicalNode
 * @see PipelineNode
 * @see ExecutionStrategy
 */
public class PipelineRewriter {

    /**
     * Analyzes child physical nodes to determine the appropriate execution strategy.
     * <p>
     * The strategy is determined by counting node types:
     * <ul>
     *   <li>If any child is {@link PhysicalAnd} or {@link PhysicalOr}, returns {@link ExecutionStrategy#NESTED}</li>
     *   <li>If no index scans exist, returns {@link ExecutionStrategy#FULL_SCAN}</li>
     *   <li>If exactly one index scan and no full scans, returns {@link ExecutionStrategy#INDEX_SCAN}</li>
     *   <li>Otherwise, returns {@link ExecutionStrategy#MIXED_SCAN} for later optimization</li>
     * </ul>
     *
     * @param children the list of child {@link PhysicalNode} instances to analyze
     * @return the determined {@link ExecutionStrategy} based on node composition
     */
    private static ExecutionStrategy determineStrategy(List<PhysicalNode> children) {
        int indexScan = 0;
        int fullScan = 0;
        int elemMatch = 0;
        boolean hasLogicalChildren = false;

        for (PhysicalNode child : children) {
            if (child instanceof PhysicalFullScan) {
                fullScan++;
            } else if (child instanceof PhysicalElemMatch physicalElemMatch) {
                // Check if the elemMatch has an indexed sub-plan
                if (hasIndexedSubPlan(physicalElemMatch.subPlan())) {
                    indexScan++;
                } else {
                    elemMatch++;
                }
            } else if (child instanceof PhysicalIndexScan || child instanceof PhysicalRangeScan
                    || child instanceof PhysicalIndexIntersection || child instanceof PhysicalCompoundIndexScan) {
                // PhysicalIndexIntersection is created by optimizer when multiple indexes are used
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

        // When we have index scans combined with elemMatch or full scans,
        // use MIXED_SCAN so the index is used and others become residual predicates
        if (indexScan >= 1 && (fullScan > 0 || elemMatch > 0)) {
            return ExecutionStrategy.MIXED_SCAN;
        }

        if (indexScan == 1 && fullScan == 0) {
            return ExecutionStrategy.INDEX_SCAN;
        }

        // More than one index, the later phases will pick
        // the most selective index and rewrite the pipeline
        return ExecutionStrategy.MIXED_SCAN;
    }

    /**
     * Recursively rewrites a list of physical nodes into their pipeline equivalents.
     * <p>
     * Each child node is transformed via {@link #rewrite(PlannerContext, PipelineContext, PhysicalNode)},
     * preserving the structure while converting to executable pipeline nodes.
     *
     * @param ctx         the planner context for ID generation and metadata access
     * @param pipelineCtx the pipeline context for parameter binding
     * @param children    the list of {@link PhysicalNode} instances to rewrite
     * @return a list of rewritten {@link PipelineNode} instances in the same order
     */
    private static List<PipelineNode> rewriteChildren(PlannerContext ctx, PipelineContext pipelineCtx, List<PhysicalNode> children) {
        return children.stream().map((node) -> PipelineRewriter.rewrite(ctx, pipelineCtx, node)).toList();
    }

    /**
     * Processes child physical nodes to determine execution strategy and rewrite to pipeline nodes.
     * <p>
     * This is the main orchestration method that:
     * <ol>
     *   <li>Calls {@link #determineStrategy(List)} to analyze node types</li>
     *   <li>Calls {@link #rewriteChildren(PlannerContext, PipelineContext, List)} to transform each node</li>
     *   <li>Returns both results bundled in an {@link IntermediatePlan}</li>
     * </ol>
     *
     * @param ctx         the planner context for ID generation and metadata access
     * @param pipelineCtx the pipeline context for parameter binding
     * @param children    the list of {@link PhysicalNode} representing child nodes in the physical plan
     * @return an {@link IntermediatePlan} containing the determined strategy and rewritten nodes
     */
    private static IntermediatePlan traverseChildren(PlannerContext ctx, PipelineContext pipelineCtx, List<PhysicalNode> children) {
        ExecutionStrategy strategy = determineStrategy(children);
        List<PipelineNode> rewritten = rewriteChildren(ctx, pipelineCtx, children);
        return new IntermediatePlan(strategy, rewritten);
    }

    /**
     * Transforms a list of pipeline nodes into a composite residual predicate.
     * <p>
     * This method converts pipeline nodes (which may include index scans, full scans, unions, etc.)
     * into residual predicates that can be evaluated against documents during post-retrieval filtering.
     * The predicates are combined using the specified strategy (AND or OR).
     *
     * @param ctx      the planner context for ID generation
     * @param children the list of {@link PipelineNode} instances to transform
     * @param strategy how to combine the predicates: {@link PredicateEvalStrategy#AND} produces
     *                 {@link ResidualAndNode}, {@link PredicateEvalStrategy#OR} produces {@link ResidualOrNode}
     * @return a composite {@link ResidualPredicateNode} combining all child predicates
     */
    private static ResidualPredicateNode transformToResidualPredicate(
            PlannerContext ctx,
            List<PipelineNode> children,
            PredicateEvalStrategy strategy) {

        List<ResidualPredicateNode> predicates = children.stream()
                .map(child -> transformNodeToResidualPredicate(ctx, child))
                .toList();

        return strategy == PredicateEvalStrategy.AND
                ? new ResidualAndNode(predicates)
                : new ResidualOrNode(predicates);
    }

    /**
     * Transforms a single pipeline node into its residual predicate equivalent.
     * <p>
     * Supports the following node types:
     * <ul>
     *   <li>{@link FullScanNode} - extracts the existing predicate directly</li>
     *   <li>{@link IndexScanNode} - converts {@link IndexScanPredicate} to {@link ResidualPredicate}</li>
     *   <li>{@link RangeScanNode} - converts to {@link ResidualAndNode} with lower/upper bound predicates</li>
     *   <li>{@link UnionNode} - recursively transforms children into {@link ResidualOrNode}</li>
     * </ul>
     *
     * @param ctx  the planner context for ID generation
     * @param node the {@link PipelineNode} to transform
     * @return the equivalent {@link ResidualPredicateNode}
     * @throws KronotopException if the node type is not supported for transformation
     */
    static ResidualPredicateNode transformNodeToResidualPredicate(PlannerContext ctx, PipelineNode node) {
        return switch (node) {
            case FullScanNode fullScanNode -> fullScanNode.predicate();
            case IndexScanNode indexScanNode -> {
                // If the transform's predicate is ResidualElemMatchNode, return it directly.
                // The ResidualElemMatchNode already contains the full predicate (including
                // the scan predicate) because it was created by createScanWithElemMatchPredicate.
                if (indexScanNode.next() instanceof TransformWithResidualPredicateNode transform &&
                        transform.predicate() instanceof ResidualElemMatchNode) {
                    yield transform.predicate();
                }

                // Convert the index scan predicate to residual
                IndexScanPredicate predicate = indexScanNode.predicate();
                ResidualPredicateNode scanPredicate = new ResidualPredicate(
                        predicate.id(), predicate.selector(), predicate.op(), predicate.operand(),
                        CollationResolver.resolve(ctx.getMetadata(), predicate.selector(), ctx.getCollation())
                );

                // If this node has a TransformWithResidualPredicateNode attached,
                // combine the scan predicate with the existing residual
                if (indexScanNode.next() instanceof TransformWithResidualPredicateNode transform) {
                    yield new ResidualAndNode(List.of(scanPredicate, transform.predicate()));
                }
                yield scanPredicate;
            }
            case RangeScanNode rangeScanNode -> {
                // If the transform's predicate is ResidualElemMatchNode, return it directly.
                if (rangeScanNode.next() instanceof TransformWithResidualPredicateNode transform &&
                        transform.predicate() instanceof ResidualElemMatchNode) {
                    yield transform.predicate();
                }

                // Convert the range scan predicate to residual
                ResidualPredicateNode scanPredicate = rangeScanPredicateToResidualAndNode(ctx, rangeScanNode.predicate());

                // If this node has a TransformWithResidualPredicateNode attached,
                // combine the scan predicate with the existing residual
                if (rangeScanNode.next() instanceof TransformWithResidualPredicateNode transform) {
                    yield new ResidualAndNode(List.of(scanPredicate, transform.predicate()));
                }
                yield scanPredicate;
            }
            case UnionNode unionNode -> {
                // If the transform's predicate is ResidualElemMatchNode, return it directly.
                if (unionNode.next() instanceof TransformWithResidualPredicateNode transform &&
                        transform.predicate() instanceof ResidualElemMatchNode) {
                    yield transform.predicate();
                }

                // Convert UnionNode children to ResidualOrNode
                List<ResidualPredicateNode> unionPredicates = unionNode.children().stream()
                        .map(child -> transformNodeToResidualPredicate(ctx, child))
                        .toList();
                yield new ResidualOrNode(unionPredicates);
            }
            default -> throw new KronotopException("Cannot transform " + node.getClass().getSimpleName()
                    + " to " + ResidualPredicateNode.class.getSimpleName());
        };
    }

    /**
     * Converts a range scan predicate into a residual AND node with lower and upper bound checks.
     * <p>
     * A range scan (e.g., {@code field >= 10 AND field <= 20}) is decomposed into two separate
     * comparison predicates combined with AND logic:
     * <ul>
     *   <li>Lower bound: GT or GTE depending on {@link RangeScanPredicate#includeLower()}</li>
     *   <li>Upper bound: LT or LTE depending on {@link RangeScanPredicate#includeUpper()}</li>
     * </ul>
     *
     * @param ctx       the planner context for ID generation
     * @param predicate the {@link RangeScanPredicate} containing range bounds and inclusion flags
     * @return a {@link ResidualAndNode} containing lower and upper bound predicates
     */
    private static ResidualPredicateNode rangeScanPredicateToResidualAndNode(PlannerContext ctx, RangeScanPredicate predicate) {
        List<ResidualPredicateNode> children = new ArrayList<>();

        Operator lowerBoundOp = Operator.GT;
        if (predicate.includeLower()) {
            lowerBoundOp = Operator.GTE;
        }
        children.add(new ResidualPredicate(ctx.nextId(), predicate.selector(), lowerBoundOp, predicate.lowerBound(),
                CollationResolver.resolve(ctx.getMetadata(), predicate.selector(), ctx.getCollation())));


        Operator upperBoundOp = Operator.LT;
        if (predicate.includeUpper()) {
            upperBoundOp = Operator.LTE;
        }
        children.add(new ResidualPredicate(ctx.nextId(), predicate.selector(), upperBoundOp, predicate.upperBound(),
                CollationResolver.resolve(ctx.getMetadata(), predicate.selector(), ctx.getCollation())));

        return new ResidualAndNode(children);
    }

    /**
     * Rewrites logical operators (AND/OR) into optimized pipeline nodes based on execution strategy.
     * <p>
     * This is the core method for handling compound queries. Based on the determined strategy:
     * <ul>
     *   <li>{@link ExecutionStrategy#FULL_SCAN} - converts to {@link FullScanNode} with combined predicates</li>
     *   <li>{@link ExecutionStrategy#NESTED} - for AND: uses indexed nodes with residual filters;
     *       for OR: creates {@link UnionNode} with flattened children</li>
     *   <li>{@link ExecutionStrategy#MIXED_SCAN} - for AND: picks the most selective index, others become residual;
     *       for OR: creates {@link UnionNode} consolidating full scans</li>
     *   <li>{@link ExecutionStrategy#INDEX_SCAN} - for AND: picks the most selective index, others become residual;
     *       for OR: creates {@link UnionNode}</li>
     * </ul>
     *
     * @param ctx               the planner context for ID generation and metadata access
     * @param pipelineCtx       the pipeline context for parameter binding
     * @param id                the node identifier for the resulting pipeline node
     * @param children          the list of child {@link PhysicalNode} instances
     * @param predicateStrategy {@link PredicateEvalStrategy#AND} or {@link PredicateEvalStrategy#OR}
     * @return the optimized {@link PipelineNode} for executing the logical operation
     */
    private static PipelineNode rewriteLogicalOperator(
            PlannerContext ctx,
            PipelineContext pipelineCtx,
            int id,
            List<PhysicalNode> children,
            PredicateEvalStrategy predicateStrategy) {

        IntermediatePlan intermediatePlan = traverseChildren(ctx, pipelineCtx, children);

        return switch (intermediatePlan.strategy()) {
            case FULL_SCAN -> convertToFullScanNode(ctx, id, intermediatePlan.children(), predicateStrategy);
            case NESTED -> {
                // For AND with nested OR (e.g., $in with index + other predicates),
                // use the indexed nodes and apply others as residual predicates
                if (PredicateEvalStrategy.AND.equals(predicateStrategy)) {
                    yield convertNestedAndToIndexedPlan(ctx, pipelineCtx, intermediatePlan.children());
                }
                // For OR with nested children, use UnionNode to combine all branches
                yield convertNestedOrToUnionNode(ctx, intermediatePlan.children());
            }
            case MIXED_SCAN, INDEX_SCAN -> {
                if (PredicateEvalStrategy.AND.equals(predicateStrategy)) {
                    yield convertToIndexScanNode(ctx, pipelineCtx, intermediatePlan.children(), predicateStrategy);
                }
                if (ctx.getSortByField() != null && isAllEqScansOnField(intermediatePlan.children(), ctx.getSortByField())) {
                    yield convertToOrderedConcatNode(ctx, pipelineCtx, intermediatePlan.children());
                }
                yield convertToUnionNode(ctx, intermediatePlan.children());
            }
        };
    }

    /**
     * Converts a nested OR plan to a {@link UnionNode}, optimizing the structure for execution.
     * <p>
     * This method handles OR queries that contain nested structures (e.g., from {@code $in} with index).
     * It performs two optimizations:
     * <ol>
     *   <li>Flattens nested {@link UnionNode} children - when {@code $in} is transformed to OR with
     *       multiple index scans, those scans are lifted to the top level</li>
     *   <li>Consolidates multiple {@link FullScanNode} instances into a single node with OR predicate,
     *       avoiding redundant full table scans</li>
     * </ol>
     * <p>
     * Example: {@code $or: [{role: {$in: [admin, editor]}}, {status: active}]} with index on role becomes:
     * {@code UnionNode([IndexScan(role=admin), IndexScan(role=editor), FullScan(status=active)])}
     *
     * @param ctx      the planner context for ID generation
     * @param children the list of rewritten {@link PipelineNode} instances from the OR branches
     * @return a {@link UnionNode} with flattened and consolidated children
     */
    private static PipelineNode convertNestedOrToUnionNode(PlannerContext ctx, List<PipelineNode> children) {
        List<PipelineNode> flattenedChildren = new ArrayList<>();
        List<PipelineNode> fullScanNodes = new ArrayList<>();

        for (PipelineNode child : children) {
            if (child instanceof UnionNode unionNode) {
                // Flatten nested UnionNode (from $in with index)
                flattenedChildren.addAll(unionNode.children());
            } else if (child instanceof FullScanNode) {
                fullScanNodes.add(child);
            } else {
                flattenedChildren.add(child);
            }
        }

        // Consolidate multiple FullScanNodes into one with OR predicate
        if (fullScanNodes.size() > 1) {
            ResidualPredicateNode predicate = transformToResidualPredicate(ctx, fullScanNodes, PredicateEvalStrategy.OR);
            flattenedChildren.add(new FullScanNode(ctx.nextId(), getPrimaryIndexDefinition(ctx), predicate));
        } else {
            flattenedChildren.addAll(fullScanNodes);
        }

        return new UnionNode(ctx.nextId(), flattenedChildren);
    }

    /**
     * Converts a nested AND plan to use indexes efficiently with residual predicate filtering.
     * <p>
     * This method optimizes AND queries containing nested structures (e.g., {@code $in} with index
     * combined with other predicates). The optimization strategy:
     * <ol>
     *   <li>Separates indexed nodes ({@link UnionNode}, {@link IndexScanNode},
     *       {@link RangeScanNode}) from non-indexed {@link FullScanNode} nodes</li>
     *   <li>If no indexed nodes exist, falls back to a full scan with combined AND predicates</li>
     *   <li>Selects the most selective indexed node using {@link SelectivityEstimator}</li>
     *   <li>Converts remaining nodes (both indexed and non-indexed) to residual predicates</li>
     *   <li>Chains the residual predicates via {@link TransformWithResidualPredicateNode} for post-filtering</li>
     * </ol>
     * <p>
     * Example: {@code $and: [{role: {$in: [admin, editor]}}, {status: active}]} with index on role becomes:
     * {@code UnionNode([IndexScan(role=admin), IndexScan(role=editor)]) -> TransformWithResidualPredicate(status=active)}
     *
     * @param ctx         the planner context for ID generation and metadata access
     * @param pipelineCtx the pipeline context for parameter binding
     * @param children    the list of rewritten {@link PipelineNode} instances from the AND branches
     * @return the primary indexed node with residual predicates chained, or a {@link FullScanNode} if no indexes
     */
    private static PipelineNode convertNestedAndToIndexedPlan(PlannerContext ctx, PipelineContext pipelineCtx, List<PipelineNode> children) {
        // Separate indexed nodes (UnionNode, IndexScanNode, RangeScanNode) from full scans
        List<PipelineNode> indexedNodes = new ArrayList<>();
        List<PipelineNode> residualNodes = new ArrayList<>();

        for (PipelineNode child : children) {
            if (child instanceof UnionNode ||
                    child instanceof IndexScanNode || child instanceof RangeScanNode ||
                    child instanceof CompoundIndexScanNode) {
                indexedNodes.add(child);
            } else {
                residualNodes.add(child);
            }
        }

        // If no indexed nodes, fall back to full scan
        if (indexedNodes.isEmpty()) {
            ResidualPredicateNode predicate = transformToResidualPredicate(ctx, children, PredicateEvalStrategy.AND);
            return new FullScanNode(ctx.nextId(), getPrimaryIndexDefinition(ctx), predicate);
        }

        // Select the primary indexed node (prefer UnionNode from $in, or use selectivity)
        PipelineNode primaryNode;
        List<PipelineNode> otherIndexedNodes;

        if (indexedNodes.size() == 1) {
            primaryNode = indexedNodes.getFirst();
            otherIndexedNodes = List.of();
        } else {
            // Use selectivity estimator to pick the best indexed node
            primaryNode = SelectivityEstimator.estimate(ctx, pipelineCtx, indexedNodes);
            otherIndexedNodes = indexedNodes.stream()
                    .filter(n -> n.id() != primaryNode.id())
                    .toList();
        }

        // Convert remaining indexed nodes and full scan nodes to residual predicates
        List<PipelineNode> allResidualSources = new ArrayList<>(otherIndexedNodes);
        allResidualSources.addAll(residualNodes);

        // Also include any existing residual predicates from the primary node's chain
        if (primaryNode.next() instanceof TransformWithResidualPredicateNode existingTransform) {
            allResidualSources.addFirst(new FullScanNode(ctx.nextId(), getPrimaryIndexDefinition(ctx), existingTransform.predicate()));
        }

        if (!allResidualSources.isEmpty()) {
            ResidualPredicateNode predicate = transformToResidualPredicate(ctx, allResidualSources, PredicateEvalStrategy.AND);
            if (primaryNode instanceof UnionNode unionNode) {
                // Push residual predicate into each child for early filtering.
                // Each child filters before the union collects, reducing dedup work
                // and enabling adaptive scan budgeting to grow per-child limits.
                for (PipelineNode child : unionNode.children()) {
                    TransformWithResidualPredicateNode childTransform =
                            new TransformWithResidualPredicateNode(ctx.nextId(), predicate);
                    child.connectNext(childTransform);
                }
            } else {
                TransformWithResidualPredicateNode nextNode = new TransformWithResidualPredicateNode(ctx.nextId(), predicate);
                if (primaryNode.next() == null) {
                    primaryNode.connectNext(nextNode);
                } else {
                    // Replace it by creating a new primary node with the merged predicate
                    // Since we can't modify the existing chain, wrap in a new structure
                    return createMergedIndexScanNode(ctx, primaryNode, nextNode);
                }
            }
        }

        return primaryNode;
    }

    /**
     * Creates a new IndexScanNode with merged residual predicates when the primary node already has a chain.
     * Clones the index scan predicate and connects the merged residual predicate.
     */
    private static PipelineNode createMergedIndexScanNode(PlannerContext ctx, PipelineNode primaryNode, TransformWithResidualPredicateNode nextNode) {
        if (primaryNode instanceof IndexScanNode indexScanNode) {
            IndexScanNode newNode = new IndexScanNode(ctx.nextId(), indexScanNode.getIndexDefinition(), indexScanNode.predicate());
            newNode.connectNext(nextNode);
            return newNode;
        } else if (primaryNode instanceof RangeScanNode rangeScanNode) {
            RangeScanNode newNode = new RangeScanNode(ctx.nextId(), rangeScanNode.getIndexDefinition(), rangeScanNode.predicate());
            newNode.connectNext(nextNode);
            return newNode;
        }
        // For UnionNode, just connect the next node to the first child that doesn't have one
        // This is a fallback - ideally, these cases should be handled differently
        return primaryNode;
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

    private static boolean isAllEqScansOnField(List<PipelineNode> children, String field) {
        for (PipelineNode child : children) {
            if (!(child instanceof IndexScanNode scanNode)) return false;
            if (!scanNode.getIndexDefinition().selector().equals(field)) return false;
            if (scanNode.predicate().op() != Operator.EQ) return false;
        }
        return !children.isEmpty();
    }

    private static PipelineNode convertToOrderedConcatNode(PlannerContext ctx, PipelineContext pipelineCtx, List<PipelineNode> children) {
        List<BqlValue> parameters = pipelineCtx.getParameters();
        List<PipelineNode> sorted = new ArrayList<>(children);
        sorted.sort((a, b) -> {
            BqlValue va = ((IndexScanNode) a).predicate().operand().resolve(parameters);
            BqlValue vb = ((IndexScanNode) b).predicate().operand().resolve(parameters);
            return TypeBracketComparator.INSTANCE.compare(
                    BSONUtil.bqlValueToBsonValue(va),
                    BSONUtil.bqlValueToBsonValue(vb));
        });
        return new OrderedConcatNode(ctx.nextId(), sorted);
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
        otherNodes.add(new FullScanNode(ctx.nextId(), getPrimaryIndexDefinition(ctx), predicate));
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
     * @return the most selective scan node with residual predicates connected as the next step
     */
    private static PipelineNode convertToIndexScanNode(PlannerContext ctx, PipelineContext pipelineCtx, List<PipelineNode> children, PredicateEvalStrategy predicateStrategy) {
        PipelineNode mostSelectiveIndexScan = SelectivityEstimator.estimate(ctx, pipelineCtx, children);

        List<PipelineNode> otherNodes = new ArrayList<>();
        for (PipelineNode node : children) {
            if (node.id() != mostSelectiveIndexScan.id()) {
                otherNodes.add(node);
            }
        }

        if (otherNodes.isEmpty()) {
            return mostSelectiveIndexScan;
        }

        ResidualPredicateNode newPredicate = transformToResidualPredicate(ctx, otherNodes, predicateStrategy);

        // If the primary node already has a TransformWithResidualPredicateNode, merge predicates
        if (mostSelectiveIndexScan.next() instanceof TransformWithResidualPredicateNode existingTransform) {
            ResidualPredicateNode merged = new ResidualAndNode(List.of(existingTransform.predicate(), newPredicate));
            // Create a new scan node with merged predicate (can't modify existing chain)
            TransformWithResidualPredicateNode mergedNode = new TransformWithResidualPredicateNode(ctx.nextId(), merged);
            return reconnectWithNewTransform(ctx, mostSelectiveIndexScan, mergedNode);
        }

        // No existing chain, connect directly
        TransformWithResidualPredicateNode nextNode = new TransformWithResidualPredicateNode(ctx.nextId(), newPredicate);
        mostSelectiveIndexScan.connectNext(nextNode);
        return mostSelectiveIndexScan;
    }

    private static PipelineNode reconnectWithNewTransform(PlannerContext ctx, PipelineNode scanNode, TransformWithResidualPredicateNode newTransform) {
        // Create a fresh copy of the scan node and connect the new transform
        if (scanNode instanceof IndexScanNode indexScan) {
            IndexScanNode newNode = new IndexScanNode(ctx.nextId(), indexScan.getIndexDefinition(), indexScan.predicate());
            newNode.connectNext(newTransform);
            return newNode;
        } else if (scanNode instanceof RangeScanNode rangeScan) {
            RangeScanNode newNode = new RangeScanNode(ctx.nextId(), rangeScan.getIndexDefinition(), rangeScan.predicate());
            newNode.connectNext(newTransform);
            return newNode;
        } else if (scanNode instanceof CompoundIndexScanNode compoundScan) {
            CompoundIndexScanNode newNode = new CompoundIndexScanNode(
                    ctx.nextId(), compoundScan.indexDefinition(), compoundScan.filters());
            newNode.connectNext(newTransform);
            return newNode;
        }
        // For other node types, just connect (this shouldn't happen in practice)
        scanNode.connectNext(newTransform);
        return scanNode;
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
        return new FullScanNode(id, getPrimaryIndexDefinition(ctx), predicate);
    }

    /**
     * Rewrites a physical execution plan into an optimized pipeline execution plan.
     * <p>
     * This is a convenience method that creates a default {@link PipelineContext} for non-parameterized execution.
     *
     * @param ctx  the planner context containing bucket metadata and ID generator
     * @param plan the physical execution plan to rewrite
     * @return the optimized {@link PipelineNode}, or {@code null} for unsatisfiable queries
     * @throws IllegalStateException if the physical node contains an invalid state or unsupported type
     */
    public static PipelineNode rewrite(PlannerContext ctx, PhysicalNode plan) {
        return rewrite(ctx, new PipelineContext(), plan);
    }

    /**
     * Rewrites a physical execution plan into an optimized pipeline execution plan.
     * <p>
     * This is the main entry point for plan transformation. It handles all physical node types:
     * <ul>
     *   <li>{@link PhysicalAnd} - rewritten via {@link #rewriteLogicalOperator} with AND strategy</li>
     *   <li>{@link PhysicalOr} - rewritten via {@link #rewriteLogicalOperator} with OR strategy</li>
     *   <li>{@link PhysicalIndexScan} - converted to {@link IndexScanNode}</li>
     *   <li>{@link PhysicalFullScan} - converted to {@link FullScanNode}</li>
     *   <li>{@link PhysicalRangeScan} - converted to {@link RangeScanNode}</li>
     *   <li>{@link PhysicalIndexIntersection} - converted to optimized index scan with residual predicates</li>
     *   <li>{@link PhysicalTrue} - converted to {@link FullScanNode} with {@link AlwaysTruePredicate}</li>
     *   <li>{@link PhysicalFalse} - returns {@code null} (query matches nothing)</li>
     * </ul>
     *
     * @param ctx         the planner context containing bucket metadata and ID generator
     * @param pipelineCtx the pipeline context for parameter binding
     * @param plan        the physical execution plan to rewrite
     * @return the optimized {@link PipelineNode}, or {@code null} for unsatisfiable queries
     * @throws IllegalStateException if the physical node contains an invalid state or unsupported type
     */
    public static PipelineNode rewrite(PlannerContext ctx, PipelineContext pipelineCtx, PhysicalNode plan) {
        assert ctx.getMetadata() != null : "Bucket metadata must be provided for query planning";
        return switch (plan) {
            case PhysicalAnd physicalAnd -> rewriteLogicalOperator(
                    ctx,
                    pipelineCtx,
                    physicalAnd.id(),
                    physicalAnd.children(),
                    PredicateEvalStrategy.AND
            );
            case PhysicalIndexScan indexScan -> {
                PhysicalNode physicalNode = indexScan.node();
                if (!(physicalNode instanceof PhysicalFilter(
                        int id, String selector, Operator op, Object operand
                ))) {
                    throw new IllegalStateException("PhysicalNode must be a PhysicalFilter instance");
                }
                Operand wrappedOperand = wrapOperand(pipelineCtx, id, 0, operand);
                IndexScanPredicate predicate = new IndexScanPredicate(id, selector, op, wrappedOperand);
                yield new IndexScanNode(indexScan.id(), indexScan.index(), predicate);
            }
            case PhysicalFullScan fullScan -> {
                PhysicalNode physicalNode = fullScan.node();
                if (!(physicalNode instanceof PhysicalFilter(
                        int id, String selector, Operator op, Object operand
                ))) {
                    throw new IllegalStateException("PhysicalNode must be a PhysicalFilter instance");
                }
                Operand wrappedOperand = wrapOperand(pipelineCtx, id, 0, operand);
                ResidualPredicate predicate = new ResidualPredicate(id, selector, op, wrappedOperand,
                        CollationResolver.resolve(ctx.getMetadata(), selector, ctx.getCollation()));
                FullScanNode fullScanNode = new FullScanNode(id, getPrimaryIndexDefinition(ctx), predicate);
                Collation queryCollation = ctx.getCollation();
                if (queryCollation != null) {
                    Index selectorIndex = ctx.getMetadata().indexes().getIndex(selector, IndexSelectionPolicy.READ);
                    if (selectorIndex != null
                            && selectorIndex.definition().bsonType() == BsonType.STRING
                            && !Objects.equals(queryCollation, selectorIndex.definition().collation())) {
                        fullScanNode.setCollationMismatch(true);
                        fullScanNode.setRejectedIndex(selectorIndex.definition().name());
                    }
                }
                yield fullScanNode;
            }
            case PhysicalRangeScan rangeScan -> {
                Operand lowerBound = rangeScan.lowerBound() != null
                        ? wrapOperand(pipelineCtx, rangeScan.id(), 0, rangeScan.lowerBound())
                        : null;
                Operand upperBound = rangeScan.upperBound() != null
                        ? wrapOperand(pipelineCtx, rangeScan.id(), rangeScan.lowerBound() != null ? 1 : 0, rangeScan.upperBound())
                        : null;
                RangeScanPredicate predicate = new RangeScanPredicate(
                        rangeScan.selector(),
                        lowerBound,
                        upperBound,
                        rangeScan.includeLower(),
                        rangeScan.includeUpper()
                );
                yield new RangeScanNode(rangeScan.id(), rangeScan.index(), predicate);
            }
            case PhysicalFalse ignored -> null; // this query makes no sense.
            case PhysicalOr physicalOr -> rewriteLogicalOperator(
                    ctx,
                    pipelineCtx,
                    physicalOr.id(),
                    physicalOr.children(),
                    PredicateEvalStrategy.OR
            );
            case PhysicalTrue catchAll -> rewritePhysicalTrue(ctx, catchAll);
            case PhysicalIndexIntersection intersection -> {
                List<PipelineNode> children = new ArrayList<>();
                for (int i = 0; i < intersection.filters().size(); i++) {
                    PhysicalFilter filter = intersection.filters().get(i);
                    Operand wrappedOperand = wrapOperand(pipelineCtx, filter.id(), 0, filter.operand());
                    IndexScanPredicate predicate = new IndexScanPredicate(
                            filter.id(), filter.selector(), filter.op(), wrappedOperand
                    );
                    children.add(new IndexScanNode(ctx.nextId(), intersection.indexes().get(i), predicate));
                }
                yield convertToIndexScanNode(ctx, pipelineCtx, children, PredicateEvalStrategy.AND);
            }
            case PhysicalCompoundIndexScan compoundScan -> {
                CompoundIndexDefinition definition = compoundScan.index();
                List<CompoundIndexScanNode.CompoundIndexScanFilter> scanFilters = new ArrayList<>();
                for (PhysicalFilter filter : compoundScan.filters()) {
                    Operand wrappedOperand = wrapOperand(pipelineCtx, filter.id(), 0, filter.operand());
                    BsonType bsonType = findCompoundFieldBsonType(definition, filter.selector());
                    scanFilters.add(new CompoundIndexScanNode.CompoundIndexScanFilter(
                            filter.selector(), filter.op(), wrappedOperand, bsonType));
                }
                yield new CompoundIndexScanNode(ctx.nextId(), definition, scanFilters);
            }
            case PhysicalElemMatch elemMatch -> {
                // Rewrite the subPlan to get a pipeline node, then convert to residual predicate
                PipelineNode subPlanNode = rewrite(ctx, pipelineCtx, elemMatch.subPlan());
                assert subPlanNode != null : "PhysicalElemMatch subPlan rewrite produced null for selector: " + elemMatch.selector();
                ResidualPredicateNode subPredicate = transformNodeToResidualPredicate(ctx, subPlanNode);
                ResidualElemMatchNode elemMatchPredicate = new ResidualElemMatchNode(elemMatch.selector(), subPredicate);

                // For scalar array $elemMatch with an indexed array field, use the index scan
                // as the primary access method and add elemMatch as a residual predicate.
                // Note: we create a fresh scan node with only elemMatchPredicate because
                // transformNodeToResidualPredicate already includes ALL conditions from subPlanNode
                // (both the scan predicate and any existing residual).
                if (usesSelectiveSecondaryIndex(subPlanNode)) {
                    yield createScanWithElemMatchPredicate(ctx, subPlanNode, elemMatchPredicate);
                }

                // Fallback: no index available, use full scan
                yield new FullScanNode(elemMatch.id(), getPrimaryIndexDefinition(ctx), elemMatchPredicate);
            }
            case PhysicalNot not -> {
                PipelineNode childPlan = rewrite(ctx, pipelineCtx, not.child());
                if (childPlan == null) {
                    // NOT(FALSE) = TRUE, return a full scan that matches everything
                    yield new FullScanNode(not.id(), getPrimaryIndexDefinition(ctx), new AlwaysTruePredicate());
                }
                // Convert child plan to residual predicate and negate it
                ResidualPredicateNode childPredicate = transformNodeToResidualPredicate(ctx, childPlan);
                ResidualNotNode notPredicate = new ResidualNotNode(childPredicate);
                // $not cannot use indexes directly, so always use a full scan
                yield new FullScanNode(not.id(), getPrimaryIndexDefinition(ctx), notPredicate);
            }
            default -> throw new IllegalStateException("Unexpected PhysicalNode: " + plan);
        };
    }

    private static BsonType findCompoundFieldBsonType(CompoundIndexDefinition definition, String selector) {
        for (CompoundIndexField field : definition.fields()) {
            if (field.selector().equals(selector)) {
                return field.bsonType();
            }
        }
        throw new IllegalStateException("Selector '" + selector + "' not found in compound index '" + definition.name() + "'");
    }

    /**
     * Rewrites a {@link PhysicalTrue} node, which represents an empty filter ({}).
     * If a sortByField is specified and an index exists for that field, creates a
     * {@link RangeScanNode} with a full range to leverage index ordering.
     * Otherwise, falls back to a {@link FullScanNode} with {@link AlwaysTruePredicate}.
     */
    private static PipelineNode rewritePhysicalTrue(PlannerContext ctx, PhysicalTrue node) {
        String sortByField = ctx.getSortByField();
        if (sortByField != null && ctx.getMetadata() != null) {
            Index index = ctx.getMetadata().indexes().getIndex(sortByField, IndexSelectionPolicy.READ);
            if (index != null) {
                // Create a full range scan on the sortBy index to preserve ordering
                RangeScanPredicate predicate = new RangeScanPredicate(
                        sortByField,
                        null,  // no lower bound
                        null,  // no upper bound
                        true,  // includeLower (doesn't matter when null)
                        true   // includeUpper (doesn't matter when null)
                );
                return new RangeScanNode(node.id(), index.definition(), predicate);
            }
        }
        return new FullScanNode(node.id(), getPrimaryIndexDefinition(ctx), new AlwaysTruePredicate());
    }

    /**
     * Checks if a physical node sub-plan uses an index.
     * Used by {@link #determineStrategy} to count {@link PhysicalElemMatch} nodes
     * with indexed sub-plans as index scans.
     */
    private static boolean hasIndexedSubPlan(PhysicalNode subPlan) {
        if (subPlan instanceof PhysicalIndexScan ||
                subPlan instanceof PhysicalRangeScan ||
                subPlan instanceof PhysicalIndexIntersection) {
            return true;
        }
        // Handle case where subPlan is PhysicalAnd containing indexed nodes
        // (e.g., $elemMatch with multiple conditions that got consolidated into a range scan)
        if (subPlan instanceof PhysicalAnd and) {
            return and.children().stream().anyMatch(PipelineRewriter::hasIndexedSubPlan);
        }
        // Handle case where subPlan is PhysicalOr containing indexed nodes
        // (e.g., $in operator transformed to OR with multiple index scans)
        if (subPlan instanceof PhysicalOr or) {
            return or.children().stream().allMatch(PipelineRewriter::hasIndexedSubPlan);
        }
        return false;
    }

    /**
     * Checks if the given pipeline node provides selective single field index access.
     * Includes {@link IndexScanNode}, {@link RangeScanNode}, and {@link UnionNode}
     * when all children use selective single field indexes (e.g., from $in operator).
     */
    private static boolean usesSelectiveSecondaryIndex(PipelineNode node) {
        if (node instanceof IndexScanNode || node instanceof RangeScanNode) {
            return true;
        }
        // UnionNode from $in operator - check if all children use selective indexes
        if (node instanceof UnionNode unionNode) {
            return unionNode.children().stream()
                    .allMatch(PipelineRewriter::usesSelectiveSecondaryIndex);
        }
        return false;
    }

    /**
     * Creates a new scan node (IndexScan, RangeScan, or UnionNode) with ONLY the elemMatch predicate.
     * This is used when rewriting PhysicalElemMatch with an indexed sub-plan.
     * Unlike attachResidualPredicate, this does NOT merge with existing residual predicates
     * because the elemMatchPredicate already contains all conditions from the sub-plan.
     */
    private static PipelineNode createScanWithElemMatchPredicate(
            PlannerContext ctx, PipelineNode subPlanNode, ResidualElemMatchNode elemMatchPredicate) {

        TransformWithResidualPredicateNode transform = new TransformWithResidualPredicateNode(ctx.nextId(), elemMatchPredicate);

        if (subPlanNode instanceof IndexScanNode indexScan) {
            IndexScanNode newNode = new IndexScanNode(ctx.nextId(), indexScan.getIndexDefinition(), indexScan.predicate());
            newNode.connectNext(transform);
            return newNode;
        } else if (subPlanNode instanceof RangeScanNode rangeScan) {
            RangeScanNode newNode = new RangeScanNode(ctx.nextId(), rangeScan.getIndexDefinition(), rangeScan.predicate());
            newNode.connectNext(transform);
            return newNode;
        } else if (subPlanNode instanceof UnionNode unionNode) {
            // For UnionNode (from $in operator), create a new UnionNode with the same children
            // and attach the elemMatch predicate to process results after union
            UnionNode newNode = new UnionNode(ctx.nextId(), unionNode.children());
            newNode.connectNext(transform);
            return newNode;
        }
        throw new IllegalStateException("createScanWithElemMatchPredicate called with unsupported node type: "
                + subPlanNode.getClass().getSimpleName());
    }

    /**
     * Retrieves the primary index definition from the bucket metadata.
     */
    private static SingleFieldIndexDefinition getPrimaryIndexDefinition(PlannerContext ctx) {
        Index index = ctx.getMetadata().indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        return index.definition();
    }

    /**
     * Wraps an operand value into the appropriate Operand type using PipelineContext.
     * Handles BqlValue, List (for $in/$nin/$all), Boolean (for $exists), and Integer.
     * In parameterized mode, creates Operand.Param; otherwise creates Operand.Literal.
     *
     * @param pipelineCtx the pipeline context for parameter binding
     * @param nodeId      the physical node ID
     * @param occurrence  the occurrence index (0 for single operands, 0/1 for range bounds)
     * @param operand     the operand value from the physical plan
     * @return the wrapped Operand instance
     */
    @SuppressWarnings("unchecked")
    private static Operand wrapOperand(PipelineContext pipelineCtx, int nodeId, int occurrence, Object operand) {
        // Handle list operands separately (for $in/$nin/$all operators)
        if (operand instanceof List<?> list) {
            return pipelineCtx.createListOperand(nodeId, (List<BqlValue>) list);
        }

        BqlValue literalValue = toBqlValue(operand);
        return pipelineCtx.createOperand(nodeId, occurrence, literalValue);
    }

    /**
     * Converts an operand value to a BqlValue.
     *
     * @param operand the operand value from the physical plan
     * @return the BqlValue representation
     */
    private static BqlValue toBqlValue(Object operand) {
        return switch (operand) {
            case BqlValue bqlValue -> bqlValue;
            case Boolean bool -> new com.kronotop.bucket.bql.ast.BooleanVal(bool);
            case Integer intVal -> new com.kronotop.bucket.bql.ast.Int32Val(intVal);
            case null -> throw new IllegalArgumentException("Cannot convert null operand");
            default ->
                    throw new IllegalArgumentException("Unsupported operand type: " + operand.getClass().getSimpleName());
        };
    }
}

/**
 * Intermediate result from analyzing and rewriting physical plan children.
 * <p>
 * Bundles together:
 * <ul>
 *   <li>The determined {@link ExecutionStrategy} based on child node types</li>
 *   <li>The list of rewritten {@link PipelineNode} instances</li>
 * </ul>
 * Used internally by PipelineRewriter#traverseChildren to pass both results
 * to the calling method for further processing.
 *
 * @param strategy the execution strategy determined from analyzing child nodes
 * @param children the list of pipeline nodes after rewriting from physical nodes
 */
record IntermediatePlan(ExecutionStrategy strategy, List<PipelineNode> children) {
}