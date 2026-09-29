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
 * Rewrites a physical plan ({@link PhysicalNode} tree) into an executable pipeline plan
 * ({@link PipelineNode} tree). Predicates that cannot use an index become residual filters.
 * <p>
 * Each AND/OR node gets an {@link ExecutionStrategy} from its children:
 * <ul>
 *   <li>{@link ExecutionStrategy#INDEX_SCAN} - exactly one index scan</li>
 *   <li>{@link ExecutionStrategy#FULL_SCAN} - no index scan</li>
 *   <li>{@link ExecutionStrategy#MIXED_SCAN} - more than one index scan, or index scans together with full scans
 *       or elemMatch</li>
 *   <li>{@link ExecutionStrategy#NESTED} - at least one nested AND/OR child, for example from {@code $in}</li>
 * </ul>
 */
public class PipelineRewriter {

    /**
     * Picks the {@link ExecutionStrategy} for the children of an AND/OR node. A {@link PhysicalElemMatch}
     * with an indexed sub-plan counts as an index scan.
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
     * Rewrites each child into a pipeline node, keeping the order.
     */
    private static List<PipelineNode> rewriteChildren(PlannerContext ctx, PipelineContext pipelineCtx, List<PhysicalNode> children) {
        return children.stream().map((node) -> PipelineRewriter.rewrite(ctx, pipelineCtx, node)).toList();
    }

    /**
     * Picks the execution strategy for the children and rewrites them into pipeline nodes.
     */
    private static IntermediatePlan traverseChildren(PlannerContext ctx, PipelineContext pipelineCtx, List<PhysicalNode> children) {
        ExecutionStrategy strategy = determineStrategy(children);
        List<PipelineNode> rewritten = rewriteChildren(ctx, pipelineCtx, children);
        return new IntermediatePlan(strategy, rewritten);
    }

    /**
     * Converts each node into a residual predicate and joins them with a {@link ResidualAndNode}
     * or a {@link ResidualOrNode}, based on {@code strategy}.
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
     * Converts a full scan, index scan, range scan or union node into a residual predicate that
     * matches the same documents.
     *
     * @throws KronotopException if the node type is not supported
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
     * Converts a range scan predicate into an AND of a lower bound check (GT or GTE) and an upper
     * bound check (LT or LTE).
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
     * Rewrites an AND/OR node based on the execution strategy of its children.
     * <ul>
     *   <li>{@link ExecutionStrategy#FULL_SCAN} - one {@link FullScanNode} with the combined predicate</li>
     *   <li>{@link ExecutionStrategy#NESTED} - see {@link #convertNestedAndToIndexedPlan} and
     *       {@link #convertNestedOrToUnionNode}</li>
     *   <li>{@link ExecutionStrategy#MIXED_SCAN} and {@link ExecutionStrategy#INDEX_SCAN} - AND scans the most
     *       selective child and filters the rest; OR builds a {@link UnionNode}, or an {@link OrderedConcatNode}
     *       when every branch is an EQ index scan on the sortBy field</li>
     * </ul>
     *
     * @param id the ID of the resulting node when the strategy is {@link ExecutionStrategy#FULL_SCAN}
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
     * Builds a {@link UnionNode} for an OR with nested children. The children of a nested
     * {@link UnionNode} move up to the top level, and multiple {@link FullScanNode}s are merged
     * into one full scan with an OR predicate.
     * <p>
     * Example: {@code $or: [{role: {$in: [admin, editor]}}, {status: active}]} with an index on role becomes
     * {@code UnionNode([IndexScan(role=admin), IndexScan(role=editor), FullScan(status=active)])}.
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
     * Rewrites an AND with nested children. The most selective indexed child (union, index scan, range
     * scan or compound index scan) drives the scan, and the other conditions filter after it. A union
     * gets the filter on each child. Without an indexed child, it returns a {@link FullScanNode}.
     * <p>
     * Example: {@code $and: [{role: {$in: [admin, editor]}}, {status: active}]} with an index on role becomes
     * {@code UnionNode([IndexScan(role=admin) -> TransformWithResidualPredicate(status=active),
     * IndexScan(role=editor) -> TransformWithResidualPredicate(status=active)])}.
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

        // Select the primary indexed node by selectivity
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
     * Returns a copy of an index scan or range scan node with {@code nextNode} attached. Other node
     * types are returned unchanged, without {@code nextNode}.
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
        // Other node types are returned unchanged and nextNode is not attached
        return primaryNode;
    }

    /**
     * Returns true if {@code children} contains more than one {@link FullScanNode}.
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
     * Builds a {@link UnionNode} for an OR. Multiple {@link FullScanNode}s are merged into one full
     * scan with an OR predicate.
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
     * Scans the most selective child, picked by {@link SelectivityEstimator}, and filters with the
     * other children as a {@link TransformWithResidualPredicateNode}. If the chosen node already has
     * a residual filter, both filters are merged.
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
     * Returns a {@link FullScanNode} that filters with the predicates of all children, joined by
     * {@code strategy}.
     */
    private static FullScanNode convertToFullScanNode(PlannerContext ctx, int id, List<PipelineNode> children, PredicateEvalStrategy strategy) {
        ResidualPredicateNode predicate = transformToResidualPredicate(ctx, children, strategy);
        return new FullScanNode(id, getPrimaryIndexDefinition(ctx), predicate);
    }

    /**
     * Rewrites a physical plan into a pipeline plan without parameter binding. All operands are literals.
     *
     * @return the pipeline plan, or {@code null} if the query matches nothing
     */
    public static PipelineNode rewrite(PlannerContext ctx, PhysicalNode plan) {
        return rewrite(ctx, new PipelineContext(), plan);
    }

    /**
     * Rewrites a physical plan into a pipeline plan.
     * <p>
     * {@code $not} cannot use an index, so {@link PhysicalNot} becomes a full scan with a negated
     * residual predicate. {@link PhysicalTrue} scans the sortBy index when one exists.
     *
     * @param ctx         the planner context containing bucket metadata and ID generator
     * @param pipelineCtx the pipeline context for parameter binding
     * @param plan        the physical execution plan to rewrite
     * @return the pipeline plan, or {@code null} if the query matches nothing
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
                    SingleFieldIndex selectorIndex = ctx.getMetadata().singleFieldIndexes().getIndex(selector, IndexSelectionPolicy.READ);
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
            case PhysicalFalse ignored -> null; // matches nothing
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
     * Rewrites a filter that matches every document. If the sortBy field has an index, it returns a
     * full range scan on that index, so the results come in sort order. Otherwise, it returns a full
     * scan.
     */
    private static PipelineNode rewritePhysicalTrue(PlannerContext ctx, PhysicalTrue node) {
        String sortByField = ctx.getSortByField();
        if (sortByField != null && ctx.getMetadata() != null) {
            SingleFieldIndex index = ctx.getMetadata().singleFieldIndexes().getIndex(sortByField, IndexSelectionPolicy.READ);
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
     * Returns true if a {@link PhysicalElemMatch} sub-plan can use an index. An AND needs at least
     * one indexed child, and an OR needs all of its children indexed.
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
     * Returns true if the node is an index scan, a range scan, or a {@link UnionNode} whose children
     * all meet this condition.
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
     * Returns a copy of an index scan, range scan or union node with {@code elemMatchPredicate} as its
     * residual filter. A scan drops its old filter, because {@code elemMatchPredicate} already contains
     * all sub-plan conditions. Union children keep their own filters.
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
     * Returns the definition of the primary index.
     */
    private static SingleFieldIndexDefinition getPrimaryIndexDefinition(PlannerContext ctx) {
        SingleFieldIndex index = ctx.getMetadata().singleFieldIndexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        return index.definition();
    }

    /**
     * Wraps an operand from the physical plan into an {@link Operand}. It returns a parameter
     * reference if the node has a parameter binding, otherwise a literal. List operands, for
     * {@code $in}, {@code $nin} and {@code $all}, become list operands.
     *
     * @param occurrence the position of the operand in the node: 0, or 1 for the upper bound of a
     *                   range with both bounds
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
     * Converts a {@link BqlValue}, {@link Boolean} or {@link Integer} operand to a {@link BqlValue}.
     *
     * @throws IllegalArgumentException if the operand is null or has another type
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
 * Execution strategy and rewritten pipeline nodes for the children of an AND/OR node.
 */
record IntermediatePlan(ExecutionStrategy strategy, List<PipelineNode> children) {
}