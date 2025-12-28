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

import com.apple.foundationdb.Transaction;
import com.kronotop.KronotopException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/*
CASE: Endurance's rotation is 67, 68 RPM.
Cooper: CASE, get ready to match our spin with the retro thrusters.
CASE: It's not possible.
Cooper: No. It's necessary.
 */

/**
 * Executes query plans by traversing the pipeline node tree.
 * <p>
 * PipelineExecutor is the runtime engine that interprets and executes {@link PipelineNode}
 * trees produced by the query planner. It handles three distinct node categories:
 * <ul>
 *   <li>{@link ScanNode}: Leaf nodes that fetch data from indexes or perform full scans</li>
 *   <li>{@link LogicalNode}: Composite nodes (Union, Intersection) that combine child results</li>
 *   <li>{@link TransformationNode}: Filter nodes that apply residual predicates</li>
 * </ul>
 * <p>
 * The executor implements batched execution to support LIMIT clauses efficiently.
 * Rather than fetching all matching documents, it iteratively requests batches until
 * the limit is satisfied or all sources are exhausted.
 *
 * @see PipelineNode for the node hierarchy
 * @see QueryContext for execution state management
 */
public class PipelineExecutor {
    private final PipelineEnv env;

    public PipelineExecutor(PipelineEnv env) {
        this.env = env;
    }

    /**
     * Recursively executes a pipeline node and its chain.
     * <p>
     * Dispatches execution based on node type:
     * <ul>
     *   <li>{@link ScanNode}: Executes the scan with the current transaction</li>
     *   <li>{@link LogicalNode}: Recursively executes non-exhausted children, then combines results</li>
     *   <li>{@link TransformationNode}: Applies transformation (e.g., residual predicate filtering)</li>
     * </ul>
     * After executing the current node, follows the {@code next} pointer to continue
     * the pipeline chain if present.
     *
     * @param tr   the FoundationDB transaction for data access
     * @param ctx  the query context tracking execution state
     * @param node the pipeline node to execute
     * @throws IllegalStateException if an unknown node type is encountered
     */
    private void executePipelineNode(Transaction tr, QueryContext ctx, PipelineNode node) {
        ctx.setCurrentNodeId(node.id());
        switch (node) {
            case ScanNode scanNode -> {
                scanNode.execute(ctx, tr);
            }
            case LogicalNode logicalNode -> {
                for (PipelineNode child : logicalNode.children()) {
                    ExecutionState state = ctx.getOrCreateExecutionState(child.id());
                    if (state.isExhausted()) {
                        continue;
                    }
                    executePipelineNode(tr, ctx, child);
                }
                logicalNode.execute(ctx);
            }
            case TransformationNode transformationNode -> {
                transformationNode.transform(ctx);
            }
            default -> throw new IllegalStateException("Unexpected PipelineNode: " + node);
        }

        PipelineNode next = node.next();
        if (next != null) {
            ctx.setRelation(next.id(), node.id());
            executePipelineNode(tr, ctx, next);
        }
    }

    /**
     * Executes a scan node with batched fetching to satisfy the requested limit.
     * <p>
     * Repeatedly executes the scan until either the limit is reached or the scan
     * is exhausted. After each batch, adjusts the remaining limit based on how
     * many documents were retrieved.
     *
     * @param tr       the FoundationDB transaction
     * @param ctx      the query context
     * @param scanNode the scan node to execute
     */
    private void visitScanNode(Transaction tr, QueryContext ctx, ScanNode scanNode) {
        ExecutionState state = ctx.getOrCreateExecutionState(scanNode.id());
        state.initializeLimit(ctx.options().limit());

        do {
            executePipelineNode(tr, ctx, scanNode);
            DataSink sink = ctx.sinks().load(ctx.currentNodeId());
            if (sink == null) {
                if (state.isExhausted()) {
                    break;
                }
                continue;
            }
            if (sink.size() < ctx.options().limit()) {
                if (state.isExhausted()) {
                    break;
                }
                state.setLimit(ctx.options().limit() - sink.size());
                continue; // fetch another batch
            }
            break;
        } while (true);
    }

    /**
     * Executes an intersection node.
     * <p>
     * Delegates to the standard pipeline execution. Intersection nodes handle
     * their own child coordination internally via {@link IntersectionNode#execute}.
     *
     * @param tr   the FoundationDB transaction
     * @param ctx  the query context
     * @param node the intersection node to execute
     */
    private void visitIntersectionNode(Transaction tr, QueryContext ctx, PipelineNode node) {
        executePipelineNode(tr, ctx, node);
    }

    /**
     * Distributes the parent's remaining limit across non-exhausted union children.
     * <p>
     * Divides the limit evenly among children that still have data. Any remainder
     * from integer division is assigned to the first child to ensure all requested
     * documents are fetched.
     *
     * @param ctx         the query context
     * @param parentState the union node's execution state containing the remaining limit
     * @param node        the union node whose children need limit assignment
     */
    private void setChildrenLimits(QueryContext ctx, ExecutionState parentState, UnionNode node) {
        List<PipelineNode> childrenStillHasData = new ArrayList<>();
        for (PipelineNode child : node.children()) {
            ExecutionState childState = ctx.getOrCreateExecutionState(child.id());
            if (!childState.isExhausted()) {
                childrenStillHasData.add(child);
            }
        }

        if (childrenStillHasData.isEmpty()) {
            // All children are exhausted
            return;
        }

        int reminder = parentState.getLimit() % childrenStillHasData.size();
        int childLimit = (parentState.getLimit() - reminder) / childrenStillHasData.size();

        for (int index = 0; index < childrenStillHasData.size(); index++) {
            PipelineNode child = childrenStillHasData.get(index);
            ExecutionState childState = ctx.getOrCreateExecutionState(child.id());
            if (index == 0) {
                childState.setLimit(childLimit + reminder);
            } else {
                childState.setLimit(childLimit);
            }
        }
    }

    /**
     * Executes a union node with batched fetching across all children.
     * <p>
     * Distributes the limit across children, executes them, and collects results.
     * Continues fetching in batches until the limit is satisfied or all children
     * are exhausted. Each iteration recalculates child limits based on remaining
     * quota and which children still have data.
     *
     * @param tr   the FoundationDB transaction
     * @param ctx  the query context
     * @param node the union node to execute
     */
    private void visitUnionNode(Transaction tr, QueryContext ctx, UnionNode node) {
        ExecutionState state = ctx.getOrCreateExecutionState(node.id());
        state.initializeLimit(ctx.options().limit());

        do {
            setChildrenLimits(ctx, state, node);
            executePipelineNode(tr, ctx, node);
            DataSink sink = ctx.sinks().load(node.id());
            if (sink == null) {
                if (state.isExhausted()) {
                    break;
                }
                continue;
            }
            if (sink.size() < ctx.options().limit()) {
                if (state.isExhausted()) {
                    break;
                }
                state.setLimit(ctx.options().limit() - sink.size());
                continue; // fetch another batch
            }
            break;
        } while (true);
    }

    /**
     * Executes the query plan stored in the context.
     * <p>
     * Entry point for query execution. Dispatches to the appropriate visitor
     * based on the root node type. A null plan (from PhysicalFalse) represents
     * a contradictory query and returns immediately with no results.
     *
     * @param tr  the FoundationDB transaction for all data access
     * @param ctx the query context containing the plan and execution options
     * @throws KronotopException if the root node type is not supported
     */
    public void execute(Transaction tr, QueryContext ctx) {
        if (Objects.isNull(ctx.plan())) {
            // PhysicalFalse -> this query makes no sense
            return;
        }

        ctx.setEnvironment(env);
        switch (ctx.plan()) {
            case ScanNode node -> visitScanNode(tr, ctx, node);
            case IntersectionNode node -> visitIntersectionNode(tr, ctx, node);
            case UnionNode node -> visitUnionNode(tr, ctx, node);
            default -> throw new KronotopException("Unknown PipelineNode type");
        }
    }
}
