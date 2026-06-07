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

import com.apple.foundationdb.Transaction;
import com.kronotop.KronotopException;
import com.kronotop.transaction.InstrumentedTransaction;

import java.time.Duration;
import java.util.*;

/*
CASE: Endurance's rotation is 67, 68 RPM.
Cooper: CASE, get ready to match our spin with the retro thrusters.
CASE: It's not possible.
Cooper: No. It's necessary.
 */

/**
 * Executes query plans by traversing the pipeline node tree.
 * <p>
 * Dispatches the root node to the appropriate visitor; then each visitor drives
 * execution through {@link #executePipelineNode} which recursively walks the
 * node chain (scan, logical, transformation, and any chained {@code next} nodes).
 * <p>
 * Scan and union nodes share an adaptive budgeting loop that decouples the
 * user-facing LIMIT from the FDB getRange limit. The budget starts at the user
 * limit and grows when residual filtering yields few results, bounded by a
 * per-transaction time budget. This reduces FDB round-trips for low-selectivity
 * queries while preventing runaway scans.
 *
 * @see PipelineNode for the node hierarchy
 * @see QueryContext for execution state management
 * @see ScanBudget for the adaptive budgeting algorithm
 */
public class PipelineExecutor {
    private static final long TX_TIME_BUDGET_NANOS = Duration.ofSeconds(4).toNanos();

    private final PipelineEnv env;

    public PipelineExecutor(PipelineEnv env) {
        this.env = env;
    }

    /**
     * Recursively executes a pipeline node and its chain.
     * <p>
     * Dispatches to the node's own execute/transform method, then follows the
     * {@code next} pointer to continue the chain. For logical nodes, recursively
     * executes non-exhausted children first.
     *
     * @param tr   the FoundationDB transaction for data access
     * @param ctx  the query context tracking execution state
     * @param node the pipeline node to execute
     * @throws IllegalStateException if an unknown node type is encountered
     */
    private void executePipelineNode(Transaction tr, QueryContext ctx, PipelineNode node) {
        ctx.setCurrentNodeId(node.id());
        switch (node) {
            case ScanNode scanNode -> scanNode.execute(ctx, tr);
            case LogicalNode logicalNode -> {
                // Save child checkpoints before execution for UnionNode rewind support
                if (logicalNode instanceof UnionNode unionNode) {
                    saveChildCheckpoints(ctx, unionNode);
                }

                if (logicalNode instanceof OrderedConcatNode concat) {
                    for (PipelineNode child : concat.getOrderedChildren(ctx)) {
                        ExecutionState state = ctx.getOrCreateExecutionState(child.id());
                        if (!state.isExhausted()) {
                            executePipelineNode(tr, ctx, child);
                            break;
                        }
                    }
                } else {
                    for (PipelineNode child : logicalNode.children()) {
                        ExecutionState state = ctx.getOrCreateExecutionState(child.id());
                        if (state.isExhausted()) {
                            continue;
                        }
                        executePipelineNode(tr, ctx, child);
                    }
                }
                logicalNode.execute(ctx);
            }
            case TransformationNode transformationNode -> transformationNode.transform(ctx);
            default -> throw new IllegalStateException("Unexpected PipelineNode: " + node);
        }

        PipelineNode next = node.next();
        if (next != null) {
            ctx.setRelation(next.id(), node.id());
            executePipelineNode(tr, ctx, next);
        }
    }

    /**
     * Shared adaptive budgeting loop used by scan and union visitors.
     * <p>
     * Iteratively executes the node in growing batches until the user limit is
     * satisfied, the data source is exhausted, or the time budget runs out.
     * Two optional hooks customize per-node-type behavior:
     * {@code beforeExecute} runs each iteration before execution (e.g., distributing
     * limits to union children), and {@code onLimitReached} runs when results meet
     * or exceed the user limit (e.g., trimming excess entries and rewinding cursors).
     *
     * @param tr             the FoundationDB transaction
     * @param ctx            the query context
     * @param node           the pipeline node to execute
     * @param beforeExecute  optional hook invoked each iteration before execution
     * @param onLimitReached optional hook invoked when results reach the user limit
     */
    private void executeWithAdaptiveBudget(
            Transaction tr, QueryContext ctx, PipelineNode node,
            Runnable beforeExecute, OnLimitReached onLimitReached) {
        ExecutionState state = ctx.getOrCreateExecutionState(node.id());
        int userLimit = ctx.options().limit();
        ScanBudget budget = tr == null
                ? ScanBudget.unbounded(userLimit)
                : new ScanBudget(userLimit, getTransactionDeadline(tr));
        int sinkNodeId = getLastNodeId(node);

        do {
            state.setLimit(Math.max(1, budget.current()));
            if (beforeExecute != null) {
                beforeExecute.run();
            }

            int sinkSizeBefore = getSinkSize(ctx, sinkNodeId);

            executePipelineNode(tr, ctx, node);

            DataSink sink = ctx.sinks().load(sinkNodeId);
            int sinkSizeAfter = sink != null ? sink.size() : 0;

            budget.recordScanned(state.isExhausted()
                    ? (sinkSizeAfter - sinkSizeBefore)
                    : budget.current());

            if (sink == null || sinkSizeAfter == 0) {
                if (state.isExhausted() || budget.anyBudgetExhausted()) {
                    break;
                }
                budget.grow();
                continue;
            }

            if (sinkSizeAfter >= userLimit) {
                if (onLimitReached != null) {
                    onLimitReached.apply(sink, userLimit);
                }
                break;
            }

            if (state.isExhausted() || budget.anyBudgetExhausted()) {
                break;
            }
            budget.grow();
        } while (true);
    }

    private void trimAndRewindCursor(QueryContext ctx, ScanNode scanNode, DataSink sink, int limit) {
        DocumentRef lastKept = sink.entries().get(limit - 1);
        sink.trimTo(limit);

        ExecutionState scanState = ctx.getOrCreateExecutionState(scanNode.id());
        ctx.env().cursorManager().rewindCursor(ctx, scanNode.id(), lastKept, scanState.isScanReversed());
    }

    private int getLastNodeId(PipelineNode node) {
        PipelineNode current = node;
        while (current.next() != null) {
            current = current.next();
        }
        return current.id();
    }

    private long getTransactionDeadline(Transaction tr) {
        if (tr == null) {
            return 0;
        }
        if (tr instanceof InstrumentedTransaction itx) {
            return itx.getMetrics().getStartNanos() + TX_TIME_BUDGET_NANOS;
        }
        throw new IllegalArgumentException("Transaction must be an InstrumentedTransaction");
    }

    private int getSinkSize(QueryContext ctx, int nodeId) {
        DataSink sink = ctx.sinks().load(nodeId);
        return sink != null ? sink.size() : 0;
    }

    /**
     * Distributes the parent's remaining limit evenly across non-exhausted union children.
     * Any remainder from the integer division is assigned to the first child.
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

        int limit = parentState.getLimit();
        int childCount = childrenStillHasData.size();
        int remainder = limit % childCount;

        // Ensure childLimit is at least 1 to avoid FDB's "limit=0 means unlimited" behavior
        int childLimit = Math.max(1, (limit - remainder) / childCount);

        for (int index = 0; index < childCount; index++) {
            PipelineNode child = childrenStillHasData.get(index);
            ExecutionState childState = ctx.getOrCreateExecutionState(child.id());
            if (index == 0) {
                childState.setLimit(childLimit + remainder);
            } else {
                childState.setLimit(childLimit);
            }
        }
    }

    private void saveChildCheckpoints(QueryContext ctx, UnionNode unionNode) {
        ExecutionState unionState = ctx.getOrCreateExecutionState(unionNode.id());
        Map<Integer, ExecutionState.SavedCursorCheckpoint> checkpoints = new HashMap<>();

        for (PipelineNode child : unionNode.children()) {
            ExecutionState childState = ctx.getOrCreateExecutionState(child.id());
            checkpoints.put(child.id(), new ExecutionState.SavedCursorCheckpoint(
                    childState.getLower(),
                    childState.getUpper(),
                    childState.getSelector()
            ));
        }
        unionState.setSavedChildCheckpoints(checkpoints);
    }

    private void visitScanNode(Transaction tr, QueryContext ctx, ScanNode scanNode) {
        executeWithAdaptiveBudget(tr, ctx, scanNode, null,
                (sink, limit) -> {
                    if (sink.size() > limit) {
                        trimAndRewindCursor(ctx, scanNode, sink, limit);
                    }
                });
    }

    private void visitOrderedConcatNode(Transaction tr, QueryContext ctx, OrderedConcatNode node) {
        ExecutionState state = ctx.getOrCreateExecutionState(node.id());
        int[] activeChildId = {-1};

        executeWithAdaptiveBudget(tr, ctx, node,
                () -> {
                    ScanNode active = findActiveScanChild(ctx, node);
                    if (active != null) {
                        activeChildId[0] = active.id();
                        ExecutionState childState = ctx.getOrCreateExecutionState(active.id());
                        childState.setLimit(state.getLimit());
                    }
                },
                (sink, limit) -> {
                    if (sink.size() > limit && activeChildId[0] >= 0) {
                        ScanNode activeChild = (ScanNode) findChildById(node, activeChildId[0]);
                        if (activeChild != null) {
                            trimAndRewindCursor(ctx, activeChild, sink, limit);
                        }
                    }
                });
    }

    private ScanNode findActiveScanChild(QueryContext ctx, OrderedConcatNode node) {
        for (PipelineNode child : node.getOrderedChildren(ctx)) {
            ExecutionState childState = ctx.getOrCreateExecutionState(child.id());
            if (!childState.isExhausted() && child instanceof ScanNode scanNode) {
                return scanNode;
            }
        }
        return null;
    }

    private PipelineNode findChildById(OrderedConcatNode node, int id) {
        for (PipelineNode child : node.children()) {
            if (child.id() == id) {
                return child;
            }
        }
        return null;
    }

    private void visitUnionNode(Transaction tr, QueryContext ctx, UnionNode node) {
        ExecutionState state = ctx.getOrCreateExecutionState(node.id());
        executeWithAdaptiveBudget(tr, ctx, node,
                () -> setChildrenLimits(ctx, state, node), null);
    }

    /**
     * Entry point for query execution. Dispatches to the appropriate visitor
     * based on the root node type. A null plan (from PhysicalFalse) produces
     * no results.
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
            case UnionNode node -> visitUnionNode(tr, ctx, node);
            case OrderedConcatNode node -> visitOrderedConcatNode(tr, ctx, node);
            default -> throw new KronotopException("Unknown PipelineNode type");
        }
    }

    @FunctionalInterface
    private interface OnLimitReached {
        void apply(DataSink sink, int limit);
    }
}
