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

public class PipelineExecutor {
    private final PipelineEnv env;

    public PipelineExecutor(PipelineEnv env) {
        this.env = env;
    }

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

    private void visitIntersectionNode(Transaction tr, QueryContext ctx, PipelineNode node) {
        executePipelineNode(tr, ctx, node);
    }

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
