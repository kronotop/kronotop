package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.kronotop.KronotopException;

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
        switch (node) {
            case ScanNode scanNode -> {
                ExecutionState state = ctx.getOrCreateExecutionState(scanNode.id());
                state.tryInitializingLimit(ctx.options().limit());
                scanNode.execute(ctx, tr);
            }
            case LogicalNode logicalNode -> {
                for (PipelineNode child : logicalNode.children()) {
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

    private void visitFullScanNode(Transaction tr, QueryContext ctx, PipelineNode node) {
        do {
            executePipelineNode(tr, ctx, node);
            ExecutionState state = ctx.getOrCreateExecutionState(node.id());
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

    private void visitIndexScanNode(Transaction tr, QueryContext ctx, PipelineNode node) {
        do {
            executePipelineNode(tr, ctx, node);
            ExecutionState state = ctx.getOrCreateExecutionState(node.id());
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

    private void visitIntersectionNode(Transaction tr, QueryContext ctx, PipelineNode node) {
        executePipelineNode(tr, ctx, node);
    }

    public void execute(Transaction tr, QueryContext ctx) {
        if (Objects.isNull(ctx.plan())) {
            // PhysicalFalse -> this query makes no sense
            return;
        }

        ctx.setEnvironment(env);
        switch (ctx.plan()) {
            case FullScanNode node -> visitFullScanNode(tr, ctx, node);
            case IndexScanNode node -> visitIndexScanNode(tr, ctx, node);
            case RangeScanNode node -> visitIndexScanNode(tr, ctx, node);
            case IntersectionNode node -> visitIntersectionNode(tr, ctx, node);
            default -> throw new KronotopException("Unknown PipelineNode type");
        }
    }
}
