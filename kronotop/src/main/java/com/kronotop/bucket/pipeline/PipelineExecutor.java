package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.KronotopException;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
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
                logicalNode.execute(ctx, tr);
            }
            default -> throw new IllegalStateException("Unexpected PipelineNode: " + node);
        }
    }

    private Map<Versionstamp, ByteBuffer> visitFullScanNode(Transaction tr, QueryContext ctx, PipelineNode node) {
        try {
            do {
                executePipelineNode(tr, ctx, node);
                ExecutionState state = ctx.getOrCreateExecutionState(node.id());
                Map<Versionstamp, ByteBuffer> documents = ctx.output().getDocuments(node.id());
                if (documents == null) {
                    if (state.isExhausted()) {
                        return Map.of();
                    }
                    continue;
                }
                if (documents.size() < ctx.options().limit()) {
                    if (state.isExhausted()) {
                        return documents;
                    }
                    state.setLimit(ctx.options().limit() - documents.size());
                    continue; // fetch another batch
                }
                return documents;
            } while (true);
        } finally {
            ctx.output().removeDocuments(node.id());
        }
    }

    private Map<Versionstamp, ByteBuffer> loadDocumentsFromVolume(QueryContext ctx, Map<Integer, DocumentLocation> locations) {
        Map<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();
        for (DocumentLocation location : locations.values()) {
            ByteBuffer document = ctx.env().documentRetriever().retrieveDocument(ctx.metadata(), location);
            results.put(location.versionstamp(), document);
        }
        return results;
    }

    private Map<Versionstamp, ByteBuffer> visitIndexScanNode(Transaction tr, QueryContext ctx, PipelineNode node) {
        try {
            do {
                executePipelineNode(tr, ctx, node);
                ExecutionState state = ctx.getOrCreateExecutionState(node.id());
                Map<Integer, DocumentLocation> locations = ctx.output().getLocations(node.id());
                if (locations == null) {
                    if (state.isExhausted()) {
                        return Map.of();
                    }
                    continue;
                }
                if (locations.size() < ctx.options().limit()) {
                    if (state.isExhausted()) {
                        return loadDocumentsFromVolume(ctx, locations);
                    }
                    state.setLimit(ctx.options().limit() - locations.size());
                    continue; // fetch another batch
                }
                return loadDocumentsFromVolume(ctx, locations);
            } while (true);
        } finally {
            ctx.output().clearLocations(node.id());
        }
    }

    private Map<Versionstamp, ByteBuffer> visitIntersectionNode(Transaction tr, QueryContext ctx, PipelineNode node) {
        executePipelineNode(tr, ctx, node);
        return Map.of();
    }

    public Map<Versionstamp, ByteBuffer> execute(Transaction tr, QueryContext ctx) {
        if (Objects.isNull(ctx.plan())) {
            // PhysicalFalse -> this query makes no sense
            return Map.of();
        }
        ctx.setEnvironment(env);
        return switch (ctx.plan()) {
            case FullScanNode node -> visitFullScanNode(tr, ctx, node);
            case IndexScanNode node -> visitIndexScanNode(tr, ctx, node);
            case RangeScanNode node -> visitIndexScanNode(tr, ctx, node);
            case IntersectionNode node -> visitIntersectionNode(tr, ctx, node);
            default -> throw new KronotopException("Unknown PipelineNode type");
        };
    }
}
