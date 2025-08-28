package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.KronotopException;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class PipelineExecutor {
    private final PipelineNode root;

    public PipelineExecutor(PipelineNode root) {
        this.root = root;
    }

    private void executePipelineNode(Transaction tr, PipelineContext ctx, PipelineNode node) {
        switch (node) {
            case ScanNode scanNode -> {
                ExecutionState state = ctx.getOrCreateExecutionState(scanNode.id());
                state.tryInitializingLimit(Math.max(ctx.limit(), PipelineContext.DEFAULT_LIMIT));
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

    private Map<Versionstamp, ByteBuffer> visitFullScanNode(Transaction tr, PipelineContext ctx, PipelineNode node) {
        Map<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();
        ExecutionState state = ctx.getOrCreateExecutionState(node.id());

        Map<Versionstamp, ByteBuffer> documents = ctx.output().getDocuments(node.id());
        if (documents == null || documents.isEmpty()) {
            if (state.isExhausted()) {
                return results;
            }
            executePipelineNode(tr, ctx, node);
        }

        if (documents == null) {
            documents = ctx.output().getDocuments(node.id());
        }

        if (documents == null) {
            return results;
        }

        int counter = 0;
        for (Iterator<Map.Entry<Versionstamp, ByteBuffer>> iterator = documents.entrySet().iterator(); iterator.hasNext(); ) {
            if (counter >= ctx.limit()) {
                break;
            }
            Map.Entry<Versionstamp, ByteBuffer> entry = iterator.next();
            results.put(entry.getKey(), entry.getValue());
            counter++;
            iterator.remove();
        }
        if (documents.isEmpty() && state.isExhausted()) {
            ctx.output().removeDocuments(node.id());
        }
        return results;
    }

    private Map<Versionstamp, ByteBuffer> visitIndexScanNode(Transaction tr, PipelineContext ctx, PipelineNode node) {
        Map<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();
        ExecutionState state = ctx.getOrCreateExecutionState(node.id());
        Map<Integer, DocumentLocation> locations = ctx.output().getLocations(node.id());
        if (locations == null || locations.isEmpty()) {
            if (state.isExhausted()) {
                return results;
            }
            executePipelineNode(tr, ctx, node);
        }
        if (locations == null) {
            locations = ctx.output().getLocations(node.id());
        }

        if (locations == null) {
            return results;
        }

        // We must have some batched results
        int counter = 0;
        for (Iterator<DocumentLocation> iterator = locations.values().iterator(); iterator.hasNext(); ) {
            if (counter >= ctx.limit()) {
                break;
            }
            DocumentLocation location = iterator.next();
            ByteBuffer document = ctx.env().documentRetriever().retrieveDocument(ctx.getMetadata(), location);
            results.put(location.versionstamp(), document);
            counter++;
            iterator.remove();
        }
        if (results.isEmpty() && state.isExhausted()) {
            ctx.output().clearLocations(node.id());
        }
        return results;
    }

    public Map<Versionstamp, ByteBuffer> execute(Transaction tr, PipelineContext ctx) {
        if (Objects.isNull(root)) {
            // PhysicalFalse -> this query makes no sense
            return Map.of();
        }
        return switch (root) {
            case FullScanNode node -> visitFullScanNode(tr, ctx, node);
            case IndexScanNode node -> visitIndexScanNode(tr, ctx, node);
            case RangeScanNode node -> visitIndexScanNode(tr, ctx, node);
            default -> throw new KronotopException("Unknown PipelineNode type");
        };
    }
}
