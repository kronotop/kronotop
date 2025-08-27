package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class PipelineExecutor {
    private final PipelineNode root;

    public PipelineExecutor(PipelineNode root) {
        this.root = root;
    }

    private Map<Versionstamp, ByteBuffer> executeAndFetchDocuments(Transaction tr, PipelineContext ctx, PipelineNode node) {
        Map<Integer, DocumentLocation> locations = ctx.output().getLocations(node.id());
        if (locations == null || locations.size() < ctx.limit()) {
            executeNode(tr, ctx, node);
        }
        if (locations == null) {
            locations = ctx.output().getLocations(node.id());
        }

        Map<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();
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
        if (results.isEmpty()) {
            ctx.output().clearLocations(node.id());
        }
        return results;
    }

    private Map<Versionstamp, ByteBuffer> executeFullScanNode(Transaction tr, PipelineContext ctx, PipelineNode node) {
        Map<Versionstamp, ByteBuffer> documents = ctx.output().getDocuments(node.id());

        if (documents == null || documents.isEmpty()) {
            executeNode(tr, ctx, node);
        }

        documents = ctx.output().getDocuments(node.id());

        Map<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();
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
        ExecutionState state = ctx.getOrCreateExecutionState(node.id());
        System.out.println(">>> " + state.isExhausted() + " " +  documents.size() + " " + results.size());
        if (results.isEmpty()) {
            ctx.output().removeDocuments(node.id());
        }
        return results;
    }

    public Map<Versionstamp, ByteBuffer> execute(Transaction tr, PipelineContext ctx) {
        return switch (root) {
            case FullScanNode node -> executeFullScanNode(tr, ctx, node);
            default -> executeAndFetchDocuments(tr, ctx, root);
        };
    }

    private void executeNode(Transaction tr, PipelineContext ctx, PipelineNode node) {
        switch (node) {
            case ScanNode scanNode -> {
                ExecutionState state = ctx.getOrCreateExecutionState(scanNode.id());
                state.tryInitializingLimit(Math.max(ctx.limit(), PipelineContext.DEFAULT_LIMIT));
                scanNode.execute(ctx, tr);
            }
            case LogicalNode logicalNode -> {
                for (PipelineNode child : logicalNode.children()) {
                    executeNode(tr, ctx, child);
                }
                logicalNode.execute(ctx, tr);
            }
            default -> throw new IllegalStateException("Unexpected PipelineNode: " + node);
        }
    }
}
