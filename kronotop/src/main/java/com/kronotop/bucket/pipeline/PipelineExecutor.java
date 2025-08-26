package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class PipelineExecutor {
    private final PipelineNode root;

    public PipelineExecutor(PipelineNode root) {
        this.root = root;
    }

    public Map<Versionstamp, ByteBuffer> execute(Transaction tr, PipelineContext ctx) {
        Map<Integer, DocumentLocation> locations = ctx.output().getLocations(root.id());
        if (locations == null || locations.size() < ctx.limit()) {
            executeNode(tr, ctx, root);
        }
        if (locations == null) {
            locations = ctx.output().getLocations(root.id());
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
            ctx.output().clear(root.id());
        }
        return results;
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
