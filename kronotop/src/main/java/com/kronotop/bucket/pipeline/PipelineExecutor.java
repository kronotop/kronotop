package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

public class PipelineExecutor {
    private final PipelineNode root;

    public PipelineExecutor(PipelineNode root) {
        this.root = root;
    }

    public Map<Versionstamp, ByteBuffer> execute(Transaction tr, PipelineContext ctx) {
        executeNode(tr, ctx, root);

        Map<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();
        Map<Integer, DocumentLocation> locations = ctx.output().getLocations(root.id());
        if (locations != null) {
            for (DocumentLocation location : locations.values()) {
                ByteBuffer document = ctx.env().documentRetriever().retrieveDocument(ctx.getMetadata(), location);
                results.put(location.versionstamp(), document);
            }
            ctx.output().clear(root.id());
        }
        return results;
    }

    private void executeNode(Transaction tr, PipelineContext ctx, PipelineNode node) {
        switch (node) {
            case ScanNode scanNode -> {
                ExecutionState state = ctx.getOrCreateExecutionState(scanNode.id());
                state.tryInitializingLimit(ctx.limit());
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
