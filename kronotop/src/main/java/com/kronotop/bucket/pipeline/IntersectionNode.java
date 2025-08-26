package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import org.roaringbitmap.RoaringBitmap;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class IntersectionNode extends AbstractLogicalNode implements LogicalNode {
    public IntersectionNode(int id, ExecutionStrategy strategy, List<PipelineNode> children) {
        super(id, strategy, children);
    }

    @Override
    public void execute(PipelineContext ctx, Transaction tr) {
        while(true) {
            // Compute intersection efficiently
            RoaringBitmap intersection = computeIntersection(ctx);
            if (intersection == null) {
                return;
            }
            TreeMap<Versionstamp, DocumentLocation> results = new TreeMap<>();
            intersection.forEach((int entryMetadataId) -> {
                for (PipelineNode child : children()) {
                    DocumentLocation location = ctx.output().getLocations(child.id()).get(entryMetadataId);
                    results.put(location.versionstamp(), location);
                }
            });
            for (DocumentLocation location : results.values()) {
                ctx.output().appendLocation(id(), location.entryMetadata().id(), location);
            }
            if (ctx.output().getLocations(id()).size() < ctx.limit()) {
                boolean exhausted = false;
                for (PipelineNode child : children()) {
                    if (child instanceof TransactionAwareNode txAwareNode) {
                        txAwareNode.execute(ctx, tr);
                    }
                    ExecutionState state = ctx.getOrCreateExecutionState(child.id());
                    if (state.isExhausted()) {
                        exhausted = true;
                        break;
                    }
                }
                if (exhausted) {
                    break;
                }
            } else {
                break;
            }
        }

        for (PipelineNode child: children()) {
            ctx.output().clear(child.id());
        }
    }

    private RoaringBitmap computeIntersection(PipelineContext ctx) {
        // Initialize
        PipelineNode firstChild = children().getFirst();
        Map<Integer, DocumentLocation> locations = ctx.output().getLocations(firstChild.id());
        if (locations == null) {
            return null;
        }
        RoaringBitmap result = createBitmapFromKeys(locations);

        // Calculate the final intersection bitmap
        for (int i = 1; i < children().size(); i++) {
            PipelineNode child = children().get(i);
            Map<Integer, DocumentLocation> loc = ctx.output().getLocations(child.id());
            RoaringBitmap childBitmap = createBitmapFromKeys(loc);
            result.and(childBitmap);

            // Early exit if the intersection becomes empty
            if (result.isEmpty()) break;
        }

        return result;
    }

    private RoaringBitmap createBitmapFromKeys(Map<Integer, DocumentLocation> locations) {
        RoaringBitmap bitmap = new RoaringBitmap();
        int[] keys = locations.keySet().stream().mapToInt(Integer::intValue).toArray();
        bitmap.addN(keys, 0, keys.length);
        return bitmap;
    }
}