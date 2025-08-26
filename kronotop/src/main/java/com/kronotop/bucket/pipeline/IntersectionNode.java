package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import org.roaringbitmap.RoaringBitmap;

import java.util.List;
import java.util.Map;

public class IntersectionNode extends AbstractLogicalNode implements LogicalNode {
    public IntersectionNode(int id, ExecutionStrategy strategy, List<PipelineNode> children) {
        super(id, strategy, children);
    }

    @Override
    public void execute(PipelineContext ctx, Transaction tr) {
        // Compute intersection efficiently
        RoaringBitmap intersection = computeIntersection(ctx);

        intersection.forEach((int entryMetadataId) -> {
            for (PipelineNode child : children()) {
                DocumentLocation location = ctx.output().getLocations(child.id()).get(entryMetadataId);
                ctx.output().appendLocation(id(), entryMetadataId, location);
            }
        });
    }

    private RoaringBitmap computeIntersection(PipelineContext ctx) {
        // Initialize
        PipelineNode firstChild = children().getFirst();
        Map<Integer, DocumentLocation> locations = ctx.output().getLocations(firstChild.id());
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

/*
if (intersection.getCardinality() < ctx.limit()) {
            for (PipelineNode child : children()) {
                if (!(child instanceof TransactionAwareNode txAwareNode)) {
                    continue;
                }
                txAwareNode.execute(ctx, tr);
            }
            childLocations = collectChildLocations(ctx);
            intersection = computeIntersection(childLocations);
        }
 */