package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IntersectionNode extends AbstractLogicalNode implements LogicalNode{
    public IntersectionNode(int id, ExecutionStrategy strategy, List<PipelineNode> children) {
        super(id, strategy, children);
    }

    @Override
    public void execute(PipelineContext ctx, Transaction tr) {
        // Collect all child results once
        List<Map<Integer, DocumentLocation>> childLocations = collectChildLocations(ctx);
        
        // Compute intersection efficiently
        RoaringBitmap intersection = computeIntersection(childLocations);

        if (intersection.getCardinality() < ctx.limit()) {
            for (PipelineNode child : children()) {
                if (!(child instanceof TransactionAwareNode txAwareNode)) {
                    continue;
                }
                txAwareNode.execute(ctx, tr);
            }
            childLocations = collectChildLocations(ctx);
            intersection = computeIntersection(childLocations);
            System.out.println(intersection.getCardinality());
        }

        // Output results using first child's locations (they're all equivalent)
        outputResults(ctx, intersection, childLocations.getFirst());
    }

    private List<Map<Integer, DocumentLocation>> collectChildLocations(PipelineContext ctx) {
        List<Map<Integer, DocumentLocation>> locations = new ArrayList<>(children().size());
        for (PipelineNode child : children()) {
            Map<Integer, DocumentLocation> childLocs = ctx.output().getLocations(child.id());
            locations.add(childLocs != null ? childLocs : Collections.emptyMap());
        }
        return locations;
    }

    private RoaringBitmap computeIntersection(List<Map<Integer, DocumentLocation>> childLocations) {
        RoaringBitmap result = createBitmapFromKeys(childLocations.getFirst());
        
        for (int i = 1; i < childLocations.size(); i++) {
            RoaringBitmap childBitmap = createBitmapFromKeys(childLocations.get(i));
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

    private void outputResults(PipelineContext ctx, RoaringBitmap intersection, 
                              Map<Integer, DocumentLocation> sourceLocations) {
        intersection.forEach((int documentId) -> {
            DocumentLocation location = sourceLocations.get(documentId);
            ctx.output().appendLocation(id(), documentId, location);
        });
    }
}
