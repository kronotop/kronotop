package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.tuple.Versionstamp;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class IntersectionNode extends AbstractLogicalNode implements LogicalNode{
    public IntersectionNode(int id, ExecutionStrategy strategy, List<PipelineNode> children) {
        super(id, strategy, children);
    }

    @Override
    public void execute(PipelineContext ctx) {
        List<RoaringBitmap> sets = new ArrayList<>();
        for (PipelineNode node : children()) {
            RoaringBitmap roaring = new RoaringBitmap();
            Map<Integer, DocumentLocation> locations = ctx.output().getLocations(node.id());
            for (int documentId : locations.keySet()) {
                roaring.add(documentId);
            }
            sets.add(roaring);
        }
        RoaringBitmap root = sets.getFirst();
        for (int i = 1; i<sets.size();i++) {
            RoaringBitmap other = sets.get(i);
            root.and(other);
        }
        for (int documentid : root) {
            for (PipelineNode node : children()) {
                Map<Integer, DocumentLocation> l = ctx.output().getLocations(node.id());
                DocumentLocation location = l.get(documentid);
                ctx.output().appendLocation(id(), documentid, location);
            }
        }
        System.out.println("EXECUTING AND");
    }
}
