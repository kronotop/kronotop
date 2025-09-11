package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.tuple.Versionstamp;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class UnionNode extends AbstractLogicalNode implements LogicalNode {
    public UnionNode(int id, List<PipelineNode> children) {
        super(id, children);
    }

    /**
     * Checks whether all child nodes are exhausted and propagates the exhaustion state
     * to the current node in the query context. If all child nodes are exhausted, the
     * current node's state is updated to exhaust.
     *
     * @param ctx the {@link QueryContext} instance that manages execution states and pipeline information
     * @return true if all child nodes are exhausted and the current node's state is set to exhaust,
     * false otherwise
     */
    private boolean checkAndPropagateExhaustion(QueryContext ctx) {
        for (PipelineNode child : children()) {
            ExecutionState childState = ctx.getOrCreateExecutionState(child.id());
            if (!childState.isExhausted()) {
                return false;
            }
        }
        ctx.getOrCreateExecutionState(id()).setExhausted(true);
        return true;
    }

    @Override
    public void execute(QueryContext ctx) {
        if (checkAndPropagateExhaustion(ctx)) {
            // All children are exhausted
            return;
        }

        Map<Integer, DocumentPointer> result = new LinkedHashMap<>();
        RoaringBitmap[] bitmaps = new RoaringBitmap[children().size()];

        for (int index = 0; index < children().size(); index++) {
            PipelineNode child = children().get(index);
            PipelineNode head = findHeadNode(child);
            DataSink sink = ctx.sinks().load(head.id());
            if (sink == null) {
                throw new IllegalStateException("No data sink found for " + child);
            }

            RoaringBitmap bitmap = new RoaringBitmap();
            switch (sink) {
                case PersistedEntrySink persistedEntrySink -> {
                    persistedEntrySink.forEach((versionstamp, persistedEntry) -> {
                        bitmap.add(persistedEntry.metadataId());
                        result.compute(persistedEntry.metadataId(), (ignored, documentPointer) -> {
                            if (documentPointer == null) {
                                DocumentPointer pointer = new DocumentPointer();
                                pointer.setVersionstamp(versionstamp);
                                pointer.setPersistedEntry(persistedEntry);
                                return pointer;
                            }
                            documentPointer.setVersionstamp(versionstamp);
                            documentPointer.setPersistedEntry(persistedEntry);
                            return documentPointer;
                        });
                    });
                }
                case DocumentLocationSink documentLocationSink ->
                        documentLocationSink.forEach((metadataId, location) -> {
                            bitmap.add(metadataId);
                            result.compute(metadataId, (ignored, documentPointer) -> {
                                if (documentPointer == null) {
                                    DocumentPointer pointer = new DocumentPointer();
                                    pointer.setLocation(location);
                                    return pointer;
                                }
                                documentPointer.setLocation(location);
                                return documentPointer;
                            });
                        });
            }

            bitmaps[index] = bitmap;
        }
        RoaringBitmap union = FastAggregation.or(bitmaps);

        // Collect the document bodies from the cluster
        DataSink sink = ctx.sinks().loadOrCreatePersistedEntrySink(id());
        for (Integer metadataId : union) {
            DocumentPointer documentPointer = result.get(metadataId);
            if (documentPointer.getPersistedEntry() != null) {
                ctx.sinks().writePersistedEntry(sink, documentPointer.getVersionstamp(), documentPointer.getPersistedEntry());
            } else {
                ByteBuffer document = ctx.env().documentRetriever().retrieveDocument(ctx.metadata(), documentPointer.getLocation());
                PersistedEntry entry = new PersistedEntry(documentPointer.getLocation().shardId(), documentPointer.getLocation().entryMetadata().id(), document);
                ctx.sinks().writePersistedEntry(sink, documentPointer.getLocation().versionstamp(), entry);
            }
        }

        // Cleanup
        for (PipelineNode child : children()) {
            PipelineNode head = findHeadNode(child);
            DataSink childSink = ctx.sinks().load(head.id());
            if (childSink == null) {
                continue;
            }
            childSink.clear();
        }
    }

    private static class DocumentPointer {
        private DocumentLocation location;
        private Versionstamp versionstamp;
        private PersistedEntry persistedEntry;

        public DocumentLocation getLocation() {
            return location;
        }

        public void setLocation(DocumentLocation location) {
            this.location = location;
        }

        public Versionstamp getVersionstamp() {
            return versionstamp;
        }

        public void setVersionstamp(Versionstamp versionstamp) {
            this.versionstamp = versionstamp;
        }

        public PersistedEntry getPersistedEntry() {
            return persistedEntry;
        }

        public void setPersistedEntry(PersistedEntry persistedEntry) {
            this.persistedEntry = persistedEntry;
        }
    }
}
