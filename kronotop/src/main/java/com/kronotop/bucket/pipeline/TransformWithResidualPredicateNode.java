package com.kronotop.bucket.pipeline;

import java.nio.ByteBuffer;

public class TransformWithResidualPredicateNode extends AbstractPipelineNode implements TransformationNode {
    private final ResidualPredicateNode residualPredicate;

    public TransformWithResidualPredicateNode(int id, ResidualPredicateNode residualPredicate) {
        super(id);
        this.residualPredicate = residualPredicate;
    }

    @Override
    public void transform(QueryContext ctx) {
        int parentId = ctx.getParentId(id());

        DataSink sink = ctx.sinks().load(parentId);
        if (sink == null) {
            return;
        }

        DataSink newSink = ctx.sinks().loadOrCreatePersistedEntrySink(id());
        try {
            switch (sink) {
                case PersistedEntrySink persistedEntrySink -> {
                    persistedEntrySink.forEach(((versionstamp, entry) -> {
                        if (residualPredicate.test(entry.document())) {
                            ctx.sinks().writePersistedEntry(newSink, versionstamp, entry);
                        }
                    }));
                }
                case DocumentLocationSink documentLocationSink -> {
                    documentLocationSink.forEach((entryMetadataId, location) -> {
                        ByteBuffer document = ctx.env().documentRetriever().retrieveDocument(ctx.metadata(), location);
                        if (residualPredicate.test(document)) {
                            PersistedEntry entry = new PersistedEntry(location.shardId(), document);
                            ctx.sinks().writePersistedEntry(newSink, location.versionstamp(), entry);
                        }
                    });
                }
                default -> throw new IllegalStateException("Unexpected value: " + sink);
            }
        } finally {
            sink.clear();
        }
    }
}
