package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.index.IndexDefinition;

import java.nio.ByteBuffer;

public class IndexScanNodeWithResidualPredicates extends IndexScanNode implements TransformationNode {
    private final ResidualPredicateNode residualPredicate;

    public IndexScanNodeWithResidualPredicates(int id, IndexDefinition index, IndexScanPredicate predicate, ResidualPredicateNode residualPredicate) {
        super(id, index, predicate);
        this.residualPredicate = residualPredicate;
    }

    @Override
    public void transform(QueryContext ctx) {
        DataSink sink = ctx.sinks().load(id());
        if (sink == null) {
            return;
        }

        ctx.sinks().remove(id());

        DataSink newSink = ctx.sinks().loadOrCreatePersistedEntrySink(id());
        try {
            switch (sink) {
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
