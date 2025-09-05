package com.kronotop.bucket.pipeline;

import java.util.function.Function;

public sealed interface DataSink permits PersistedEntrySink, DocumentLocationSink {
    int parentNodeId();

    int size();

    void clear();

    default <R> R match(
            Function<PersistedEntrySink, R> onBuffer,
            Function<DocumentLocationSink, R> onDocLoc
    ) {
        return switch (this) {
            case PersistedEntrySink b -> onBuffer.apply(b);
            case DocumentLocationSink d -> onDocLoc.apply(d);
        };
    }
}
