package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.tuple.Versionstamp;

public final class PersistedEntrySink extends AbstractDataSink<Versionstamp, PersistedEntry> implements DataSink {
    public PersistedEntrySink(int parentNodeId) {
        super(parentNodeId);
    }
}
