package com.kronotop.bucket.pipeline;

public final class DocumentLocationSink extends AbstractDataSink<Integer, DocumentLocation> implements DataSink {
    public DocumentLocationSink(int parentNodeId) {
        super(parentNodeId);
    }
}
