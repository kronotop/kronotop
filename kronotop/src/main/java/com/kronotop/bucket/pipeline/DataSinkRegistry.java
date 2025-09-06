package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.tuple.Versionstamp;

import java.util.concurrent.ConcurrentHashMap;

public class DataSinkRegistry {
    private final ConcurrentHashMap<Integer, DataSink> sinks = new ConcurrentHashMap<>();

    public DataSink load(int parentNodeId) {
        return sinks.get(parentNodeId);
    }

    public void remove(int parentNodeId) {
        sinks.remove(parentNodeId);
    }

    public DataSink loadOrCreateDocumentLocationSink(int parentNodeId) {
        return sinks.computeIfAbsent(parentNodeId, (ignored) -> new DocumentLocationSink(parentNodeId));
    }

    public DataSink loadOrCreatePersistedEntrySink(int parentNodeId) {
        return sinks.computeIfAbsent(parentNodeId, (ignored) -> new PersistedEntrySink(parentNodeId));
    }

    void writePersistedEntry(DataSink sink, Versionstamp versionstamp, PersistedEntry entry) {
        sink.match(
                buf -> {
                    buf.append(versionstamp, entry);
                    return null;
                },
                doc -> {
                    throw new IllegalStateException("This sink expects ByteBuffer");
                }
        );
    }

    void writeDocumentLocation(DataSink sink, int locationId, DocumentLocation location) {
        sink.match(
                buf -> {
                    throw new IllegalStateException("This sink expects DocumentLocation");
                },
                doc -> {
                    doc.append(locationId, location);
                    return null;
                }
        );
    }
}
