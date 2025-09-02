package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Output {
    private final ReadWriteLock locationsLock = new ReentrantReadWriteLock(true);
    private final Map<Integer, Map<Integer, DocumentLocation>> locationsByNodeId = new LinkedHashMap<>();

    private final ReadWriteLock documentsLock = new ReentrantReadWriteLock(true);
    private final Map<Integer, Map<Versionstamp, ByteBuffer>> documents = new LinkedHashMap<>();

    public void appendLocation(int nodeId, int locationId, DocumentLocation location) {
        locationsLock.writeLock().lock();
        try {
            Map<Integer, DocumentLocation> locations = locationsByNodeId.computeIfAbsent(nodeId,
                    (ignored) -> new LinkedHashMap<>());
            locations.put(locationId, location);
        } finally {
            locationsLock.writeLock().unlock();
        }
    }

    public void appendDocument(int nodeId, Versionstamp versionstamp, ByteBuffer document) {
        documentsLock.writeLock().lock();
        try {
            Map<Versionstamp, ByteBuffer> locations = documents.computeIfAbsent(nodeId,
                    (ignored) -> new LinkedHashMap<>());
            locations.put(versionstamp, document);
        } finally {
            documentsLock.writeLock().unlock();
        }
    }

    public Map<Integer, DocumentLocation> getLocations(int nodeId) {
        locationsLock.readLock().lock();
        try {
            return locationsByNodeId.get(nodeId);
        } finally {
            locationsLock.readLock().unlock();
        }
    }

    public Map<Versionstamp, ByteBuffer> getDocuments(int nodeId) {
        documentsLock.readLock().lock();
        try {
            return documents.get(nodeId);
        } finally {
            documentsLock.readLock().unlock();
        }
    }

    public void clearLocations(int nodeId) {
        locationsLock.writeLock().lock();
        try {
            locationsByNodeId.remove(nodeId);
        } finally {
            locationsLock.writeLock().unlock();
        }
    }

    public void removeDocuments(int nodeId) {
        documentsLock.writeLock().lock();
        try {
            documents.remove(nodeId);
        } finally {
            documentsLock.writeLock().unlock();
        }
    }
}
