package com.kronotop.bucket.pipeline;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Output {
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Map<Integer, Map<Integer, DocumentLocation>> locationsByNodeId = new LinkedHashMap<>();

    public void appendLocation(int nodeId, int locationId, DocumentLocation location) {
        lock.writeLock().lock();
        try {
            Map<Integer, DocumentLocation> locations = locationsByNodeId.computeIfAbsent(nodeId,
                    (ignored) -> new LinkedHashMap<>());
            locations.put(locationId, location);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Map<Integer, DocumentLocation> getLocations(int nodeId) {
        lock.readLock().lock();
        try {
            return locationsByNodeId.get(nodeId);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void clear(int nodeId) {
        lock.writeLock().lock();
        try {
            Map<Integer, DocumentLocation> locations = locationsByNodeId.get(nodeId);
            locations.clear();
            locationsByNodeId.remove(nodeId);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
