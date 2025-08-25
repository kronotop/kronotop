package com.kronotop.bucket.pipeline;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Output {
    private final Map<Integer, Map<Integer, DocumentLocation>> locationsByNodeId = new ConcurrentHashMap<>();

    public void appendLocation(int nodeId, int locationId, DocumentLocation location) {
        Map<Integer, DocumentLocation> locations = locationsByNodeId.computeIfAbsent(nodeId,
                (ignored) -> new ConcurrentHashMap<>());
        locations.put(locationId, location);
    }

    public Map<Integer, DocumentLocation> getLocations(int nodeId) {
        Map<Integer, DocumentLocation> locations = locationsByNodeId.get(nodeId);
        if (locations == null) {
            return null;
        }
        return Collections.unmodifiableMap(locations);
    }
}
