/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.vector;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.pipeline.DocumentLocation;
import com.kronotop.volume.EntryMetadata;
import org.bson.types.ObjectId;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

public class OnHeapVectorGraphIndexMetadata implements VectorGraphIndexMetadata {
    private final StampedLock lock = new StampedLock();
    private final HashSet<Integer> pendingDeletes = new HashSet<>();
    private final Map<ObjectId, Integer> objectIds = new HashMap<>();
    private final Map<Integer, DocumentLocation> ordinals = new HashMap<>();
    private final AtomicReference<Versionstamp> firstVersionstamp = new AtomicReference<>();
    private final AtomicReference<Versionstamp> latestVersionstamp = new AtomicReference<>();

    public void put(ObjectId objectId, int ordinal, int shardId, EntryMetadata metadata) {
        long stamp = lock.writeLock();
        try {
            // Ordinals are monotonically increasing, so a higher ordinal always
            // represents a more recent version. Skip the write if the existing
            // ordinal is already newer to prevent stale data from overwriting it.
            Integer existing = objectIds.get(objectId);
            if (existing != null && existing > ordinal) {
                return;
            }
            objectIds.put(objectId, ordinal);
            ordinals.put(ordinal, new DocumentLocation(objectId, shardId, metadata));
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public void removeMapping(ObjectId objectId, int ordinal) {
        long stamp = lock.writeLock();
        try {
            objectIds.remove(objectId);
            ordinals.remove(ordinal);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public void addPendingDelete(int node) {
        long stamp = lock.writeLock();
        try {
            pendingDeletes.add(node);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public void clearPendingDeletes(Consumer<? super Integer> action) {
        long stamp = lock.writeLock();
        try {
            pendingDeletes.forEach(action);
            pendingDeletes.clear();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public DocumentLocation findDocumentLocation(int ordinal) {
        long stamp = lock.tryOptimisticRead();
        DocumentLocation entry = ordinals.get(ordinal);
        if (!lock.validate(stamp)) {
            stamp = lock.readLock();
            try {
                entry = ordinals.get(ordinal);
            } finally {
                lock.unlockRead(stamp);
            }
        }
        return entry;
    }

    Map<ObjectId, Integer> getObjectIds() {
        return Collections.unmodifiableMap(objectIds);
    }

    Map<Integer, DocumentLocation> getOrdinals() {
        return Collections.unmodifiableMap(ordinals);
    }

    /**
     * Returns true when at least one live ordinal mapping exists. Deleted nodes are removed
     * from the mappings at delete time, so this reflects graph liveness.
     */
    public boolean hasLiveMappings() {
        long stamp = lock.tryOptimisticRead();
        boolean result = !ordinals.isEmpty();
        if (!lock.validate(stamp)) {
            stamp = lock.readLock();
            try {
                result = !ordinals.isEmpty();
            } finally {
                lock.unlockRead(stamp);
            }
        }
        return result;
    }

    public int findOrdinal(ObjectId objectId) {
        long stamp = lock.tryOptimisticRead();
        int ordinal = objectIds.getOrDefault(objectId, -1);
        if (!lock.validate(stamp)) {
            stamp = lock.readLock();
            try {
                ordinal = objectIds.getOrDefault(objectId, -1);
            } finally {
                lock.unlockRead(stamp);
            }
        }
        return ordinal;
    }

    /**
     * Advances the latest versionstamp if the given one is newer. Thread-safe via CAS loop.
     */
    public void advanceVersionstamp(Versionstamp versionstamp) {
        firstVersionstamp.compareAndSet(null, versionstamp);
        while (true) {
            Versionstamp current = latestVersionstamp.get();
            if (current != null && Arrays.compareUnsigned(current.getBytes(), versionstamp.getBytes()) >= 0) {
                return;
            }
            if (latestVersionstamp.compareAndSet(current, versionstamp)) {
                return;
            }
        }
    }

    public Versionstamp getFirstVersionstamp() {
        return firstVersionstamp.get();
    }

    public Versionstamp getLatestVersionstamp() {
        return latestVersionstamp.get();
    }
}
