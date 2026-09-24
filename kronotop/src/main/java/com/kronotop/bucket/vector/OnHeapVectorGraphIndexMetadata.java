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
    private final Map<ObjectId, GraphNodeRef> objectIds = new HashMap<>();
    private final Map<Integer, DocumentLocation> ordinals = new HashMap<>();
    private final AtomicReference<Versionstamp> firstVersionstamp = new AtomicReference<>();
    private final AtomicReference<Versionstamp> latestVersionstamp = new AtomicReference<>();

    /**
     * Binds the object to the node with the given ordinal and add versionstamp. The node with the older
     * versionstamp loses: its location is dropped and its ordinal is queued for deletion. Returns the
     * ordinal of the stale node, or -1 when the object had no node before.
     */
    public int put(ObjectId objectId, int ordinal, Versionstamp versionstamp, int shardId, EntryMetadata metadata) {
        long stamp = lock.writeLock();
        try {
            GraphNodeRef existing = objectIds.get(objectId);
            if (existing != null && existing.versionstamp().compareTo(versionstamp) > 0) {
                // Stale add: the existing node is newer, the incoming node loses.
                pendingDeletes.add(ordinal);
                return ordinal;
            }
            objectIds.put(objectId, new GraphNodeRef(ordinal, versionstamp));
            ordinals.put(ordinal, new DocumentLocation(objectId, shardId, metadata));
            if (existing == null) {
                return -1;
            }
            ordinals.remove(existing.ordinal());
            pendingDeletes.add(existing.ordinal());
            // Return the stale node's ordinal.
            return existing.ordinal();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Removes the mapping of the object if the delete is newer than the node. Returns the ordinal of the
     * removed node, or -1 when the object has no node or the delete is stale.
     */
    public int removeMapping(ObjectId objectId, Versionstamp deleteVs) {
        long stamp = lock.writeLock();
        try {
            GraphNodeRef ref = objectIds.get(objectId);
            if (ref == null || deleteVs.compareTo(ref.versionstamp()) <= 0) {
                return -1;
            }
            objectIds.remove(objectId);
            ordinals.remove(ref.ordinal());
            pendingDeletes.add(ref.ordinal());
            return ref.ordinal();
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

    Map<ObjectId, GraphNodeRef> getObjectIds() {
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

    public GraphNodeRef findNodeRef(ObjectId objectId) {
        long stamp = lock.tryOptimisticRead();
        GraphNodeRef ref = objectIds.get(objectId);
        if (!lock.validate(stamp)) {
            stamp = lock.readLock();
            try {
                ref = objectIds.get(objectId);
            } finally {
                lock.unlockRead(stamp);
            }
        }
        return ref;
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
