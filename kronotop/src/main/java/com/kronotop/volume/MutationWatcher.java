/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;
import com.kronotop.KronotopException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;

/**
 * Manages FoundationDB key watches for volume mutations with thread-safe lifecycle handling.
 *
 * <p>This class multiplexes watch requests: multiple callers waiting on the same volume share
 * a single FoundationDB watch. Only one watcher exists per volume at any given time.</p>
 *
 * <p>Callers should pass their own transaction to avoid race conditions. By reading state and
 * creating the watch in the same transaction, FoundationDB guarantees that if the key was
 * modified after the transaction's read version, the watch fires immediately after commit.</p>
 *
 * <p>Uses {@link StampedLock} to coordinate between concurrent watch requests and shutdown.
 * Instance is not reusable after shutdown.</p>
 */
public class MutationWatcher {
    private final ConcurrentHashMap<Long, CompletableFuture<Void>> watchers = new ConcurrentHashMap<>();
    private final StampedLock lock = new StampedLock();
    private volatile boolean shutdown;

    /**
     * Creates and registers a new watcher for the given key.
     * Thread-safe as it runs within the map's compute block.
     */
    private CompletableFuture<Void> setWatcher(Transaction tr, long volumeId, byte[] key) {
        CompletableFuture<Void> watcher = tr.watch(key);
        tr.commit().join();

        // Clean up from the map when watch triggers to prevent zombie entries.
        watcher.whenComplete((v, th) -> {
            // Atomic remove only if this specific instance is still in the map.
            watchers.remove(volumeId, watcher);
        });

        return watcher;
    }


    /**
     * Watches for mutations on the specified key for a volume.
     *
     * <p>If a watcher already exists and is still active, returns the existing one.
     * The compute operation is atomic - even with concurrent calls, only one watcher is created.</p>
     *
     * <p>The caller must pass a transaction that was used to read the current state. This ensures
     * no events are missed: if the key changes between reading and committing, the watch fires
     * immediately. The transaction is committed internally.</p>
     *
     * @param tr       the transaction used to read current state (will be committed)
     * @param volumeId the volume identifier
     * @param key      the FoundationDB key to watch
     * @return a future that completes when the key is mutated
     * @throws KronotopException if called after shutdown
     */
    public CompletableFuture<Void> watch(Transaction tr, long volumeId, byte[] key) {
        if (shutdown) {
            throw new KronotopException("MutationWatcher is not reusable");
        }

        // Read lock blocks if shutdown (write lock) is in progress.
        long stamp = lock.readLock();
        try {
            return watchers.compute(volumeId, (k, existing) -> {
                if (existing == null || existing.isDone()) {
                    return setWatcher(tr, volumeId, key);
                }
                return existing;
            });
        } finally {
            lock.unlockRead(stamp);
        }
    }

    /**
     * Cancels and removes the watcher for the specified volume.
     *
     * @param volumeId the volume identifier
     */
    public void unwatch(long volumeId) {
        long stamp = lock.readLock();
        try {
            watchers.computeIfPresent(volumeId, (ignored, watcher) -> {
                watcher.cancel(true);
                return null;
            });
        } finally {
            lock.unlockRead(stamp);
        }
    }

    /**
     * Shuts down the watcher, cancelling all active watches.
     *
     * <p>Acquires write lock to block new watch requests and waits for
     * in-progress operations to complete before cancelling all watchers.</p>
     */
    public void shutdown() {
        long stamp = lock.writeLock();
        try {
            shutdown = true;
            watchers.forEach((ignored, watcher) -> {
                watcher.cancel(true);
            });
            watchers.clear();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public boolean hasWatcher(long volumeId) {
        return watchers.containsKey(volumeId);
    }
}