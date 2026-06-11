/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.zmap;

import com.apple.foundationdb.Transaction;
import com.google.common.io.BaseEncoding;
import com.kronotop.Context;
import com.kronotop.KronotopException;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;

/**
 * Multiplexes FoundationDB key watches for the ZWATCH command. Any number of clients waiting on the
 * same key share a single underlying FoundationDB watch, bounding the watch count to the number of
 * distinct watched keys rather than the number of waiting clients.
 *
 * <p>Each key carries a set of waiting clients and one underlying watch. The first client on a key
 * registers the watch; later clients only join the set. When the watch fires, every waiter receives the
 * signal. A client leaves by being removed from the set; the underlying watch is cancelled only when
 * the set drains to empty.</p>
 *
 * <p>Joining, leaving, and firing all mutate the same per-key set and are serialized through the
 * map's atomic {@code compute} on the key. The keys passed in are already namespace-scoped packed
 * FoundationDB keys, so two namespaces watching the same raw key watch different entries.</p>
 */
public class ZWatcher {
    private final Context context;
    private final ConcurrentHashMap<String, KeyWatch> watches = new ConcurrentHashMap<>();
    private final StampedLock lock = new StampedLock();
    private volatile boolean shutdown;

    public ZWatcher(Context context) {
        this.context = context;
    }

    private static String stringifyKey(byte[] key) {
        return BaseEncoding.base64().encode(key);
    }

    /**
     * Registers a watch on the given packed key for the given client and returns a future that completes
     * when the watch fires. If a watch already exists for the key, the client joins the existing one
     * and shares its baseline. A late joiner may therefore wake on a change committed before it
     * joined; the wake is harmless since the client reads the current value regardless.
     *
     * <p>When no watch exists yet, a candidate watch is registered outside the map's atomic section. If a
     * concurrent registration installed a watch first, the candidate is discarded and the client joins
     * the winner.</p>
     *
     * @param packedKey the namespace-scoped packed FoundationDB key to watch
     * @param clientId  the calling client's identifier
     * @return a future that completes when the watch fires, or is cancelled if the client leaves
     */
    public CompletableFuture<Void> register(byte[] packedKey, long clientId) {
        if (shutdown) {
            throw new KronotopException("ZWatcher is not reusable");
        }
        String key = stringifyKey(packedKey);
        CompletableFuture<Void> signal = new CompletableFuture<>();

        // Read lock blocks if shutdown (write lock) is in progress.
        long stamp = lock.readLock();
        try {
            // Re-check under the lock: a shutdown may have completed between the check above and here.
            // Holding the read lock now keeps shutdown out until this registration finishes.
            if (shutdown) {
                throw new KronotopException("ZWatcher is not reusable");
            }

            // Fast path: join an existing watch without registering a new one and without any FoundationDB I/O.
            boolean[] joined = {false};
            watches.computeIfPresent(key, (k, current) -> {
                current.waiters.put(clientId, signal);
                joined[0] = true;
                return current;
            });
            if (joined[0]) {
                return signal;
            }

            // Slow path: build a candidate watch outside compute, then install it or join whoever won.
            KeyWatch candidate = createWatch(packedKey);
            KeyWatch winner = watches.compute(key, (k, current) -> {
                if (current == null) {
                    candidate.waiters.put(clientId, signal);
                    return candidate;
                }
                current.waiters.put(clientId, signal);
                return current;
            });
            if (winner == candidate) {
                // Won the install race: wire the fire callback only now, after the candidate is visible
                // in the map. If the underlying watch already completed during commit, whenCompleteAsync
                // dispatches fire immediately; the identity check matches the installed candidate and the
                // waiter is signalled. Wiring before install would let an early fire find no map entry and
                // drop the only signal, leaving the waiter blocked forever.
                candidate.underlying.whenCompleteAsync(
                        (ignored, th) -> fire(key, candidate, th),
                        context.getVirtualThreadPerTaskExecutor());
            } else {
                // Lost the install race: discard the spare watch. The candidate was never wired, so its
                // cancellation has no fire callback to run.
                candidate.underlying.cancel(true);
            }
            return signal;
        } finally {
            lock.unlockRead(stamp);
        }
    }

    /**
     * Opens an underlying FoundationDB watch on its own transaction and wraps it in a fresh KeyWatch
     * without wiring its completion callback. Opening requires committing the snapshot the watch was
     * built from, so the watch observes changes committed after this point. The blocking commit runs
     * outside the map's atomic section. The caller wires the fire callback only after installing the
     * KeyWatch into the map, so an early completion cannot fire against an absent entry.
     */
    private KeyWatch createWatch(byte[] packedKey) {
        CompletableFuture<Void> underlying;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            underlying = tr.watch(packedKey);
            tr.commit().join();
        }
        return new KeyWatch(underlying);
    }

    /**
     * Signals every waiter on the key and removes the entry. A stale fire whose KeyWatch generation is
     * no longer the one mapped to the key leaves the current generation untouched and signals no one.
     * Serialized with registering and leaving through the atomic compute on the key.
     */
    private void fire(String key, KeyWatch fired, Throwable throwable) {
        watches.compute(key, (ignored, current) -> {
            if (current == null || current != fired) {
                return current;
            }
            for (CompletableFuture<Void> signal : current.waiters.values()) {
                if (throwable == null) {
                    signal.complete(null);
                } else {
                    signal.completeExceptionally(throwable);
                }
            }
            current.waiters.clear();
            return null;
        });
    }

    /**
     * Removes a single client from the key's waiter set, unblocking it. When the set drains to empty,
     * the underlying FoundationDB watch is cancelled and the entry removed.
     *
     * @param packedKey the namespace-scoped packed FoundationDB key the client was waiting on
     * @param clientId  the leaving client's identifier
     */
    public void leave(byte[] packedKey, long clientId) {
        String key = stringifyKey(packedKey);
        long stamp = lock.readLock();
        try {
            watches.computeIfPresent(key, (ignored, keyWatch) -> {
                CompletableFuture<Void> signal = keyWatch.waiters.remove(clientId);
                if (signal != null) {
                    signal.cancel(false);
                }
                if (keyWatch.waiters.isEmpty()) {
                    keyWatch.underlying.cancel(true);
                    return null;
                }
                return keyWatch;
            });
        } finally {
            lock.unlockRead(stamp);
        }
    }

    /**
     * Cancels all active watches and unblocks all waiters. The watcher is not reusable afterwards.
     */
    public void shutdown() {
        long stamp = lock.writeLock();
        try {
            shutdown = true;
            watches.forEach((ignored, keyWatch) -> {
                keyWatch.underlying.cancel(true);
                for (CompletableFuture<Void> signal : keyWatch.waiters.values()) {
                    signal.cancel(false);
                }
            });
            watches.clear();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Returns the number of distinct keys currently watched, which equals the number of underlying
     * FoundationDB watches.
     */
    public int watcherCount() {
        return watches.size();
    }

    /**
     * Returns the total number of waiting clients across all watched keys.
     */
    public int totalWaiters() {
        int[] total = {0};
        watches.forEach((ignored, keyWatch) -> total[0] += keyWatch.waiters.size());
        return total[0];
    }

    private static final class KeyWatch {
        private final CompletableFuture<Void> underlying;
        // Concurrent so shutdown() and totalWaiters() can iterate while fire/register/leave mutate.
        private final Map<Long, CompletableFuture<Void>> waiters = new ConcurrentHashMap<>();

        private KeyWatch(CompletableFuture<Void> underlying) {
            this.underlying = underlying;
        }
    }
}
