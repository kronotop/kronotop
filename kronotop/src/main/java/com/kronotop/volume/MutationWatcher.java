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
import com.kronotop.Context;
import com.kronotop.KronotopException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;

public class MutationWatcher {
    private final Context context;
    private final ConcurrentHashMap<Long, CompletableFuture<Void>> watchers;
    private final StampedLock lock = new StampedLock();
    private volatile boolean shutdown;

    public MutationWatcher(Context context) {
        this.context = context;
        this.watchers = new ConcurrentHashMap<>();
    }

    private CompletableFuture<Void> setWatcher(long volumeId, byte[] key) {
        // Burada Transaction açıp FDB'ye gidiyoruz.
        // Bu işlem map.compute bloğu içinde olduğu için thread-safe.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            CompletableFuture<Void> watcher = tr.watch(key);
            tr.commit().join();

            // BU KISIM ŞART: Watch tetiklendiğinde map'ten temizlenmeli.
            // Yoksa shard silinse bile map'te zombie entry kalır.
            watcher.whenComplete((v, th) -> {
                // remove(key, value) sadece bu specific instance map'teyse siler.
                // Race condition yaratmaz.
                watchers.remove(volumeId, watcher);
            });

            return watcher;
        }
    }

    public CompletableFuture<Void> watch(long volumeId, byte[] key) {
        if (shutdown) {
            throw new KronotopException("MutationWatcher is not reusable");
        }

        // "Biletini göster geç" (Read Lock).
        // Eğer Shutdown (Write Lock) devredeyse, burası BLOKLANIR.
        long stamp = lock.readLock();
        try {

            // compute bloğu atomiktir.
            // 10 thread aynı anda gelse bile setWatcher sadece 1 kere çalışır.
            return watchers.compute(volumeId, (k, existing) -> {
                if (existing == null || existing.isDone()) {
                    return setWatcher(volumeId, key);
                }
                return existing;
            });
        } finally {
            // Kilidi bırak
            lock.unlockRead(stamp);
        }
    }

    public void unwatch(long volumeId, byte[] key) {
        long stamp = lock.readLock();
        try {
            watchers.computeIfPresent(volumeId, (ignored, watcher) -> {
                watcher.cancel(true);
                return null; // Entry'yi siler
            });
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public void shutdown() {
        // Write Lock alıyoruz.
        // Bu satır çalıştığı an, watch() metodundaki lock.readLock() çağrıları bloklanır.
        // İçeride (try bloğunda) işlem yapan varsa, onların bitmesi beklenir.
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
}