/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop;

import com.apple.foundationdb.Transaction;
import com.google.common.io.BaseEncoding;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The KeyWatcher class is responsible for watching keys in FoundationDB transactions.
 * It allows registering a key to be watched and cancelling the watch on a key.
 */
public class KeyWatcher {
    private final ConcurrentHashMap<String, CompletableFuture<Void>> watchers = new ConcurrentHashMap<>();

    private String stringifyKey(byte[] key) {
        return BaseEncoding.base64().encode(key);
    }

    /**
     * Watches a key in the transaction and returns a CompletableFuture that completes when the key changes or is deleted.
     * It cancels and removes the previous watcher, if any.
     *
     * @param tr  the transaction to watch the key in
     * @param key the key to watch
     * @return a CompletableFuture that completes when the key changes or is deleted
     */
    public CompletableFuture<Void> watch(Transaction tr, byte[] key) {
        return watchers.compute(stringifyKey(key), (k, watcher) -> {
            if (watcher != null) {
                watcher.cancel(true);
            }
            return tr.watch(key);
        });
    }

    /**
     * Cancels and removes the watch on a key in the key watcher.
     *
     * @param key the key to unwatch
     */
    public void unwatch(byte[] key) {
        watchers.computeIfPresent(stringifyKey(key), (k, watcher) -> {
            watcher.cancel(true);
            return null;
        });
    }
}
