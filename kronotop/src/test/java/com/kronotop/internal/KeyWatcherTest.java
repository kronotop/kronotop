/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.internal;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class KeyWatcherTest extends BaseStandaloneInstanceTest {

    @Test
    public void test_watch_then_unwatch() throws InterruptedException {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test_watch_then_unwatch");
        byte[] key = subspace.pack("key");
        KeyWatcher keyWatcher = new KeyWatcher();

        CompletableFuture<Void> watcher;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            watcher = keyWatcher.watch(tr, key);
            tr.commit().join();
        }

        CountDownLatch latch = new CountDownLatch(1);

        class WatchTrigger implements Runnable {
            @Override
            public void run() {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    tr.set(key, "foobar".getBytes());
                    tr.commit().join();
                }
            }
        }

        class WatchListener implements Runnable {
            @Override
            public void run() {
                latch.countDown();
                assertDoesNotThrow(watcher::join);
            }
        }

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            executor.submit(new WatchListener());
            latch.await();
            executor.submit(new WatchTrigger());
            assertDoesNotThrow(() -> keyWatcher.unwatch(key));
        } finally {
            executor.shutdownNow();
        }
    }
}