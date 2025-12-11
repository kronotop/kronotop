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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MutationWatcherTest extends BaseVolumeIntegrationTest {

    @Test
    void shouldWatchForMutations() {
        byte[] trigger = volume.computeMutationTriggerKey();
        MutationWatcher watcher = new MutationWatcher();

        CompletableFuture<Void> future;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            future = watcher.watch(tr, volume.getId(), trigger);
        }

        assertNotNull(future);
        assertFalse(future.isDone());

        watcher.shutdown();
    }

    @Test
    void shouldTriggerWatcherOnAppend() throws IOException {
        byte[] trigger = volume.computeMutationTriggerKey();
        MutationWatcher watcher = new MutationWatcher();

        CompletableFuture<Void> future;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            future = watcher.watch(tr, volume.getId(), trigger);
        }
        assertFalse(future.isDone());

        // Append triggers the watcher
        ByteBuffer[] entries = getEntries(1);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        // Watcher should complete
        assertDoesNotThrow(() -> future.get(5, TimeUnit.SECONDS));
        assertTrue(future.isDone());

        watcher.shutdown();
    }

    @Test
    void shouldReturnSameWatcherForSameVolume() {
        byte[] trigger = volume.computeMutationTriggerKey();
        MutationWatcher watcher = new MutationWatcher();

        CompletableFuture<Void> first;
        CompletableFuture<Void> second;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            first = watcher.watch(tr, volume.getId(), trigger);
        }
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            second = watcher.watch(tr, volume.getId(), trigger);
        }

        assertSame(first, second);

        watcher.shutdown();
    }

    @Test
    void shouldCreateNewWatcherAfterPreviousCompletes() throws IOException {
        byte[] trigger = volume.computeMutationTriggerKey();
        MutationWatcher watcher = new MutationWatcher();

        CompletableFuture<Void> first;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            first = watcher.watch(tr, volume.getId(), trigger);
        }

        // Trigger the watcher
        ByteBuffer[] entries = getEntries(1);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        assertDoesNotThrow(() -> first.get(5, TimeUnit.SECONDS));
        assertTrue(first.isDone());

        // New watch should create a new watcher
        CompletableFuture<Void> second;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            second = watcher.watch(tr, volume.getId(), trigger);
        }
        assertNotSame(first, second);
        assertFalse(second.isDone());

        watcher.shutdown();
    }

    @Test
    void shouldUnwatchCancelWatcher() {
        byte[] trigger = volume.computeMutationTriggerKey();
        MutationWatcher watcher = new MutationWatcher();

        CompletableFuture<Void> future;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            future = watcher.watch(tr, volume.getId(), trigger);
        }
        assertFalse(future.isDone());

        watcher.unwatch(volume.getId());

        assertTrue(future.isCancelled());

        watcher.shutdown();
    }

    @Test
    void shouldThrowExceptionWhenWatchingAfterShutdown() {
        byte[] trigger = volume.computeMutationTriggerKey();
        MutationWatcher watcher = new MutationWatcher();

        watcher.shutdown();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertThrows(KronotopException.class, () -> watcher.watch(tr, volume.getId(), trigger));
        }
    }

    @Test
    void shouldCancelAllWatchersOnShutdown() {
        byte[] trigger = volume.computeMutationTriggerKey();
        MutationWatcher watcher = new MutationWatcher();

        CompletableFuture<Void> future;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            future = watcher.watch(tr, volume.getId(), trigger);
        }
        assertFalse(future.isDone());

        watcher.shutdown();

        assertTrue(future.isCancelled());
    }

    @Test
    void shouldTriggerAllWaitingThreadsOnAppend() throws InterruptedException, IOException {
        byte[] trigger = volume.computeMutationTriggerKey();
        MutationWatcher watcher = new MutationWatcher();

        int threadCount = 10;
        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch allThreadsReady = new CountDownLatch(threadCount);
        CountDownLatch allThreadsDone = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            Thread.startVirtualThread(() -> {
                CompletableFuture<Void> future;
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    future = watcher.watch(tr, volume.getId(), trigger);
                }
                allThreadsReady.countDown();

                try {
                    future.get(10, TimeUnit.SECONDS);
                    counter.incrementAndGet();
                } catch (Exception e) {
                    fail("Watcher should complete without exception");
                } finally {
                    allThreadsDone.countDown();
                }
            });
        }

        // Wait for all threads to register their watchers
        assertTrue(allThreadsReady.await(5, TimeUnit.SECONDS));

        // Trigger the watcher with an append
        ByteBuffer[] entries = getEntries(1);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        // Wait for all threads to complete
        assertTrue(allThreadsDone.await(10, TimeUnit.SECONDS));

        assertEquals(threadCount, counter.get());

        watcher.shutdown();
    }
}
