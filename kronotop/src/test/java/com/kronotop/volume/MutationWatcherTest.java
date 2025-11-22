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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MutationWatcherTest extends BaseVolumeIntegrationTest {

    @Test
    void shouldWatchForMutations() {
        byte[] trigger = volume.computeMutationTriggerKey();
        MutationWatcher watcher = new MutationWatcher(context);

        CompletableFuture<Void> future = watcher.watch(volume.getId(), trigger);

        assertNotNull(future);
        assertFalse(future.isDone());

        watcher.shutdown();
    }

    @Test
    void shouldTriggerWatcherOnAppend() throws IOException {
        byte[] trigger = volume.computeMutationTriggerKey();
        MutationWatcher watcher = new MutationWatcher(context);

        CompletableFuture<Void> future = watcher.watch(volume.getId(), trigger);
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
        MutationWatcher watcher = new MutationWatcher(context);

        CompletableFuture<Void> first = watcher.watch(volume.getId(), trigger);
        CompletableFuture<Void> second = watcher.watch(volume.getId(), trigger);

        assertSame(first, second);

        watcher.shutdown();
    }

    @Test
    void shouldCreateNewWatcherAfterPreviousCompletes() throws IOException {
        byte[] trigger = volume.computeMutationTriggerKey();
        MutationWatcher watcher = new MutationWatcher(context);

        CompletableFuture<Void> first = watcher.watch(volume.getId(), trigger);

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
        CompletableFuture<Void> second = watcher.watch(volume.getId(), trigger);
        assertNotSame(first, second);
        assertFalse(second.isDone());

        watcher.shutdown();
    }

    @Test
    void shouldUnwatchCancelWatcher() {
        byte[] trigger = volume.computeMutationTriggerKey();
        MutationWatcher watcher = new MutationWatcher(context);

        CompletableFuture<Void> future = watcher.watch(volume.getId(), trigger);
        assertFalse(future.isDone());

        watcher.unwatch(volume.getId(), trigger);

        assertTrue(future.isCancelled());

        watcher.shutdown();
    }

    @Test
    void shouldThrowExceptionWhenWatchingAfterShutdown() {
        byte[] trigger = volume.computeMutationTriggerKey();
        MutationWatcher watcher = new MutationWatcher(context);

        watcher.shutdown();

        assertThrows(KronotopException.class, () -> watcher.watch(volume.getId(), trigger));
    }

    @Test
    void shouldCancelAllWatchersOnShutdown() {
        byte[] trigger = volume.computeMutationTriggerKey();
        MutationWatcher watcher = new MutationWatcher(context);

        CompletableFuture<Void> future = watcher.watch(volume.getId(), trigger);
        assertFalse(future.isDone());

        watcher.shutdown();

        assertTrue(future.isCancelled());
    }
}
