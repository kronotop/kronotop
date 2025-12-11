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

package com.kronotop.volume;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.kronotop.internal.DirectorySubspaceCache;
import com.kronotop.journal.Consumer;
import com.kronotop.journal.ConsumerConfig;
import com.kronotop.journal.Event;
import com.kronotop.journal.JournalName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class MarkStalePrefixesTaskTest extends BaseVolumeIntegrationTest {

    private int countPrefixes() {
        DirectorySubspace prefixesSubspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.PREFIXES);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] begin = prefixesSubspace.pack();
            byte[] end = ByteArrayUtil.strinc(begin);
            Range range = new Range(begin, end);
            List<KeyValue> prefixes = tr.getRange(range).asList().join();
            return prefixes.size();
        }
    }

    @Test
    void shouldMarkAndRemoveAllStalePrefixes() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-subspace");
        byte[] prefixPointer = subspace.pack("prefix-pointer");
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < 20; i++) {
                Prefix testPrefix = new Prefix(UUID.randomUUID().toString());
                PrefixUtil.register(context, tr, prefixPointer, testPrefix);
            }
            tr.commit().join();
        }

        // Remove the pointer and make prefixes stale
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            tr.clear(prefixPointer);
            tr.commit().join();
        }

        // 20/5 = 4, it will take 4 iterations to remove all stale prefixes.
        MarkStalePrefixesTask task = new MarkStalePrefixesTask(context, 5);
        task.run();

        assertEquals(0, countPrefixes());
    }

    @Test
    void shouldKeepNonStalePrefixes() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-subspace");

        // Register 10 non-stale prefixes with valid pointers
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < 10; i++) {
                byte[] prefixPointer = subspace.pack("valid-pointer-" + i);
                Prefix testPrefix = new Prefix(UUID.randomUUID().toString());
                PrefixUtil.register(context, tr, prefixPointer, testPrefix);
            }
            tr.commit().join();
        }

        // Register 10 stale prefixes (pointer will be removed)
        byte[] stalePointer = subspace.pack("stale-pointer");
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < 10; i++) {
                Prefix testPrefix = new Prefix(UUID.randomUUID().toString());
                PrefixUtil.register(context, tr, stalePointer, testPrefix);
            }
            tr.commit().join();
        }

        assertEquals(20, countPrefixes());

        // Remove the stale pointer
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            tr.clear(stalePointer);
            tr.commit().join();
        }

        MarkStalePrefixesTask task = new MarkStalePrefixesTask(context, 5);
        task.run();

        // Only 10 non-stale prefixes should remain
        assertEquals(10, countPrefixes());
    }

    @Test
    void shouldCompleteImmediatelyWhenNoPrefixesExist() {
        // No prefixes registered
        assertEquals(0, countPrefixes());

        MarkStalePrefixesTask task = new MarkStalePrefixesTask(context);
        task.run();

        assertTrue(task.isCompleted());
        assertEquals(0, countPrefixes());
    }

    @Test
    void shouldCompleteWhenAllPrefixesAreValid() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-subspace");

        // Register 10 prefixes with valid pointers
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < 10; i++) {
                byte[] prefixPointer = subspace.pack("valid-pointer-" + i);
                Prefix testPrefix = new Prefix(UUID.randomUUID().toString());
                PrefixUtil.register(context, tr, prefixPointer, testPrefix);
            }
            tr.commit().join();
        }

        assertEquals(10, countPrefixes());

        MarkStalePrefixesTask task = new MarkStalePrefixesTask(context, 5);
        task.run();

        assertTrue(task.isCompleted());
        assertEquals(10, countPrefixes());
    }

    @Test
    void shouldPublishStalePrefixesToJournal() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-subspace");

        // Register 5 stale prefixes
        byte[] stalePointer = subspace.pack("stale-pointer");
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < 5; i++) {
                Prefix testPrefix = new Prefix(UUID.randomUUID().toString());
                PrefixUtil.register(context, tr, stalePointer, testPrefix);
            }
            tr.commit().join();
        }

        // Remove the pointer to make prefixes stale
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            tr.clear(stalePointer);
            tr.commit().join();
        }

        MarkStalePrefixesTask task = new MarkStalePrefixesTask(context);
        task.run();

        // Consume events from the DISUSED_PREFIXES journal
        ConsumerConfig config = new ConsumerConfig(
                "test-consumer",
                JournalName.DISUSED_PREFIXES.getValue(),
                ConsumerConfig.Offset.EARLIEST
        );
        Consumer consumer = new Consumer(context, config);
        consumer.start();

        List<byte[]> publishedPrefixes = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Event event;
            while ((event = consumer.consume(tr)) != null) {
                publishedPrefixes.add(event.value());
                consumer.markConsumed(tr, event);
            }
            tr.commit().join();
        }
        consumer.stop();

        assertEquals(5, publishedPrefixes.size());
    }

    @Test
    void shouldResumeFromLastPrefixAfterRestart() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-subspace");

        // Register 20 stale prefixes
        byte[] stalePointer = subspace.pack("stale-pointer");
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < 20; i++) {
                Prefix testPrefix = new Prefix(UUID.randomUUID().toString());
                PrefixUtil.register(context, tr, stalePointer, testPrefix);
            }
            tr.commit().join();
        }

        // Remove the pointer to make prefixes stale
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            tr.clear(stalePointer);
            tr.commit().join();
        }

        assertEquals(20, countPrefixes());

        // First task: run in a separate thread and shutdown immediately
        MarkStalePrefixesTask firstTask = new MarkStalePrefixesTask(context, 5);
        Thread taskThread = new Thread(firstTask);
        taskThread.start();
        firstTask.shutdown();

        // Second task: should resume and complete the remaining work
        MarkStalePrefixesTask secondTask = new MarkStalePrefixesTask(context, 5);
        secondTask.run();

        assertTrue(secondTask.isCompleted());
        assertEquals(0, countPrefixes());
    }

    @Test
    void shouldStopGracefullyOnShutdown() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-subspace");

        // Register stale prefixes
        byte[] stalePointer = subspace.pack("stale-pointer");
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < 20; i++) {
                Prefix testPrefix = new Prefix(UUID.randomUUID().toString());
                PrefixUtil.register(context, tr, stalePointer, testPrefix);
            }
            tr.commit().join();
        }

        // Remove the pointer to make prefixes stale
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            tr.clear(stalePointer);
            tr.commit().join();
        }

        MarkStalePrefixesTask task = new MarkStalePrefixesTask(context, 5);
        Thread taskThread = new Thread(task);
        taskThread.start();

        // Call shutdown - it should complete gracefully without throwing exceptions,
        // regardless of whether the task has finished or is still running
        assertDoesNotThrow(task::shutdown);
    }
}