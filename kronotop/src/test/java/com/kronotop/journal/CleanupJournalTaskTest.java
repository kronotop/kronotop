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

package com.kronotop.journal;

import com.apple.foundationdb.Transaction;
import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class CleanupJournalTaskTest extends BaseStandaloneInstanceTest {
    private final String TEST_JOURNAL = "test-journal";

    private List<Event> consumeAllEvents() {
        ConsumerConfig config = new ConsumerConfig("test-consumer-" + System.nanoTime(), TEST_JOURNAL, ConsumerConfig.Offset.EARLIEST);
        Consumer consumer = new Consumer(context, config);
        consumer.start();

        List<Event> events = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Event event;
            while ((event = consumer.consume(tr)) != null) {
                events.add(event);
                consumer.markConsumed(tr, event);
            }
            tr.commit().join();
        }
        consumer.stop();
        return events;
    }

    @Test
    void shouldEvictAllExpiredEntries() {
        Journal journal = new Journal(config, context.getFoundationDB());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < 3; i++) {
                journal.getPublisher().publish(tr, TEST_JOURNAL, "message " + i);
            }
            tr.commit().join();
        }

        // TTL of 0 means all entries are immediately expired
        CleanupJournalTask task = new CleanupJournalTask(journal, 0, TimeUnit.MILLISECONDS);
        task.run();

        assertEquals(0, consumeAllEvents().size());
    }

    @Test
    void shouldNotEvictJournalEntryWhenWithinTTL() {
        Journal journal = new Journal(config, context.getFoundationDB());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            journal.getPublisher().publish(tr, TEST_JOURNAL, "message");
            tr.commit().join();
        }

        // TTL = 1 hour, entry should not be expired
        CleanupJournalTask task = new CleanupJournalTask(journal, 1, TimeUnit.HOURS);
        task.run();

        assertEquals(1, consumeAllEvents().size());
    }

    @Test
    void shouldEvictOnlyExpiredEntries() {
        Journal journal = new Journal(config, context.getFoundationDB());

        // Publish first batch of entries
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < 3; i++) {
                journal.getPublisher().publish(tr, TEST_JOURNAL, "old message " + i);
            }
            tr.commit().join();
        }

        // Evict all existing entries (TTL=0)
        CleanupJournalTask cleanupTask = new CleanupJournalTask(journal, 0, TimeUnit.MILLISECONDS);
        cleanupTask.run();

        // Publish the second batch of entries
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < 3; i++) {
                journal.getPublisher().publish(tr, TEST_JOURNAL, "new message " + i);
            }
            tr.commit().join();
        }

        // Run cleanup with long TTL - new entries should survive
        CleanupJournalTask keepTask = new CleanupJournalTask(journal, 1, TimeUnit.HOURS);
        keepTask.run();

        // Only the 3 new entries should remain
        assertEquals(3, consumeAllEvents().size());
    }

    @Test
    void shouldHandleEmptyJournal() {
        Journal journal = new Journal(config, context.getFoundationDB());

        // Create the journal without publishing any entries
        journal.getJournalMetadata(TEST_JOURNAL);

        // Task should complete without error on empty journal
        CleanupJournalTask task = new CleanupJournalTask(journal, 0, TimeUnit.MILLISECONDS);
        assertDoesNotThrow(task::run);
    }

    @Test
    void shouldCleanupMultipleJournals() {
        Journal journal = new Journal(config, context.getFoundationDB());
        String journal1 = "cleanup-test-journal-1";
        String journal2 = "cleanup-test-journal-2";

        // Publish entries to both journals
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < 3; i++) {
                journal.getPublisher().publish(tr, journal1, "message " + i);
                journal.getPublisher().publish(tr, journal2, "message " + i);
            }
            tr.commit().join();
        }

        // Verify both journals exist
        List<String> journals = journal.listJournals();
        assertTrue(journals.contains(journal1));
        assertTrue(journals.contains(journal2));

        // TTL of 0 means all entries are immediately expired
        CleanupJournalTask task = new CleanupJournalTask(journal, 0, TimeUnit.MILLISECONDS);
        task.run();

        // Verify both journals are cleaned
        for (String journalName : List.of(journal1, journal2)) {
            ConsumerConfig consumerConfig = new ConsumerConfig("consumer-" + journalName, journalName, ConsumerConfig.Offset.EARLIEST);
            Consumer consumer = new Consumer(context, consumerConfig);
            consumer.start();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                assertNull(consumer.consume(tr), "Journal " + journalName + " should be empty");
            }
            consumer.stop();
        }
    }

    @Test
    void shouldNeverComplete() {
        Journal journal = new Journal(config, context.getFoundationDB());

        CleanupJournalTask task = new CleanupJournalTask(journal, 1, TimeUnit.HOURS);

        // Before running
        assertFalse(task.isCompleted());

        task.run();

        // After running - still not completed (perpetual task)
        assertFalse(task.isCompleted());
    }

    @Test
    void shouldHandleShutdownGracefully() {
        Journal journal = new Journal(config, context.getFoundationDB());

        CleanupJournalTask task = new CleanupJournalTask(journal, 1, TimeUnit.HOURS);

        // Shutdown is a no-op for this task, should not throw
        assertDoesNotThrow(task::shutdown);
    }
}