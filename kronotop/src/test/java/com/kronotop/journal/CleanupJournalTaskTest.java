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

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class CleanupJournalTaskTest extends BaseStandaloneInstanceTest {
    private final String TEST_JOURNAL = "test-journal";

    private Event fetchEarliestEvent() {
        ConsumerConfig config = new ConsumerConfig("test-consumer", TEST_JOURNAL, ConsumerConfig.Offset.EARLIEST);
        Consumer consumer = new Consumer(context, config);
        consumer.start();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return consumer.consume(tr);
        }
    }

    @Test
    public void should_all_entries_be_evicted() {
        Journal journal = new Journal(config, context.getFoundationDB());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < 3; i++) {
                journal.getPublisher().publish(tr, TEST_JOURNAL, "message " + i);
                Thread.sleep(10);
            }
            tr.commit().join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        CleanupJournalTask task = new CleanupJournalTask(journal, 5, TimeUnit.MILLISECONDS);
        task.run();

        assertNull(fetchEarliestEvent());
    }

    @Test
    public void should_journal_entry_not_be_evicted() {
        Journal journal = new Journal(config, context.getFoundationDB());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            journal.getPublisher().publish(tr, TEST_JOURNAL, "message");
            tr.commit().join();
        }

        // TTL = 1 second
        CleanupJournalTask task = new CleanupJournalTask(journal, 1000, TimeUnit.MILLISECONDS);
        task.run();
        assertNotNull(fetchEarliestEvent());
    }
}