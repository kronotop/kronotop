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

package com.kronotop.journal;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.task.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * CleanupJournalsTask is a background task that periodically checks and removes expired entries
 * from a journal based on a configured time-to-live (TTL) value.
 */
public class CleanupJournalTask implements Task {
    private static final Logger LOGGER = LoggerFactory.getLogger(CleanupJournalTask.class);

    private final Duration retentionPeriod;
    private final Journal journal;

    private int counter = 0;
    private long now;

    public CleanupJournalTask(Journal journal, long retentionPeriod, TimeUnit unit) {
        this.journal = journal;
        this.retentionPeriod = Duration.of(retentionPeriod, unit.toChronoUnit());
    }

    @Override
    public String name() {
        return "journal:cleanup-task";
    }

    @Override
    public boolean isCompleted() {
        // Never ending task
        return false;
    }

    @Override
    public void shutdown() {
        // No state to clean
    }

    @Override
    public void awaitTermination() throws InterruptedException {
        // Not required for this task
    }

    /**
     * Checks if a given journal entry is expired based on the specified retention period.
     *
     * @param keyValue The KeyValue object representing the journal entry to be checked.
     * @return The key of the journal entry if it is expired, otherwise null.
     */
    private byte[] checkEntry(KeyValue keyValue) {
        Entry entry = Entry.decode(ByteBuffer.wrap(keyValue.getValue()));
        if (now - entry.timestamp() >= retentionPeriod.toMillis()) {
            counter++;
            return keyValue.getKey();
        }
        return null;
    }

    /**
     * This method identifies the range of expired journal entries that need to be cleaned up
     * from a given subspace within a transaction.
     *
     * @param tr       The transaction context used to access and modify the database.
     * @param subspace The directory subspace representing the journal's event space.
     * @return A Range object specifying the byte range of keys to be cleaned up, or null if no expired entries are found.
     */
    private Range findCleanupRange(Transaction tr, DirectorySubspace subspace) {
        JournalMetadata metadata = new JournalMetadata(subspace);
        KeySelector begin = KeySelector.firstGreaterThan(metadata.getEventsSubspace().pack());
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(metadata.getEventsSubspace().pack()));

        byte[] rangeBegin = null;
        byte[] rangeEnd = null;
        for (KeyValue keyValue : tr.getRange(begin, end)) {
            byte[] key = checkEntry(keyValue);
            if (key == null) {
                continue;
            }

            if (rangeBegin == null) {
                // set the beginning key only one time.
                rangeBegin = key;
            }
            // We found an expired Journal entry, extend the range.
            rangeEnd = key;
        }

        if (rangeBegin != null) {
            return new Range(rangeBegin, rangeEnd);
        }

        return null;
    }

    @Override
    public void run() {
        try (Transaction tr = journal.database.createTransaction()) {
            List<String> journals = journal.listJournals(tr);
            for (String journalName : journals) {
                now = Instant.now().toEpochMilli();
                counter = 0;
                KronotopDirectoryNode directory =
                        KronotopDirectory.kronotop().cluster(journal.cluster).journals().journal(journalName);
                DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, directory.toList()).join();

                Range range = findCleanupRange(tr, subspace);
                if (range != null) {
                    tr.clear(range);
                    // Clears a range of keys in the database. The upper bound of the range is exclusive; that is,
                    // the key (if one exists) that is specified as the end of the range will NOT be cleared as part of
                    // this operation.
                    tr.clear(range.end);
                    LOGGER.debug("{} entries have been removed from {}", counter, journalName);
                }
            }
            tr.commit().join();
        }
    }
}
