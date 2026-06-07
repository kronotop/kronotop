/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.transaction;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InstrumentedTransactionTest extends BaseStandaloneInstanceTest {

    private static final byte[] TEST_KEY = Tuple.from("test", "instrumented", "key1").pack();
    private static final byte[] TEST_VALUE = "hello-world".getBytes(StandardCharsets.UTF_8);

    private InstrumentedTransaction createInstrumentedTransaction() {
        Transaction tr = context.getFoundationDB().createTransaction();
        return new InstrumentedTransaction(tr);
    }

    @Test
    void shouldCountSetOperation() {
        // Behavior: A set() call increments the writes counter by 1.
        try (InstrumentedTransaction tr = createInstrumentedTransaction()) {
            tr.set(TEST_KEY, TEST_VALUE);
            tr.commit().join();

            TransactionMetrics metrics = tr.getMetrics();
            assertEquals(1, metrics.getWrites());
        }
    }

    @Test
    void shouldTrackBytesWrittenOnSet() {
        // Behavior: A set() call records key.length + value.length as bytes written.
        try (InstrumentedTransaction tr = createInstrumentedTransaction()) {
            tr.set(TEST_KEY, TEST_VALUE);
            tr.commit().join();

            TransactionMetrics metrics = tr.getMetrics();
            assertEquals(TEST_KEY.length + TEST_VALUE.length, metrics.getBytesWritten());
        }
    }

    @Test
    void shouldCountGetOperation() {
        // Behavior: A get() call increments the read counter by 1.
        try (InstrumentedTransaction setupTr = createInstrumentedTransaction()) {
            setupTr.set(TEST_KEY, TEST_VALUE);
            setupTr.commit().join();
        }

        try (InstrumentedTransaction tr = createInstrumentedTransaction()) {
            byte[] result = tr.get(TEST_KEY).join();

            TransactionMetrics metrics = tr.getMetrics();
            assertEquals(1, metrics.getReads());
            assertNotNull(result);
        }
    }

    @Test
    void shouldTrackBytesReadOnGet() {
        // Behavior: A get() that returns a value records value.length as bytes read.
        try (InstrumentedTransaction setupTr = createInstrumentedTransaction()) {
            setupTr.set(TEST_KEY, TEST_VALUE);
            setupTr.commit().join();
        }

        try (InstrumentedTransaction tr = createInstrumentedTransaction()) {
            tr.get(TEST_KEY).join();

            TransactionMetrics metrics = tr.getMetrics();
            assertEquals(TEST_VALUE.length, metrics.getBytesRead());
        }
    }

    @Test
    void shouldNotCountBytesReadOnMissingKey() {
        // Behavior: A get() that returns null does not add to bytes read.
        byte[] missingKey = Tuple.from("test", "instrumented", "nonexistent").pack();

        try (InstrumentedTransaction tr = createInstrumentedTransaction()) {
            byte[] result = tr.get(missingKey).join();

            assertNull(result);
            assertEquals(0, tr.getMetrics().getBytesRead());
        }
    }

    @Test
    void shouldCountClearOperation() {
        // Behavior: A clear(key) call increments the deletes counter by 1.
        try (InstrumentedTransaction tr = createInstrumentedTransaction()) {
            tr.set(TEST_KEY, TEST_VALUE);
            tr.clear(TEST_KEY);
            tr.commit().join();

            assertEquals(1, tr.getMetrics().getDeletes());
            assertEquals(0, tr.getMetrics().getRangeDeletes());
        }
    }

    @Test
    void shouldCountRangeClear() {
        // Behavior: A clear(begin, end) call increments both deletes and rangeDeletes counters.
        byte[] begin = Tuple.from("test", "instrumented", "range-a").pack();
        byte[] end = Tuple.from("test", "instrumented", "range-z").pack();

        try (InstrumentedTransaction tr = createInstrumentedTransaction()) {
            tr.clear(begin, end);
            tr.commit().join();

            assertEquals(1, tr.getMetrics().getDeletes());
            assertEquals(1, tr.getMetrics().getRangeDeletes());
        }
    }

    @Test
    void shouldCountMutateOperation() {
        // Behavior: A mutate() call increments the mutations counter by 1.
        byte[] mutationKey = Tuple.from("test", "instrumented", "mutation-key").pack();

        try (InstrumentedTransaction tr = createInstrumentedTransaction()) {
            tr.set(mutationKey, Tuple.from(0L).pack());
            tr.mutate(MutationType.ADD, mutationKey, Tuple.from(5L).pack());
            tr.commit().join();

            assertEquals(1, tr.getMetrics().getMutations());
        }
    }

    @Test
    void shouldCountRangeReadOnGetRange() {
        // Behavior: Iterating a getRange result increments the range reads counter.
        try (InstrumentedTransaction setupTr = createInstrumentedTransaction()) {
            for (int i = 0; i < 3; i++) {
                byte[] key = Tuple.from("test", "instrumented", "range-read", i).pack();
                setupTr.set(key, TEST_VALUE);
            }
            setupTr.commit().join();
        }

        try (InstrumentedTransaction tr = createInstrumentedTransaction()) {
            byte[] begin = Tuple.from("test", "instrumented", "range-read").range().begin;
            byte[] end = Tuple.from("test", "instrumented", "range-read").range().end;
            List<KeyValue> results = tr.getRange(begin, end).asList().join();

            assertEquals(3, results.size());
            assertEquals(1, tr.getMetrics().getRangeReads());
        }
    }

    @Test
    void shouldTrackBytesReadOnGetRange() {
        // Behavior: asList() on a getRange result accumulates key+value bytes for all returned KVs.
        byte[] key = Tuple.from("test", "instrumented", "range-bytes", 0).pack();

        try (InstrumentedTransaction setupTr = createInstrumentedTransaction()) {
            setupTr.set(key, TEST_VALUE);
            setupTr.commit().join();
        }

        try (InstrumentedTransaction tr = createInstrumentedTransaction()) {
            byte[] begin = Tuple.from("test", "instrumented", "range-bytes").range().begin;
            byte[] end = Tuple.from("test", "instrumented", "range-bytes").range().end;
            List<KeyValue> results = tr.getRange(begin, end).asList().join();

            assertEquals(1, results.size());
            long expectedBytes = key.length + TEST_VALUE.length;
            assertEquals(expectedBytes, tr.getMetrics().getBytesRead());
        }
    }

    @Test
    void shouldRecordCommitLatencyOnCommit() {
        // Behavior: After a successful commit, commit latency is recorded as a positive nanosecond value.
        try (InstrumentedTransaction tr = createInstrumentedTransaction()) {
            tr.set(TEST_KEY, TEST_VALUE);
            tr.commit().join();

            assertTrue(tr.getMetrics().getCommitLatencyNanos() > 0);
        }
    }

    @Test
    void shouldRecordTotalDurationOnClose() {
        // Behavior: After close(), total duration is recorded as a positive nanosecond value.
        InstrumentedTransaction tr = createInstrumentedTransaction();
        tr.set(TEST_KEY, TEST_VALUE);
        tr.commit().join();
        tr.close();

        assertTrue(tr.getMetrics().getTotalDurationNanos() > 0);
    }

    @Test
    void shouldMarkCommittedOnSuccessfulCommit() {
        // Behavior: A successful commit sets the committed flag to true.
        try (InstrumentedTransaction tr = createInstrumentedTransaction()) {
            tr.set(TEST_KEY, TEST_VALUE);
            tr.commit().join();

            assertTrue(tr.getMetrics().isCommitted());
            assertFalse(tr.getMetrics().isConflicted());
        }
    }

    @Test
    void shouldDelegateSnapshotWithSharedMetrics() {
        // Behavior: snapshot() returns a ReadTransaction that shares the same TransactionMetrics instance.
        try (InstrumentedTransaction setupTr = createInstrumentedTransaction()) {
            setupTr.set(TEST_KEY, TEST_VALUE);
            setupTr.commit().join();
        }

        try (InstrumentedTransaction tr = createInstrumentedTransaction()) {
            ReadTransaction snap = tr.snapshot();
            snap.get(TEST_KEY).join();

            // The snapshot read should have been counted in the shared metrics
            assertEquals(1, tr.getMetrics().getReads());
            assertTrue(tr.getMetrics().getBytesRead() > 0);
        }
    }

    @Test
    void shouldDelegateGetReadVersion() {
        // Behavior: getReadVersion() delegates to the underlying transaction and returns a valid version.
        try (InstrumentedTransaction tr = createInstrumentedTransaction()) {
            long readVersion = tr.getReadVersion().join();
            assertTrue(readVersion > 0);
        }
    }

    @Test
    void shouldDelegateOptionsToUnderlying() {
        // Behavior: options() returns the underlying transaction's options object (not null).
        try (InstrumentedTransaction tr = createInstrumentedTransaction()) {
            assertNotNull(tr.options());
        }
    }

    @Test
    void shouldAccumulateMultipleOperations() {
        // Behavior: Multiple operations within one transaction accumulate in the same metrics instance.
        byte[] key1 = Tuple.from("test", "instrumented", "multi", "a").pack();
        byte[] key2 = Tuple.from("test", "instrumented", "multi", "b").pack();
        byte[] val1 = "value1".getBytes(StandardCharsets.UTF_8);
        byte[] val2 = "value2".getBytes(StandardCharsets.UTF_8);

        try (InstrumentedTransaction tr = createInstrumentedTransaction()) {
            tr.set(key1, val1);
            tr.set(key2, val2);
            tr.get(key1).join();
            tr.clear(key2);
            tr.commit().join();

            TransactionMetrics m = tr.getMetrics();
            assertEquals(2, m.getWrites());
            assertEquals(1, m.getReads());
            assertEquals(1, m.getDeletes());
            long expectedWritten = (key1.length + val1.length) + (key2.length + val2.length);
            assertEquals(expectedWritten, m.getBytesWritten());
            // bytesRead = val1.length (read-your-own-write)
            assertEquals(val1.length, m.getBytesRead());
        }
    }
}
