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

package com.kronotop.metrics;

import com.kronotop.transaction.TransactionMetrics;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AggregatedTransactionMetricsTest {

    @Test
    void shouldStartAtZero() {
        // Behavior: a new instance reports zero for every counter
        AggregatedTransactionMetrics metrics = new AggregatedTransactionMetrics();

        assertEquals(0, metrics.getCreated());
        assertEquals(0, metrics.getCommitted());
        assertEquals(0, metrics.getConflicted());
        assertEquals(0, metrics.getCommitFailed());
        assertEquals(0, metrics.getRolledBack());
        assertEquals(0, metrics.getReads());
        assertEquals(0, metrics.getRangeReads());
        assertEquals(0, metrics.getWrites());
        assertEquals(0, metrics.getDeletes());
        assertEquals(0, metrics.getRangeDeletes());
        assertEquals(0, metrics.getMutations());
        assertEquals(0, metrics.getBytesRead());
        assertEquals(0, metrics.getBytesWritten());
        assertEquals(0, metrics.getDurationNanos());
        assertEquals(0, metrics.getCommitLatencyNanos());
        assertEquals(0, metrics.getMaxCommitLatencyNanos());
    }

    @Test
    void shouldCountLifecycleEvents() {
        // Behavior: every increment adds one to its own counter only
        AggregatedTransactionMetrics metrics = new AggregatedTransactionMetrics();

        metrics.incrementCreated();
        metrics.incrementCreated();
        metrics.incrementCommitted();
        metrics.incrementConflicted();
        metrics.incrementCommitFailed();
        metrics.incrementRolledBack();

        assertEquals(2, metrics.getCreated());
        assertEquals(1, metrics.getCommitted());
        assertEquals(1, metrics.getConflicted());
        assertEquals(1, metrics.getCommitFailed());
        assertEquals(1, metrics.getRolledBack());
    }

    @Test
    void shouldFoldTransactionMetrics() {
        // Behavior: record() adds every per-transaction value to the totals
        AggregatedTransactionMetrics metrics = new AggregatedTransactionMetrics();

        TransactionMetrics tx = new TransactionMetrics();
        tx.incrementReads();
        tx.incrementReads();
        tx.incrementRangeReads();
        tx.incrementWrites();
        tx.incrementDeletes();
        tx.incrementRangeDeletes();
        tx.incrementMutations();
        tx.addBytesRead(10);
        tx.addBytesWritten(20);
        tx.recordTotalDuration();

        metrics.record(tx);
        metrics.record(tx);

        assertEquals(4, metrics.getReads());
        assertEquals(2, metrics.getRangeReads());
        assertEquals(2, metrics.getWrites());
        assertEquals(2, metrics.getDeletes());
        assertEquals(2, metrics.getRangeDeletes());
        assertEquals(2, metrics.getMutations());
        assertEquals(20, metrics.getBytesRead());
        assertEquals(40, metrics.getBytesWritten());
        assertEquals(2 * tx.getTotalDurationNanos(), metrics.getDurationNanos());
    }

    @Test
    void shouldSumAndTrackMaxCommitLatency() {
        // Behavior: commit latency is summed; the max keeps the largest value seen
        AggregatedTransactionMetrics metrics = new AggregatedTransactionMetrics();

        metrics.recordCommitLatency(300);
        metrics.recordCommitLatency(100);
        metrics.recordCommitLatency(200);

        assertEquals(600, metrics.getCommitLatencyNanos());
        assertEquals(300, metrics.getMaxCommitLatencyNanos());
    }
}
