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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TransactionMetricsTest {

    @Test
    void shouldStartWithZeroCounters() {
        // Behavior: A fresh TransactionMetrics instance has all counters at zero and status flags false.
        TransactionMetrics metrics = new TransactionMetrics();

        assertEquals(0, metrics.getReads());
        assertEquals(0, metrics.getRangeReads());
        assertEquals(0, metrics.getWrites());
        assertEquals(0, metrics.getDeletes());
        assertEquals(0, metrics.getRangeDeletes());
        assertEquals(0, metrics.getMutations());
        assertEquals(0, metrics.getBytesRead());
        assertEquals(0, metrics.getBytesWritten());
        assertEquals(0, metrics.getCommitLatencyNanos());
        assertEquals(0, metrics.getTotalDurationNanos());
        assertFalse(metrics.isCommitted());
        assertFalse(metrics.isConflicted());
    }

    @Test
    void shouldIncrementReads() {
        // Behavior: Each incrementReads call increases the read counter by 1.
        TransactionMetrics metrics = new TransactionMetrics();

        metrics.incrementReads();
        metrics.incrementReads();
        metrics.incrementReads();

        assertEquals(3, metrics.getReads());
    }

    @Test
    void shouldIncrementRangeReads() {
        // Behavior: Each incrementRangeReads call increases the range read counter by 1.
        TransactionMetrics metrics = new TransactionMetrics();

        metrics.incrementRangeReads();
        metrics.incrementRangeReads();

        assertEquals(2, metrics.getRangeReads());
    }

    @Test
    void shouldIncrementWrites() {
        // Behavior: Each incrementWrites call increases the write counter by 1.
        TransactionMetrics metrics = new TransactionMetrics();

        metrics.incrementWrites();

        assertEquals(1, metrics.getWrites());
    }

    @Test
    void shouldIncrementDeletes() {
        // Behavior: Each incrementDeletes call increases the delete counter by 1.
        TransactionMetrics metrics = new TransactionMetrics();

        metrics.incrementDeletes();
        metrics.incrementDeletes();

        assertEquals(2, metrics.getDeletes());
    }

    @Test
    void shouldIncrementRangeDeletes() {
        // Behavior: Each incrementRangeDeletes call increases the range delete counter by 1.
        TransactionMetrics metrics = new TransactionMetrics();

        metrics.incrementRangeDeletes();

        assertEquals(1, metrics.getRangeDeletes());
    }

    @Test
    void shouldIncrementMutations() {
        // Behavior: Each incrementMutations call increases the mutation counter by 1.
        TransactionMetrics metrics = new TransactionMetrics();

        metrics.incrementMutations();
        metrics.incrementMutations();
        metrics.incrementMutations();

        assertEquals(3, metrics.getMutations());
    }

    @Test
    void shouldAddBytesRead() {
        // Behavior: addBytesRead accumulates the total bytes read.
        TransactionMetrics metrics = new TransactionMetrics();

        metrics.addBytesRead(100);
        metrics.addBytesRead(250);

        assertEquals(350, metrics.getBytesRead());
    }

    @Test
    void shouldAddBytesWritten() {
        // Behavior: addBytesWritten accumulates the total bytes written.
        TransactionMetrics metrics = new TransactionMetrics();

        metrics.addBytesWritten(512);
        metrics.addBytesWritten(1024);

        assertEquals(1536, metrics.getBytesWritten());
    }

    @Test
    void shouldRecordCommitLatency() {
        // Behavior: recordCommitLatency stores the provided nanosecond value.
        TransactionMetrics metrics = new TransactionMetrics();

        metrics.recordCommitLatency(5_000_000L);

        assertEquals(5_000_000L, metrics.getCommitLatencyNanos());
    }

    @Test
    void shouldRecordTotalDuration() {
        // Behavior: recordTotalDuration computes elapsed time since construction.
        TransactionMetrics metrics = new TransactionMetrics();

        // Let some time pass
        long before = System.nanoTime();
        metrics.recordTotalDuration();
        long after = System.nanoTime();

        assertTrue(metrics.getTotalDurationNanos() > 0);
        assertTrue(metrics.getTotalDurationNanos() <= after - metrics.getStartNanos());
    }

    @Test
    void shouldMarkCommitted() {
        // Behavior: markCommitted sets the committed flag to true.
        TransactionMetrics metrics = new TransactionMetrics();

        assertFalse(metrics.isCommitted());
        metrics.markCommitted();
        assertTrue(metrics.isCommitted());
    }

    @Test
    void shouldMarkConflicted() {
        // Behavior: markConflicted sets the conflicted flag to true.
        TransactionMetrics metrics = new TransactionMetrics();

        assertFalse(metrics.isConflicted());
        metrics.markConflicted();
        assertTrue(metrics.isConflicted());
    }

    @Test
    void shouldProduceReadableToString() {
        // Behavior: toString returns a human-readable summary containing all metric names.
        TransactionMetrics metrics = new TransactionMetrics();
        metrics.incrementReads();
        metrics.incrementWrites();
        metrics.addBytesRead(42);

        String result = metrics.toString();

        assertTrue(result.contains("reads=1"));
        assertTrue(result.contains("writes=1"));
        assertTrue(result.contains("bytesRead=42"));
        assertTrue(result.contains("committed=false"));
    }
}
