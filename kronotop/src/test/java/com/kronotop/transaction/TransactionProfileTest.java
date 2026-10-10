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

class TransactionProfileTest {

    @Test
    void shouldStartWithZeroCounters() {
        // Behavior: A fresh TransactionProfile instance has all counters at zero and status flags false.
        TransactionMetrics profile = new TransactionMetrics();

        assertEquals(0, profile.getReads());
        assertEquals(0, profile.getRangeReads());
        assertEquals(0, profile.getWrites());
        assertEquals(0, profile.getDeletes());
        assertEquals(0, profile.getRangeDeletes());
        assertEquals(0, profile.getMutations());
        assertEquals(0, profile.getBytesRead());
        assertEquals(0, profile.getBytesWritten());
        assertEquals(0, profile.getCommitLatencyNanos());
        assertEquals(0, profile.getTotalDurationNanos());
        assertFalse(profile.isCommitted());
        assertFalse(profile.isConflicted());
    }

    @Test
    void shouldIncrementReads() {
        // Behavior: Each incrementReads call increases the read counter by 1.
        TransactionMetrics profile = new TransactionMetrics();

        profile.incrementReads();
        profile.incrementReads();
        profile.incrementReads();

        assertEquals(3, profile.getReads());
    }

    @Test
    void shouldIncrementRangeReads() {
        // Behavior: Each incrementRangeReads call increases the range read counter by 1.
        TransactionMetrics profile = new TransactionMetrics();

        profile.incrementRangeReads();
        profile.incrementRangeReads();

        assertEquals(2, profile.getRangeReads());
    }

    @Test
    void shouldIncrementWrites() {
        // Behavior: Each incrementWrites call increases the write counter by 1.
        TransactionMetrics profile = new TransactionMetrics();

        profile.incrementWrites();

        assertEquals(1, profile.getWrites());
    }

    @Test
    void shouldIncrementDeletes() {
        // Behavior: Each incrementDeletes call increases the delete counter by 1.
        TransactionMetrics profile = new TransactionMetrics();

        profile.incrementDeletes();
        profile.incrementDeletes();

        assertEquals(2, profile.getDeletes());
    }

    @Test
    void shouldIncrementRangeDeletes() {
        // Behavior: Each incrementRangeDeletes call increases the range delete counter by 1.
        TransactionMetrics profile = new TransactionMetrics();

        profile.incrementRangeDeletes();

        assertEquals(1, profile.getRangeDeletes());
    }

    @Test
    void shouldIncrementMutations() {
        // Behavior: Each incrementMutations call increases the mutation counter by 1.
        TransactionMetrics profile = new TransactionMetrics();

        profile.incrementMutations();
        profile.incrementMutations();
        profile.incrementMutations();

        assertEquals(3, profile.getMutations());
    }

    @Test
    void shouldAddBytesRead() {
        // Behavior: addBytesRead accumulates the total bytes read.
        TransactionMetrics profile = new TransactionMetrics();

        profile.addBytesRead(100);
        profile.addBytesRead(250);

        assertEquals(350, profile.getBytesRead());
    }

    @Test
    void shouldAddBytesWritten() {
        // Behavior: addBytesWritten accumulates the total bytes written.
        TransactionMetrics profile = new TransactionMetrics();

        profile.addBytesWritten(512);
        profile.addBytesWritten(1024);

        assertEquals(1536, profile.getBytesWritten());
    }

    @Test
    void shouldRecordCommitLatency() {
        // Behavior: recordCommitLatency stores the provided nanosecond value.
        TransactionMetrics profile = new TransactionMetrics();

        profile.recordCommitLatency(5_000_000L);

        assertEquals(5_000_000L, profile.getCommitLatencyNanos());
    }

    @Test
    void shouldRecordTotalDuration() {
        // Behavior: recordTotalDuration computes elapsed time since construction.
        TransactionMetrics profile = new TransactionMetrics();

        // Let some time pass
        long before = System.nanoTime();
        profile.recordTotalDuration();
        long after = System.nanoTime();

        assertTrue(profile.getTotalDurationNanos() > 0);
        assertTrue(profile.getTotalDurationNanos() <= after - profile.getStartNanos());
    }

    @Test
    void shouldMarkCommitted() {
        // Behavior: markCommitted sets the committed flag to true.
        TransactionMetrics profile = new TransactionMetrics();

        assertFalse(profile.isCommitted());
        profile.markCommitted();
        assertTrue(profile.isCommitted());
    }

    @Test
    void shouldMarkConflicted() {
        // Behavior: markConflicted sets the conflicted flag to true.
        TransactionMetrics profile = new TransactionMetrics();

        assertFalse(profile.isConflicted());
        profile.markConflicted();
        assertTrue(profile.isConflicted());
    }

    @Test
    void shouldProduceReadableToString() {
        // Behavior: toString returns a human-readable summary containing all metric names.
        TransactionMetrics profile = new TransactionMetrics();
        profile.incrementReads();
        profile.incrementWrites();
        profile.addBytesRead(42);

        String result = profile.toString();

        assertTrue(result.contains("reads=1"));
        assertTrue(result.contains("writes=1"));
        assertTrue(result.contains("bytesRead=42"));
        assertTrue(result.contains("committed=false"));
    }
}
