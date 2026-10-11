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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Instance-wide totals of FDB transaction activity. Lifecycle counters are updated
 * when a transaction is created, committed, or cancelled. Operation totals are folded
 * in once from the per-transaction {@link TransactionMetrics} when the transaction closes.
 */
public class AggregatedTransactionMetrics {
    // Lifecycle
    private final LongAdder created = new LongAdder();
    private final LongAdder committed = new LongAdder();
    private final LongAdder conflicted = new LongAdder();
    private final LongAdder commitFailed = new LongAdder();
    private final LongAdder rolledBack = new LongAdder();

    // Operation totals
    private final LongAdder reads = new LongAdder();
    private final LongAdder rangeReads = new LongAdder();
    private final LongAdder writes = new LongAdder();
    private final LongAdder deletes = new LongAdder();
    private final LongAdder rangeDeletes = new LongAdder();
    private final LongAdder mutations = new LongAdder();
    private final LongAdder bytesRead = new LongAdder();
    private final LongAdder bytesWritten = new LongAdder();
    private final LongAdder durationNanos = new LongAdder();

    // Commit latency
    private final LongAdder commitLatencyNanos = new LongAdder();
    private final AtomicLong maxCommitLatencyNanos = new AtomicLong();

    public void incrementCreated() {
        created.increment();
    }

    public void incrementCommitted() {
        committed.increment();
    }

    public void incrementConflicted() {
        conflicted.increment();
    }

    public void incrementCommitFailed() {
        commitFailed.increment();
    }

    public void incrementRolledBack() {
        rolledBack.increment();
    }

    public void recordCommitLatency(long nanos) {
        commitLatencyNanos.add(nanos);
        maxCommitLatencyNanos.accumulateAndGet(nanos, Math::max);
    }

    /**
     * Adds the totals of one closed transaction. Call it exactly once per transaction.
     */
    public void record(TransactionMetrics metrics) {
        reads.add(metrics.getReads());
        rangeReads.add(metrics.getRangeReads());
        writes.add(metrics.getWrites());
        deletes.add(metrics.getDeletes());
        rangeDeletes.add(metrics.getRangeDeletes());
        mutations.add(metrics.getMutations());
        bytesRead.add(metrics.getBytesRead());
        bytesWritten.add(metrics.getBytesWritten());
        durationNanos.add(metrics.getTotalDurationNanos());
    }

    public long getCreated() {
        return created.sum();
    }

    public long getCommitted() {
        return committed.sum();
    }

    public long getConflicted() {
        return conflicted.sum();
    }

    public long getCommitFailed() {
        return commitFailed.sum();
    }

    public long getRolledBack() {
        return rolledBack.sum();
    }

    public long getReads() {
        return reads.sum();
    }

    public long getRangeReads() {
        return rangeReads.sum();
    }

    public long getWrites() {
        return writes.sum();
    }

    public long getDeletes() {
        return deletes.sum();
    }

    public long getRangeDeletes() {
        return rangeDeletes.sum();
    }

    public long getMutations() {
        return mutations.sum();
    }

    public long getBytesRead() {
        return bytesRead.sum();
    }

    public long getBytesWritten() {
        return bytesWritten.sum();
    }

    public long getDurationNanos() {
        return durationNanos.sum();
    }

    public long getCommitLatencyNanos() {
        return commitLatencyNanos.sum();
    }

    public long getMaxCommitLatencyNanos() {
        return maxCommitLatencyNanos.get();
    }
}
