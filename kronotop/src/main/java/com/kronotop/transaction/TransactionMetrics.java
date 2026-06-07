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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe counters for tracking FDB transaction lifecycle metrics:
 * operation counts, byte volumes, timing, and commit status.
 */
public class TransactionMetrics {

    // Operation counters
    private final AtomicLong reads = new AtomicLong();
    private final AtomicLong rangeReads = new AtomicLong();
    private final AtomicLong writes = new AtomicLong();
    private final AtomicLong deletes = new AtomicLong();
    private final AtomicLong rangeDeletes = new AtomicLong();
    private final AtomicLong mutations = new AtomicLong();

    // Byte counters
    private final AtomicLong bytesRead = new AtomicLong();
    private final AtomicLong bytesWritten = new AtomicLong();

    // Timing
    private final long startNanos;
    private volatile long commitLatencyNanos;
    private volatile long totalDurationNanos;

    // Status
    private volatile boolean committed;
    private volatile boolean conflicted;

    public TransactionMetrics() {
        this.startNanos = System.nanoTime();
    }

    TransactionMetrics(long startNanos) {
        this.startNanos = startNanos;
    }

    // --- Increment methods ---

    public void incrementReads() {
        reads.incrementAndGet();
    }

    public void incrementRangeReads() {
        rangeReads.incrementAndGet();
    }

    public void incrementWrites() {
        writes.incrementAndGet();
    }

    public void incrementDeletes() {
        deletes.incrementAndGet();
    }

    public void incrementRangeDeletes() {
        rangeDeletes.incrementAndGet();
    }

    public void incrementMutations() {
        mutations.incrementAndGet();
    }

    public void addBytesRead(long bytes) {
        bytesRead.addAndGet(bytes);
    }

    public void addBytesWritten(long bytes) {
        bytesWritten.addAndGet(bytes);
    }

    // --- Timing ---

    public void recordCommitLatency(long nanos) {
        this.commitLatencyNanos = nanos;
    }

    public void recordTotalDuration() {
        this.totalDurationNanos = System.nanoTime() - startNanos;
    }

    // --- Status ---

    public void markCommitted() {
        this.committed = true;
    }

    public void markConflicted() {
        this.conflicted = true;
    }

    // --- Getters ---

    public long getReads() {
        return reads.get();
    }

    public long getRangeReads() {
        return rangeReads.get();
    }

    public long getWrites() {
        return writes.get();
    }

    public long getDeletes() {
        return deletes.get();
    }

    public long getRangeDeletes() {
        return rangeDeletes.get();
    }

    public long getMutations() {
        return mutations.get();
    }

    public long getBytesRead() {
        return bytesRead.get();
    }

    public long getBytesWritten() {
        return bytesWritten.get();
    }

    public long getStartNanos() {
        return startNanos;
    }

    public long getCommitLatencyNanos() {
        return commitLatencyNanos;
    }

    public long getTotalDurationNanos() {
        return totalDurationNanos;
    }

    public boolean isCommitted() {
        return committed;
    }

    public boolean isConflicted() {
        return conflicted;
    }

    @Override
    public String toString() {
        return "TransactionMetrics{" +
                "reads=" + reads.get() +
                ", rangeReads=" + rangeReads.get() +
                ", writes=" + writes.get() +
                ", deletes=" + deletes.get() +
                ", rangeDeletes=" + rangeDeletes.get() +
                ", mutations=" + mutations.get() +
                ", bytesRead=" + bytesRead.get() +
                ", bytesWritten=" + bytesWritten.get() +
                ", commitLatencyNanos=" + commitLatencyNanos +
                ", totalDurationNanos=" + totalDurationNanos +
                ", committed=" + committed +
                ", conflicted=" + conflicted +
                '}';
    }
}
