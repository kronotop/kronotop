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

import com.apple.foundationdb.*;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Wraps an FDB {@link Transaction} to collect operation counts, byte volumes,
 * commit latency, and total duration into a {@link TransactionMetrics} instance.
 */
public class InstrumentedTransaction extends InstrumentedReadTransaction<Transaction> implements Transaction {

    private ReadTransaction snapshot;

    public InstrumentedTransaction(Transaction underlying) {
        super(underlying, new TransactionMetrics());
    }

    public InstrumentedTransaction(Transaction underlying, TransactionMetrics metrics) {
        super(underlying, metrics);
    }

    public TransactionMetrics getMetrics() {
        return metrics;
    }

    // --- Instrumented write operations ---

    @Override
    public void set(byte[] key, byte[] value) {
        underlying.set(key, value);
        metrics.incrementWrites();
        metrics.addBytesWritten(key.length + value.length);
    }

    @Override
    public void clear(byte[] key) {
        underlying.clear(key);
        metrics.incrementDeletes();
    }

    @Override
    public void clear(byte[] keyBegin, byte[] keyEnd) {
        underlying.clear(keyBegin, keyEnd);
        metrics.incrementDeletes();
        metrics.incrementRangeDeletes();
    }

    @Override
    public void clear(Range range) {
        underlying.clear(range);
        metrics.incrementDeletes();
        metrics.incrementRangeDeletes();
    }

    @SuppressWarnings("deprecation")
    @Override
    public void clearRangeStartsWith(byte[] prefix) {
        underlying.clearRangeStartsWith(prefix);
        metrics.incrementDeletes();
        metrics.incrementRangeDeletes();
    }

    @Override
    public void mutate(MutationType opType, byte[] key, byte[] param) {
        underlying.mutate(opType, key, param);
        metrics.incrementMutations();
    }

    // --- Instrumented lifecycle ---

    @Override
    public CompletableFuture<Void> commit() {
        long commitStartNanos = System.nanoTime();
        return underlying.commit().whenComplete((v, ex) -> {
            metrics.recordCommitLatency(System.nanoTime() - commitStartNanos);
            if (ex == null) {
                metrics.markCommitted();
            } else {
                Throwable cause = ex;
                while (cause != null && !(cause instanceof FDBException)) {
                    cause = cause.getCause();
                }
                if (cause instanceof FDBException fdb && fdb.getCode() == 1020) {
                    metrics.markConflicted();
                }
            }
        });
    }

    @Override
    public void close() {
        underlying.close();
        metrics.recordTotalDuration();
    }

    // --- Snapshot ---

    @Override
    public ReadTransaction snapshot() {
        if (snapshot == null) {
            snapshot = new InstrumentedSnapshot(underlying.snapshot(), metrics);
        }
        return snapshot;
    }

    // --- Transaction-specific delegation ---

    @Override
    public void addReadConflictRange(byte[] keyBegin, byte[] keyEnd) {
        underlying.addReadConflictRange(keyBegin, keyEnd);
    }

    @Override
    public void addReadConflictKey(byte[] key) {
        underlying.addReadConflictKey(key);
    }

    @Override
    public void addWriteConflictRange(byte[] keyBegin, byte[] keyEnd) {
        underlying.addWriteConflictRange(keyBegin, keyEnd);
    }

    @Override
    public void addWriteConflictKey(byte[] key) {
        underlying.addWriteConflictKey(key);
    }

    @Override
    public Long getCommittedVersion() {
        return underlying.getCommittedVersion();
    }

    @Override
    public CompletableFuture<byte[]> getVersionstamp() {
        return underlying.getVersionstamp();
    }

    @Override
    public CompletableFuture<Long> getApproximateSize() {
        return underlying.getApproximateSize();
    }

    @Override
    public CompletableFuture<Transaction> onError(Throwable throwable) {
        return underlying.onError(throwable);
    }

    @Override
    public void cancel() {
        underlying.cancel();
    }

    @Override
    public CompletableFuture<Void> watch(byte[] key) throws FDBException {
        return underlying.watch(key);
    }

    @Override
    public Database getDatabase() {
        return underlying.getDatabase();
    }

    @Override
    public <T> T run(Function<? super Transaction, T> function) {
        return function.apply(this);
    }

    @Override
    public <T> CompletableFuture<T> runAsync(Function<? super Transaction, ? extends CompletableFuture<T>> function) {
        return function.apply(this);
    }

    // --- Inner class ---

    private static class InstrumentedSnapshot extends InstrumentedReadTransaction<ReadTransaction> implements ReadTransaction {

        InstrumentedSnapshot(ReadTransaction underlying, TransactionMetrics metrics) {
            super(underlying, metrics);
        }

        @Override
        public boolean isSnapshot() {
            return true;
        }

        @Override
        public ReadTransaction snapshot() {
            return this;
        }
    }
}
