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
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.metrics.AggregatedTransactionMetrics;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Wraps an FDB {@link Transaction} to collect operation counts, byte volumes,
 * commit latency, and total duration into a {@link TransactionMetrics} instance.
 */
public class InstrumentedTransaction extends InstrumentedReadTransaction<Transaction> implements Transaction {

    /**
     * Client id of a transaction that no session owns.
     */
    public static final long INTERNAL = -1;

    private final TransactionManager manager;
    private final AggregatedTransactionMetrics aggregated;
    private ReadTransaction snapshot;
    private volatile long clientId = INTERNAL;
    private volatile TransactionState state = TransactionState.ACTIVE;
    // Metadata of the command that runs on this transaction, shown by TRANSACTIONS.
    private volatile String command;
    private volatile String model;
    private volatile String namespace;
    private volatile String bucket;
    private volatile BqlExpr query;

    InstrumentedTransaction(long id, Transaction underlying, TransactionManager manager) {
        super(id, underlying, new TransactionMetrics());
        this.manager = manager;
        this.aggregated = manager.getAggregatedTransactionMetrics();
    }

    public long getClientId() {
        return clientId;
    }

    public void setClientId(long clientId) {
        this.clientId = clientId;
    }

    public TransactionState getState() {
        return state;
    }

    public void setState(TransactionState state) {
        this.state = state;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public BqlExpr getQuery() {
        return query;
    }

    public void setQuery(BqlExpr query) {
        this.query = query;
    }

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

    @Override
    public CompletableFuture<Void> commit() {
        long commitStartNanos = System.nanoTime();
        return underlying.commit().whenComplete((ignored, ex) -> {
            long latency = System.nanoTime() - commitStartNanos;
            metrics.recordCommitLatency(latency);
            aggregated.recordCommitLatency(latency);
            if (ex == null) {
                metrics.markCommitted();
                aggregated.incrementCommitted();
            } else {
                Throwable cause = ex;
                while (cause != null && !(cause instanceof FDBException)) {
                    cause = cause.getCause();
                }
                if (cause instanceof FDBException fdb && fdb.getCode() == 1020) {
                    metrics.markConflicted();
                    aggregated.incrementConflicted();
                } else {
                    aggregated.incrementCommitFailed();
                }
            }
        });
    }

    @Override
    public void close() {
        // Duration must be final before release() folds it into the aggregated totals.
        metrics.recordTotalDuration();
        manager.release(this);
        underlying.close();
    }

    @Override
    public ReadTransaction snapshot() {
        if (snapshot == null) {
            snapshot = new InstrumentedSnapshot(getId(), underlying.snapshot(), metrics);
        }
        return snapshot;
    }

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
        aggregated.incrementRolledBack();
    }

    @Override
    public CompletableFuture<Void> watch(byte[] key) throws FDBException {
        metrics.markWatching();
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

    private static class InstrumentedSnapshot extends InstrumentedReadTransaction<ReadTransaction> implements ReadTransaction {

        InstrumentedSnapshot(long id, ReadTransaction underlying, TransactionMetrics metrics) {
            super(id, underlying, metrics);
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
