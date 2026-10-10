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
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import org.jspecify.annotations.NonNull;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Abstract base that instruments read operations on an FDB {@link ReadTransaction},
 * tracking operation counts and bytes read into a shared {@link TransactionMetrics}.
 *
 * @param <T> the concrete transaction type being wrapped
 */
abstract class InstrumentedReadTransaction<T extends ReadTransaction> implements ReadTransaction {

    protected final T underlying;
    protected final TransactionMetrics metrics;

    protected InstrumentedReadTransaction(T underlying, TransactionMetrics metrics) {
        this.underlying = underlying;
        this.metrics = metrics;
    }

    // --- Instrumented read operations ---

    @Override
    public CompletableFuture<byte[]> get(byte[] key) {
        metrics.incrementReads();
        return underlying.get(key).thenApply(value -> {
            if (value != null) {
                metrics.addBytesRead(value.length);
            }
            return value;
        });
    }

    @Override
    public CompletableFuture<byte[]> getKey(KeySelector keySelector) {
        metrics.incrementReads();
        return underlying.getKey(keySelector).thenApply(value -> {
            if (value != null) {
                metrics.addBytesRead(value.length);
            }
            return value;
        });
    }

    // --- Instrumented range reads (KeySelector overloads) ---

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end) {
        return new ByteCountingAsyncIterable(underlying.getRange(begin, end), metrics);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit) {
        return new ByteCountingAsyncIterable(underlying.getRange(begin, end, limit), metrics);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, boolean reverse) {
        return new ByteCountingAsyncIterable(underlying.getRange(begin, end, limit, reverse), metrics);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, boolean reverse, StreamingMode mode) {
        return new ByteCountingAsyncIterable(underlying.getRange(begin, end, limit, reverse, mode), metrics);
    }

    // --- Instrumented range reads (byte[] overloads) ---

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end) {
        return new ByteCountingAsyncIterable(underlying.getRange(begin, end), metrics);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end, int limit) {
        return new ByteCountingAsyncIterable(underlying.getRange(begin, end, limit), metrics);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end, int limit, boolean reverse) {
        return new ByteCountingAsyncIterable(underlying.getRange(begin, end, limit, reverse), metrics);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end, int limit, boolean reverse, StreamingMode mode) {
        return new ByteCountingAsyncIterable(underlying.getRange(begin, end, limit, reverse, mode), metrics);
    }

    // --- Instrumented range reads (Range overloads) ---

    @Override
    public AsyncIterable<KeyValue> getRange(Range range) {
        return new ByteCountingAsyncIterable(underlying.getRange(range), metrics);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, int limit) {
        return new ByteCountingAsyncIterable(underlying.getRange(range, limit), metrics);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, int limit, boolean reverse) {
        return new ByteCountingAsyncIterable(underlying.getRange(range, limit, reverse), metrics);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, int limit, boolean reverse, StreamingMode mode) {
        return new ByteCountingAsyncIterable(underlying.getRange(range, limit, reverse, mode), metrics);
    }

    // --- Pure delegation ---

    @Override
    public AsyncIterable<MappedKeyValue> getMappedRange(KeySelector begin, KeySelector end, byte[] mapper, int limit, boolean reverse, StreamingMode mode) {
        return underlying.getMappedRange(begin, end, mapper, limit, reverse, mode);
    }

    @Override
    public CompletableFuture<Long> getEstimatedRangeSizeBytes(byte[] begin, byte[] end) {
        return underlying.getEstimatedRangeSizeBytes(begin, end);
    }

    @Override
    public CompletableFuture<Long> getEstimatedRangeSizeBytes(Range range) {
        return underlying.getEstimatedRangeSizeBytes(range);
    }

    @Override
    public CompletableFuture<KeyArrayResult> getRangeSplitPoints(byte[] begin, byte[] end, long chunkSize) {
        return underlying.getRangeSplitPoints(begin, end, chunkSize);
    }

    @Override
    public CompletableFuture<KeyArrayResult> getRangeSplitPoints(Range range, long chunkSize) {
        return underlying.getRangeSplitPoints(range, chunkSize);
    }

    @Override
    public CompletableFuture<KeyRangeArrayResult> getBlobGranuleRanges(byte[] begin, byte[] end, int rowLimit) {
        return underlying.getBlobGranuleRanges(begin, end, rowLimit);
    }

    @Override
    public boolean isSnapshot() {
        return underlying.isSnapshot();
    }

    @Override
    public CompletableFuture<Long> getReadVersion() {
        return underlying.getReadVersion();
    }

    @Override
    public void setReadVersion(long version) {
        underlying.setReadVersion(version);
    }

    @Override
    public boolean addReadConflictRangeIfNotSnapshot(byte[] keyBegin, byte[] keyEnd) {
        return underlying.addReadConflictRangeIfNotSnapshot(keyBegin, keyEnd);
    }

    @Override
    public boolean addReadConflictKeyIfNotSnapshot(byte[] key) {
        return underlying.addReadConflictKeyIfNotSnapshot(key);
    }

    @Override
    public TransactionOptions options() {
        return underlying.options();
    }

    @Override
    public <V> V read(Function<? super ReadTransaction, V> function) {
        return function.apply(this);
    }

    @Override
    public <V> CompletableFuture<V> readAsync(Function<? super ReadTransaction, ? extends CompletableFuture<V>> function) {
        return function.apply(this);
    }

    @Override
    public Executor getExecutor() {
        return underlying.getExecutor();
    }

    // --- Byte-counting inner classes ---

    private static class ByteCountingAsyncIterable implements AsyncIterable<KeyValue> {
        private final AsyncIterable<KeyValue> underlying;
        private final TransactionMetrics metrics;

        ByteCountingAsyncIterable(AsyncIterable<KeyValue> underlying, TransactionMetrics metrics) {
            this.underlying = underlying;
            this.metrics = metrics;
        }

        @Override
        public @NonNull AsyncIterator<KeyValue> iterator() {
            metrics.incrementRangeReads();
            return new ByteCountingAsyncIterator(underlying.iterator(), metrics);
        }

        @Override
        public CompletableFuture<List<KeyValue>> asList() {
            metrics.incrementRangeReads();
            return underlying.asList().thenApply(keyValues -> {
                int bytes = 0;
                for (KeyValue kv : keyValues) {
                    bytes += kv.getKey().length + kv.getValue().length;
                }
                metrics.addBytesRead(bytes);
                return keyValues;
            });
        }
    }

    private static class ByteCountingAsyncIterator implements AsyncIterator<KeyValue> {
        private final AsyncIterator<KeyValue> underlying;
        private final TransactionMetrics metrics;

        ByteCountingAsyncIterator(AsyncIterator<KeyValue> underlying, TransactionMetrics metrics) {
            this.underlying = underlying;
            this.metrics = metrics;
        }

        @Override
        public CompletableFuture<Boolean> onHasNext() {
            return underlying.onHasNext();
        }

        @Override
        public boolean hasNext() {
            return underlying.hasNext();
        }

        @Override
        public KeyValue next() {
            KeyValue kv = underlying.next();
            metrics.addBytesRead(kv.getKey().length + kv.getValue().length);
            return kv;
        }

        @Override
        public void cancel() {
            underlying.cancel();
        }
    }
}
