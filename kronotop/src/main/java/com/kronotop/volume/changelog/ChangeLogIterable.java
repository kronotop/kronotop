/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.volume.changelog;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.volume.OperationKind;
import com.kronotop.volume.ParentOperationKind;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.kronotop.volume.Subspaces.CHANGELOG_SUBSPACE;

/**
 * Provides iteration over changelog entries within a specified range.
 * <p>
 * This class wraps FoundationDB's async range query to iterate over entries stored in the changelog.
 * Each iteration returns a {@link ChangeLogEntry} containing operation details (APPEND, UPDATE, DELETE),
 * coordinates, and prefix information.
 * <p>
 * Supports range filtering via {@link SequenceNumberSelector}, limiting results, reverse iteration,
 * and filtering by {@link ParentOperationKind} (LIFECYCLE for APPEND/UPDATE, FINALIZATION for DELETE).
 * Configuration is provided through {@link ChangeLogIterableOptions}.
 * <p>
 * Note: The limit is applied at the FoundationDB range query level before iterator-level filtering.
 * When using {@link ParentOperationKind} filtering with a limit, the actual number of returned entries
 * may be less than the limit since non-matching entries are filtered out after the range query.
 */
public class ChangeLogIterable implements Iterable<ChangeLogEntry> {
    private final AsyncIterable<KeyValue> asyncIterable;
    private final DirectorySubspace subspace;
    private final ParentOperationKind parentOpKind;

    /**
     * Creates a new ChangeLogIterable that iterates over all entries.
     *
     * @param tr       the transaction to use for the query
     * @param subspace the directory subspace containing the changelog
     */
    public ChangeLogIterable(Transaction tr, DirectorySubspace subspace) {
        this(tr, subspace, new ChangeLogIterableOptions.Builder().build());
    }

    /**
     * Creates a new ChangeLogIterable with the specified options.
     *
     * @param tr       the transaction to use for the query
     * @param subspace the directory subspace containing the changelog
     * @param options  configuration options for range, limit, and order
     */
    public ChangeLogIterable(Transaction tr, DirectorySubspace subspace, ChangeLogIterableOptions options) {
        this.subspace = subspace;
        this.parentOpKind = options.parentOperationKind();
        int limit = options.limit() != null ? options.limit() : ReadTransaction.ROW_LIMIT_UNLIMITED;
        boolean reverse = options.reverse() != null && options.reverse();
        this.asyncIterable = createAsyncIterable(tr, options.begin(), options.end(), limit, reverse);
    }

    private byte[] packRoot() {
        return subspace.pack(Tuple.from(CHANGELOG_SUBSPACE));
    }

    private byte[] packKey(long sequenceNumber) {
        return subspace.pack(Tuple.from(CHANGELOG_SUBSPACE, sequenceNumber));
    }

    private AsyncIterable<KeyValue> createAsyncIterable(Transaction tr, SequenceNumberSelector begin, SequenceNumberSelector end, int limit, boolean reverse) {
        KeySelector beginKeySelector;
        if (begin == null) {
            beginKeySelector = KeySelector.firstGreaterOrEqual(packRoot());
        } else {
            // For the firstGreaterThan (orEqual=true), use strinc to get the first key after all entries with this sequence number.
            // For the firstGreaterOrEqual (orEqual=false), use the prefix to get the first key at or after this sequence number.
            byte[] key = begin.orEqual() ? ByteArrayUtil.strinc(packKey(begin.sequenceNumber())) : packKey(begin.sequenceNumber());
            beginKeySelector = KeySelector.firstGreaterOrEqual(key);
        }

        KeySelector endKeySelector;
        if (end == null) {
            endKeySelector = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(packRoot()));
        } else {
            // Same logic for end selector
            byte[] key = end.orEqual() ? ByteArrayUtil.strinc(packKey(end.sequenceNumber())) : packKey(end.sequenceNumber());
            endKeySelector = KeySelector.firstGreaterOrEqual(key);
        }
        return tr.getRange(beginKeySelector, endKeySelector, limit, reverse);
    }

    @NotNull
    @Override
    public Iterator<ChangeLogEntry> iterator() {
        return new ChangeLogIterator(subspace, asyncIterable.iterator(), parentOpKind);
    }

    private static class ChangeLogIterator implements Iterator<ChangeLogEntry> {
        private final DirectorySubspace subspace;
        private final AsyncIterator<KeyValue> asyncIterator;
        private final ParentOperationKind parentOpKind;

        // single-element lookahead buffer
        private KeyValue prefetched = null;

        ChangeLogIterator(DirectorySubspace subspace,
                          AsyncIterator<KeyValue> asyncIterator,
                          ParentOperationKind parentOpKind) {
            this.subspace = subspace;
            this.asyncIterator = asyncIterator;
            this.parentOpKind = parentOpKind;
        }

        @Override
        public boolean hasNext() {
            if (prefetched != null) {
                return true;
            }

            while (asyncIterator.hasNext()) {
                KeyValue kv = asyncIterator.next();
                if (matches(kv)) {
                    prefetched = kv;
                    return true;
                }
            }

            return false;
        }

        @Override
        public ChangeLogEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            KeyValue kv = prefetched;
            prefetched = null;

            Tuple key = subspace.unpack(kv.getKey());
            return decodeEntry(key, kv.getValue());
        }

        private boolean matches(KeyValue kv) {
            if (parentOpKind == null) {
                return true;
            }
            Tuple t = subspace.unpack(kv.getKey());
            byte v = (byte) t.getLong(2);
            return ParentOperationKind.valueOf(v) == parentOpKind;
        }

        private ChangeLogEntry decodeEntry(Tuple keyTuple, byte[] valueBytes) {
            Tuple value = Tuple.fromBytes(valueBytes);

            long sequenceNumber = keyTuple.getLong(1);
            byte childValue = (byte) keyTuple.getLong(3);
            OperationKind opKind = OperationKind.valueOf(childValue);
            Versionstamp vs = keyTuple.getVersionstamp(4);

            ChangeLogCoordinate before = null;
            ChangeLogCoordinate after = null;
            long prefix;

            switch (opKind) {
                case APPEND -> {
                    long segmentId = value.getLong(0);
                    long pos = value.getLong(1);
                    long len = value.getLong(2);
                    prefix = value.getLong(3);
                    after = new ChangeLogCoordinate(sequenceNumber, segmentId, pos, len);
                }
                case DELETE -> {
                    long segmentId = value.getLong(0);
                    long pos = value.getLong(1);
                    long len = value.getLong(2);
                    prefix = value.getLong(3);
                    before = new ChangeLogCoordinate(sequenceNumber, segmentId, pos, len);
                }
                case UPDATE -> {
                    long segmentId = value.getLong(0);
                    long pos = value.getLong(1);
                    long len = value.getLong(2);
                    long prevSegmentId = value.getLong(3);
                    long prevPos = value.getLong(4);
                    long prevLen = value.getLong(5);
                    prefix = value.getLong(6);
                    after = new ChangeLogCoordinate(sequenceNumber, segmentId, pos, len);
                    before = new ChangeLogCoordinate(sequenceNumber, prevSegmentId, prevPos, prevLen);
                }
                default -> throw new IllegalStateException("Unexpected operation kind: " + opKind);
            }

            return new ChangeLogEntry(vs, opKind, before, after, prefix);
        }
    }
}
