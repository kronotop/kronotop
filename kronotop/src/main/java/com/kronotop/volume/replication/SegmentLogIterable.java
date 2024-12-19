/*
 * Copyright (c) 2023-2024 Kronotop
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.volume.replication;

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
import com.kronotop.volume.VersionstampedKeySelector;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.kronotop.volume.Subspaces.SEGMENT_LOG_SUBSPACE;

public class SegmentLogIterable implements Iterable<SegmentLogEntry> {
    private final AsyncIterable<KeyValue> asyncIterable;
    private final String segmentName;
    private final DirectorySubspace subspace;

    public SegmentLogIterable(Transaction tr, DirectorySubspace subspace, String segmentName) {
        this(tr, subspace, segmentName, null, null);
    }

    SegmentLogIterable(Transaction tr, DirectorySubspace subspace, String segmentName, VersionstampedKeySelector begin, VersionstampedKeySelector end) {
        this(tr, subspace, segmentName, begin, end, ReadTransaction.ROW_LIMIT_UNLIMITED, false);
    }

    SegmentLogIterable(Transaction tr, DirectorySubspace subspace, String segmentName, VersionstampedKeySelector begin, VersionstampedKeySelector end, int limit) {
        this(tr, subspace, segmentName, begin, end, limit, false);
    }

    SegmentLogIterable(Transaction tr, DirectorySubspace subspace, String segmentName, VersionstampedKeySelector begin, VersionstampedKeySelector end, int limit, boolean reverse) {
        this.subspace = subspace;
        this.segmentName = segmentName;
        this.asyncIterable = createAsyncIterable(tr, begin, end, limit, reverse);
    }

    private byte[] packSegmentRoot() {
        return subspace.pack(Tuple.from(SEGMENT_LOG_SUBSPACE, segmentName));
    }

    private byte[] packKey(Versionstamp key) {
        return subspace.pack(Tuple.from(SEGMENT_LOG_SUBSPACE, segmentName, key));
    }

    private AsyncIterable<KeyValue> createAsyncIterable(Transaction tr, VersionstampedKeySelector begin, VersionstampedKeySelector end, int limit, boolean reverse) {
        KeySelector beginKeySelector;
        if (begin == null) {
            beginKeySelector = KeySelector.firstGreaterOrEqual(packSegmentRoot());
        } else {
            beginKeySelector = new KeySelector(packKey(begin.getKey()), begin.orEqual(), begin.getOffset());
        }

        KeySelector endKeySelector;
        if (end == null) {
            endKeySelector = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(packSegmentRoot()));
        } else {
            endKeySelector = new KeySelector(packKey(end.getKey()), end.orEqual(), end.getOffset());
        }
        return tr.getRange(beginKeySelector, endKeySelector, limit, reverse);
    }

    @Nonnull
    @Override
    public Iterator<SegmentLogEntry> iterator() {
        return new SegmentLogIterator(subspace, asyncIterable.iterator());
    }

    private static class SegmentLogIterator implements Iterator<SegmentLogEntry> {
        private final DirectorySubspace subspace;
        private final AsyncIterator<KeyValue> asyncIterator;

        SegmentLogIterator(DirectorySubspace subspace, AsyncIterator<KeyValue> asyncIterator) {
            this.subspace = subspace;
            this.asyncIterator = asyncIterator;
        }

        /**
         * Returns {@code true} if the iteration has more elements.
         * (In other words, returns {@code true} if {@link #next} would
         * return an element rather than throwing an exception.)
         *
         * @return {@code true} if the iteration has more elements
         */
        @Override
        public boolean hasNext() {
            return asyncIterator.hasNext();
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next element in the iteration
         * @throws NoSuchElementException if the iteration has no more elements
         */
        @Override
        public SegmentLogEntry next() {
            KeyValue keyValue = asyncIterator.next();
            Tuple unpacked = subspace.unpack(keyValue.getKey());
            Versionstamp key = (Versionstamp) unpacked.get(2);
            long timestamp;
            Versionstamp entryKey;
            if (unpacked.size() == 5) {
                entryKey = (Versionstamp) unpacked.get(3);
                timestamp = (long) unpacked.get(4);
            } else {
                entryKey = key;
                timestamp = (long) unpacked.get(3);
            }
            return new SegmentLogEntry(key, entryKey, timestamp, SegmentLogValue.decode(ByteBuffer.wrap(keyValue.getValue())));
        }
    }
}
