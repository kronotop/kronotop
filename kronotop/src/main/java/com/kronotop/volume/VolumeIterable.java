/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.volume;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Versionstamp;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;


class VolumeIterable implements Iterable<KeyEntryPair> {
    private final AsyncIterable<KeyValue> asyncIterable;
    private final Volume volume;
    private final VolumeSession session;
    private final int limit;
    private final boolean reverse;

    VolumeIterable(Volume volume, VolumeSession session, VersionstampedKeySelector begin, VersionstampedKeySelector end, int limit, boolean reverse) {
        this.volume = volume;
        this.session = session;
        this.limit = limit;
        this.reverse = reverse;
        this.asyncIterable = createAsyncIterable(session, begin, end);
    }

    private AsyncIterable<KeyValue> createAsyncIterable(VolumeSession session, VersionstampedKeySelector begin, VersionstampedKeySelector end) {
        VolumeSubspace subspace = volume.getSubspace();
        KeySelector beginKeySelector;
        if (begin == null) {
            beginKeySelector = KeySelector.firstGreaterOrEqual(subspace.packEntryKeyPrefix(session.prefix()));
        } else {
            beginKeySelector = new KeySelector(subspace.packEntryKey(session.prefix(), begin.getKey()), begin.orEqual(), begin.getOffset());
        }

        KeySelector endKeySelector;
        if (end == null) {
            endKeySelector = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(beginKeySelector.getKey()));
        } else {
            endKeySelector = new KeySelector(subspace.packEntryKey(session.prefix(), end.getKey()), end.orEqual(), end.getOffset());
        }
        return session.transaction().getRange(beginKeySelector, endKeySelector, limit, reverse);
    }

    @Nonnull
    @Override
    public Iterator<KeyEntryPair> iterator() {
        return new VolumeIterator(volume, session, asyncIterable.iterator());
    }

    private static class VolumeIterator implements Iterator<KeyEntryPair> {
        private final Volume volume;
        private final VolumeSession session;
        private final AsyncIterator<KeyValue> asyncIterator;

        VolumeIterator(Volume volume, VolumeSession session, AsyncIterator<KeyValue> asyncIterator) {
            this.volume = volume;
            this.session = session;
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
        public KeyEntryPair next() {
            KeyValue keyValue = asyncIterator.next();
            Versionstamp key = (Versionstamp) volume.getConfig().subspace().unpack(keyValue.getKey()).get(2);
            EntryMetadata entryMetadata = EntryMetadata.decode(ByteBuffer.wrap(keyValue.getValue()));
            try {
                ByteBuffer entry = volume.getByEntryMetadata(session.prefix(), key, entryMetadata);
                return new KeyEntryPair(key, entry);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
