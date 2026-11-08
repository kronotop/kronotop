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

package com.kronotop.volume.blob;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.volume.*;
import com.kronotop.volume.segment.SegmentNotFoundException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;

/**
 * Reads the chunks stored under a prefix in pages and hands each chunk to a {@link BlobChunkSink}. The caller owns
 * the sink and opens a new transaction for every call, so a large prefix does not hit the transaction time limit.
 */
public final class BlobChunkReader {
    private final Volume volume;
    private final Prefix prefix;

    public BlobChunkReader(Volume volume, Prefix prefix) {
        this.volume = volume;
        this.prefix = prefix;
    }

    /**
     * Passes up to {@code limit} chunks after the cursor to the sink in storage order and returns the new cursor.
     *
     * @param tr     transaction the range read runs in
     * @param cursor key returned by the previous call, or null to start at the beginning of the prefix
     * @param limit  maximum number of chunks to read in this call
     * @param sink   receiver of the chunks
     * @return key of the last chunk read, or null when there are no chunks after the cursor
     * @throws IOException               if the sink cannot consume a chunk
     * @throws BlobChunkReadException    if a chunk cannot be read from Volume
     * @throws CorruptBlobChunkException if an entry does not start with a valid header
     */
    public Versionstamp read(Transaction tr, Versionstamp cursor, int limit, BlobChunkSink sink) throws IOException {
        VolumeSession session = new VolumeSession(tr, prefix);
        Iterable<VolumeEntry> entries = cursor == null
                ? volume.getRange(session, limit)
                : volume.getRange(session, VersionstampedKeySelector.firstGreaterThan(cursor), null, limit);
        Versionstamp last = null;
        Iterator<VolumeEntry> iterator = entries.iterator();
        while (true) {
            VolumeEntry entry;
            try {
                if (!iterator.hasNext()) {
                    break;
                }
                entry = iterator.next();
            } catch (UncheckedIOException | SegmentNotFoundException exp) {
                throw new BlobChunkReadException(exp);
            }
            BlobChunkHeader header = BlobChunkHeader.readHeader(entry.entry());
            sink.accept(header, entry.entry());
            last = entry.key();
        }
        return last;
    }
}
