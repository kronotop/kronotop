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
import com.kronotop.volume.Prefix;
import com.kronotop.volume.Volume;
import com.kronotop.volume.VolumeEntry;
import com.kronotop.volume.VolumeSession;

import java.io.IOException;

/**
 * Reads every chunk stored under a prefix and hands each one to a {@link BlobChunkSink}. The caller owns the
 * transaction and the sink.
 */
public final class BlobChunkReader {
    private final Volume volume;
    private final Prefix prefix;

    public BlobChunkReader(Volume volume, Prefix prefix) {
        this.volume = volume;
        this.prefix = prefix;
    }

    /**
     * Passes all chunks under the prefix to the sink in storage order.
     *
     * @param tr   transaction the range read runs in
     * @param sink receiver of the chunks
     * @throws IOException               if a chunk cannot be read or consumed
     * @throws CorruptBlobChunkException if an entry does not start with a valid header
     */
    public void read(Transaction tr, BlobChunkSink sink) throws IOException {
        for (VolumeEntry entry : volume.getRange(new VolumeSession(tr, prefix))) {
            BlobChunkHeader header = BlobChunkHeader.readHeader(entry.entry());
            sink.accept(header, entry.entry());
        }
    }
}
