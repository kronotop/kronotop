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

package com.kronotop.volume.segment;

import com.kronotop.volume.NotEnoughSpaceException;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A segment that supports append, positional insert, and flush operations.
 *
 * @see ReadableSegment
 */
public interface WritableSegment extends Segment {

    /**
     * Returns the number of bytes still available for writing.
     *
     * @return remaining free bytes in this segment
     */
    long getFreeBytes();

    /**
     * Appends an entry to the end of this segment, advancing the write position atomically.
     *
     * @param entry the data to append
     * @return the position and length of the written entry
     * @throws NotEnoughSpaceException if the segment cannot accommodate the entry
     * @throws IOException             if an I/O error occurs
     */
    SegmentAppendResult append(ByteBuffer entry) throws NotEnoughSpaceException, IOException;

    /**
     * Writes an entry at an explicit position without advancing the write pointer.
     * Used during replication to reproduce the primary's segment layout.
     *
     * @param entry    the data to write
     * @param position the exact byte offset to write at
     * @throws IOException             if an I/O error occurs
     * @throws NotEnoughSpaceException if position + entry length exceeds the segment size
     */
    void insert(ByteBuffer entry, long position) throws IOException, NotEnoughSpaceException;

    /**
     * Flushes pending writes to durable storage.
     *
     * @throws IOException if an I/O error occurs during sync
     */
    void flush() throws IOException;
}
