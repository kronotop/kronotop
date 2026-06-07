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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A segment that supports position-based random reads.
 *
 * @see WritableSegment
 */
public interface ReadableSegment extends Segment {
    /**
     * Reads an entry from the segment at the given position and length.
     *
     * @param position the byte offset of the entry
     * @param length   the number of bytes to read
     * @return a {@link ByteBuffer} containing the entry data, positioned at 0
     * @throws IOException if an I/O error occurs
     */
    ByteBuffer get(long position, long length) throws IOException;
}
