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

package com.kronotop.volume;

import java.nio.ByteBuffer;

import static com.kronotop.volume.segment.Segment.SEGMENT_NAME_SIZE;

public record EntryMetadata(String segment, byte[] prefix, long position, long length) {
    public static int ENTRY_PREFIX_SIZE = 8;
    public static int ENTRY_METADATA_SIZE = SEGMENT_NAME_SIZE + ENTRY_PREFIX_SIZE + 17; // position(8 bytes) + length (8 bytes) + subspace seperator
    static byte SUBSPACE_SEPARATOR = 0x0;

    public static EntryMetadata decode(ByteBuffer buffer) {
        byte[] rawSegment = new byte[SEGMENT_NAME_SIZE];
        buffer.get(rawSegment);
        buffer.get(); // Consume the separator
        String segment = new String(rawSegment);
        byte[] prefix = new byte[ENTRY_PREFIX_SIZE];
        buffer.get(prefix);
        long position = buffer.getLong();
        long length = buffer.getLong();
        return new EntryMetadata(segment, prefix, position, length);
    }

    public ByteBuffer encode() {
        if (prefix.length != ENTRY_PREFIX_SIZE) {
            throw new IllegalArgumentException("Invalid prefix length");
        }
        return ByteBuffer.
                allocate(ENTRY_METADATA_SIZE).
                put(segment.getBytes()).
                put(SUBSPACE_SEPARATOR).
                put(prefix).
                putLong(position).
                putLong(length).flip();
    }
}
