/*
 * Copyright (c) 2023-2026 Burak Sezer
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

/**
 * EntryMetadata represents metadata for a storage entry within a segmented storage system.
 * Contains segment identifier, prefix, position, length, and unique entry handle.
 */
public record EntryMetadata(long segmentId, byte[] prefix, long position, long length, long handle) {
    public static final int ENCODED_SIZE = 40;
    public static int ENTRY_PREFIX_SIZE = 8;

    /**
     * Decodes entry metadata from a fixed-size byte array.
     *
     * @param data 40-byte array containing entry metadata
     * @return decoded EntryMetadata instance
     */
    public static EntryMetadata decode(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        long segmentId = buffer.getLong();
        byte[] prefix = new byte[ENTRY_PREFIX_SIZE];
        buffer.get(prefix);
        long position = buffer.getLong();
        long length = buffer.getLong();
        long handle = buffer.getLong();
        return new EntryMetadata(segmentId, prefix, position, length, handle);
    }

    /**
     * Extracts the handle from a fixed-size byte array.
     *
     * @param data 40-byte array containing entry metadata
     * @return the entry handle
     */
    public static long extractHandle(byte[] data) {
        return ByteBuffer.wrap(data).getLong(32);
    }

    /**
     * Encodes entry metadata into a fixed-size byte array.
     *
     * @return 40-byte array
     * @throws IllegalArgumentException if the prefix length is invalid
     */
    public byte[] encode() {
        if (prefix.length != ENTRY_PREFIX_SIZE) {
            throw new IllegalArgumentException("Invalid prefix length");
        }
        ByteBuffer buffer = ByteBuffer.allocate(ENCODED_SIZE);
        buffer.putLong(segmentId);
        buffer.put(prefix);
        buffer.putLong(position);
        buffer.putLong(length);
        buffer.putLong(handle);
        return buffer.array();
    }
}
