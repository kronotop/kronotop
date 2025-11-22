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

import com.apple.foundationdb.tuple.Tuple;

/**
 * EntryMetadata represents metadata for a storage entry within a segmented storage system.
 * Contains segment identifier, prefix, position, length, and unique entry handle.
 */
public record EntryMetadata(long segmentId, byte[] prefix, long position, long length, long handle) {
    public static int ENTRY_PREFIX_SIZE = 8;

    /**
     * Decodes entry metadata from packed tuple bytes.
     *
     * @param data packed tuple bytes containing entry metadata
     * @return decoded EntryMetadata instance
     */
    public static EntryMetadata decode(byte[] data) {
        Tuple tuple = Tuple.fromBytes(data);
        long segmentId = tuple.getLong(0);
        byte[] prefix = tuple.getBytes(1);
        long position = tuple.getLong(2);
        long length = tuple.getLong(3);
        long handle = tuple.getLong(4);
        return new EntryMetadata(segmentId, prefix, position, length, handle);
    }

    /**
     * Extracts the handle from packed tuple bytes.
     *
     * @param data packed tuple bytes containing entry metadata
     * @return the entry handle
     */
    public static long extractHandle(byte[] data) {
        Tuple tuple = Tuple.fromBytes(data);
        return tuple.getLong(4);
    }

    /**
     * Encodes entry metadata into packed tuple bytes.
     *
     * @return packed tuple bytes
     * @throws IllegalArgumentException if prefix length is invalid
     */
    public byte[] encode() {
        if (prefix.length != ENTRY_PREFIX_SIZE) {
            throw new IllegalArgumentException("Invalid prefix length");
        }
        return Tuple.from(segmentId, prefix, position, length, handle).pack();
    }
}
