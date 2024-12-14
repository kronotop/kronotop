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

import static com.google.common.hash.Hashing.sipHash24;
import static com.kronotop.volume.segment.Segment.SEGMENT_NAME_SIZE;

/**
 * EntryMetadata is a record class representing the metadata attached to a specific entry
 * within a segmented storage system. This metadata includes information about the segment
 * name, the prefix used for organizing or identifying the entry, and its position and length
 * within the storage medium.
 * <p>
 * The segment name is stored as a string, while the prefix is a byte array, facilitating
 * efficient identification and retrieval operations. The position and length are long values
 * indicating where the associated data begins and its continuity in bytes.
 * <p>
 * EntryMetadata also provides mechanisms for encoding and decoding this metadata to and from
 * ByteBuffers, enabling streamlined persistence or transmission of this information in a
 * binary format. The design supports fixed sizes for various components to ensure direct access
 * and manipulation efficiency.
 */
public record EntryMetadata(String segment, byte[] prefix, long position, long length) {
    public static int ENTRY_PREFIX_SIZE = 8;
    public static int SUBSPACE_SEPARATOR_SIZE = 1;
    // 16 = position(8 bytes) + length (8 bytes)
    public static int ENTRY_METADATA_SIZE = SEGMENT_NAME_SIZE + ENTRY_PREFIX_SIZE + SUBSPACE_SEPARATOR_SIZE + 16;
    static byte SUBSPACE_SEPARATOR = 0x0;


    /**
     * Decodes a ByteBuffer to extract entry metadata, including the segment name, prefix,
     * position, and length.
     *
     * @param buffer the ByteBuffer which contains the encoded entry metadata.
     *               The buffer should be positioned at the start of the entry metadata.
     * @return an instance of EntryMetadata containing the decoded segment name, prefix,
     * position, and length extracted from the provided ByteBuffer.
     */
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

    /**
     * Encodes the current state of the entry metadata into a ByteBuffer. The resulting ByteBuffer
     * contains the segment name, subspace separator, prefix, position, and length in a binary format
     * that is suitable for storage or transmission.
     *
     * @return a ByteBuffer containing the binary representation of this EntryMetadata instance,
     * with the data elements sequentially packed in the specified order.
     * @throws IllegalArgumentException if the length of the prefix does not match the predefined
     *                                  ENTRY_PREFIX_SIZE.
     */
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

    public long cacheKey() {
        return EntryMetadata.cacheKey(segment, position);
    }

    /**
     * Generates a cache key based on the provided segment name and position.
     * This method uses the SipHash-2-4 algorithm to calculate a hash value
     * combining the segment name and position, ensuring efficient
     * and consistent key generation.
     *
     * @param segmentName the name of the segment, represented as a string.
     *                    This is included as part of the key computation.
     * @param position    the position within the segment, represented as a long.
     *                    This is also included in the key computation.
     * @return a long value representing the computed cache key.
     */
    @SuppressWarnings("UnstableApiUsage")
    public static long cacheKey(String segmentName, long position) {
        return sipHash24().newHasher().
                putBytes(segmentName.getBytes()).
                putLong(position).
                hash().asLong();
    }
}
