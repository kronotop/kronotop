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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class BlobChunkHeaderTest {

    @Test
    void shouldRoundTripHeader() {
        // Behavior: a header with name "vector-index.bin", offset 3072 and chunkCount 7 is encoded to bytes
        // and decoded again; the decoded record is equal to the original, so all three fields survive the trip.
        BlobChunkHeader header = new BlobChunkHeader("vector-index.bin", 3L * 1024, 7);

        BlobChunkHeader decoded = BlobChunkHeader.readHeader(ByteBuffer.wrap(header.encode()));

        assertEquals(header, decoded);
    }

    @Test
    void shouldLeaveBufferPositionAtPayloadStart() {
        // Behavior: a buffer holds the encoded header followed by 16 payload bytes; after readHeader the buffer
        // position is at the end of the header, so the remaining bytes are exactly the payload.
        byte[] headerBytes = new BlobChunkHeader("vector-index.bin", 3L * 1024, 7).encode();
        byte[] payload = new byte[16];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }
        ByteBuffer buffer = ByteBuffer.allocate(headerBytes.length + payload.length);
        buffer.put(headerBytes).put(payload);

        BlobChunkHeader.readHeader(buffer);

        assertEquals(headerBytes.length, buffer.position());
        assertEquals(payload.length, buffer.remaining());
        byte[] remaining = new byte[buffer.remaining()];
        buffer.get(remaining);
        assertArrayEquals(payload, remaining);
    }

    @Test
    void shouldRejectChunkWithBadMagic() {
        // Behavior: a valid encoded header whose first 4 bytes are overwritten with zeros is not a blob chunk;
        // readHeader throws CorruptBlobChunkException instead of decoding it.
        byte[] bytes = new BlobChunkHeader("vector-index.bin", 3L * 1024, 7).encode();
        ByteBuffer.wrap(bytes).putInt(0);

        assertThrows(CorruptBlobChunkException.class, () -> BlobChunkHeader.readHeader(ByteBuffer.wrap(bytes)));
    }

    @Test
    void shouldRejectTruncatedHeader() {
        // Behavior: a buffer of 8 bytes is shorter than the fixed header part; readHeader throws
        // CorruptBlobChunkException without reading any field.
        assertThrows(CorruptBlobChunkException.class, () -> BlobChunkHeader.readHeader(ByteBuffer.allocate(8)));
    }

    @Test
    void shouldRejectNegativeOffsetOrZeroChunkCount() {
        // Behavior: a header with offset -1, and a header with chunkCount 0, both encode fine but cannot describe a
        // real chunk; readHeader throws CorruptBlobChunkException for each of them.
        byte[] negativeOffset = new BlobChunkHeader("x", -1, 3).encode();
        byte[] zeroChunkCount = new BlobChunkHeader("x", 0, 0).encode();

        assertThrows(CorruptBlobChunkException.class, () -> BlobChunkHeader.readHeader(ByteBuffer.wrap(negativeOffset)));
        assertThrows(CorruptBlobChunkException.class, () -> BlobChunkHeader.readHeader(ByteBuffer.wrap(zeroChunkCount)));
    }
}
