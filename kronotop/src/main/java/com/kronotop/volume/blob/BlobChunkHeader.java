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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Header stored in front of every chunk of a blob.
 *
 * @param name       ASCII name of the file this chunk belongs to. Many files can share one prefix, the name separates them.
 * @param offset     byte offset of the chunk payload in the blob
 * @param chunkCount total number of chunks in the blob
 */
public record BlobChunkHeader(String name, long offset, int chunkCount) {

    static final int MAGIC = 0x4B424C42;
    // magic (int) + header length (int) + offset (long) + chunk count (int)
    private static final int FIXED_LENGTH = 4 + 4 + 8 + 4;

    /**
     * Reads a header from the start of the buffer and leaves the position at the first payload byte.
     *
     * @param buffer buffer that starts with an encoded header
     * @return the decoded header
     * @throws CorruptBlobChunkException if the header is malformed
     */
    public static BlobChunkHeader readHeader(ByteBuffer buffer) {
        buffer.rewind();
        if (buffer.remaining() < FIXED_LENGTH) {
            throw new CorruptBlobChunkException("Chunk shorter than header");
        }
        if (buffer.getInt() != MAGIC) {
            throw new CorruptBlobChunkException("Bad magic");
        }
        int headerLength = buffer.getInt();
        if (headerLength <= FIXED_LENGTH || headerLength > buffer.limit()) {
            throw new CorruptBlobChunkException("Invalid header length: " + headerLength);
        }
        long offset = buffer.getLong();
        int chunkCount = buffer.getInt();
        if (offset < 0 || chunkCount <= 0) {
            throw new CorruptBlobChunkException("Invalid chunk fields: " + offset + "/" + chunkCount);
        }
        byte[] name = new byte[headerLength - FIXED_LENGTH];
        buffer.get(name);
        return new BlobChunkHeader(new String(name, StandardCharsets.US_ASCII), offset, chunkCount);
    }

    /**
     * Encodes this header to its wire form.
     *
     * @return the encoded header
     */
    public byte[] encode() {
        int headerLength = FIXED_LENGTH + name.length();
        ByteBuffer buffer = ByteBuffer.allocate(headerLength);
        buffer.putInt(MAGIC);
        buffer.putInt(headerLength);
        buffer.putLong(offset);
        buffer.putInt(chunkCount);
        buffer.put(name.getBytes(StandardCharsets.US_ASCII));
        return buffer.array();
    }
}
