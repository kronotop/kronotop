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
 * @param name       ASCII name of the blob
 * @param chunkIndex zero based position of this chunk in the blob
 * @param chunkCount total number of chunks in the blob
 */
public record BlobChunkHeader(String name, int chunkIndex, int chunkCount) {

    static final int MAGIC = 0x4B424C42;
    private static final int FIXED_LENGTH = 4 * 4;

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
        buffer.putInt(chunkIndex);
        buffer.putInt(chunkCount);
        buffer.put(name.getBytes(StandardCharsets.US_ASCII));
        return buffer.array();
    }

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
            throw new CorruptBlobChunkException("chunk shorter than header");
        }
        if (buffer.getInt() != MAGIC) {
            throw new CorruptBlobChunkException("bad magic");
        }
        int headerLength = buffer.getInt();
        if (headerLength <= FIXED_LENGTH || headerLength > buffer.limit()) {
            throw new CorruptBlobChunkException("invalid header length: " + headerLength);
        }
        int chunkIndex = buffer.getInt();
        int chunkCount = buffer.getInt();
        if (chunkIndex < 0 || chunkIndex >= chunkCount) {
            throw new CorruptBlobChunkException("invalid chunk position: " + chunkIndex + "/" + chunkCount);
        }
        byte[] name = new byte[headerLength - FIXED_LENGTH];
        buffer.get(name);
        return new BlobChunkHeader(new String(name, StandardCharsets.US_ASCII), chunkIndex, chunkCount);
    }
}
