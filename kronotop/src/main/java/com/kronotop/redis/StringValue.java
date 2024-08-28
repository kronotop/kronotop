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

package com.kronotop.redis;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The StringValue class represents a string value stored as a byte array.
 * It can be decoded from and encoded into a byte array using MessagePack serialization.
 */
public class StringValue {
    public static int HEADER_SIZE = 13;
    public static byte MAGIC = 0x53;
    private final byte[] value;
    private long ttl = 0;

    public StringValue(byte[] value) {
        this.value = value;
    }

    public StringValue(byte[] value, long ttl) {
        this.value = value;
        this.ttl = ttl;
    }

    /**
     * Decodes a byte buffer into a StringValue object.
     *
     * @param buffer the byte buffer to decode
     * @return the decoded StringValue object
     * @throws IOException if an I/O error occurs while decoding the data
     */
    public static StringValue decode(ByteBuffer buffer) throws IOException {
        if (buffer.remaining() == 0) {
            throw new IOException("buffer cannot be empty");
        }
        byte magic = buffer.get();
        if (magic != MAGIC) {
            throw new IOException("value is not a String");
        }
        long ttl = buffer.getLong();
        int valueLength = buffer.getInt();
        byte[] value = new byte[valueLength];
        buffer.get(value);
        return new StringValue(value, ttl);
    }

    /**
     * Retrieves the value of the StringValue object.
     *
     * @return the byte array representing the value
     */
    public byte[] getValue() {
        return value;
    }

    /**
     * Retrieves the time-to-live (TTL) value associated with the StringValue object.
     *
     * @return the TTL value in milliseconds
     */
    public long getTTL() {
        return ttl;
    }

    /**
     * Encodes the StringValue object into a byte array.
     * The encoding format consists of the following steps:
     * 1. Create a MessageBufferPacker to pack the values.
     * 2. Pack the TTL value using packLong() method.
     * 3. Pack the length of the value byte array using packInt() method.
     * 4. Write the value byte array using writePayload() method.
     * 5. Convert the packed data into a byte array using toByteArray() method.
     * 6. Close the MessageBufferPacker to release resources.
     *
     * @return the byte array representing the encoded StringValue object
     * @throws IOException if an I/O error occurs while encoding the data
     */
    public ByteBuffer encode() throws IOException {
        // HEADER SIZE: 1 + 8 + 4 = 13
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + value.length);
        buffer.put(MAGIC);
        buffer.putLong(getTTL());
        buffer.putInt(value.length);
        buffer.put(value);
        buffer.flip();
        return buffer;
    }
}
