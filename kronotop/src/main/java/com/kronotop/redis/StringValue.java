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

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The StringValue class represents a string value stored as a byte array.
 * It can be decoded from and encoded into a byte array using MessagePack serialization.
 */
public class StringValue {
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
     * Decodes the given byte array and returns a StringValue object.
     *
     * @param data the byte array to decode
     * @return the decoded StringValue object
     * @throws IOException if an I/O error occurs while unpacking the data
     */
    public static StringValue decode(byte[] data) throws IOException {
        if (data.length == 0) {
            throw new IOException("data cannot be empty");
        }
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data)) {
            long ttl = unpacker.unpackLong();
            int payloadSize = unpacker.unpackInt();
            ByteBuffer buffer = ByteBuffer.allocate(payloadSize);
            unpacker.readPayload(buffer);
            return new StringValue(buffer.array(), ttl);
        }
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
    public byte[] encode() throws IOException {
        try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
            packer.packLong(this.getTTL());
            packer.packInt(this.getValue().length);
            packer.writePayload(this.getValue());
            return packer.toByteArray();
        }
    }
}
