/*
 * Copyright (c) 2023 Kronotop
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

    public static StringValue decode(byte[] data) throws IOException {
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data)) {
            long ttl = unpacker.unpackLong();
            int payloadSize = unpacker.unpackInt();
            ByteBuffer buffer = ByteBuffer.allocate(payloadSize);
            unpacker.readPayload(buffer);
            return new StringValue(buffer.array(), ttl);
        }
    }

    public byte[] getValue() {
        return value;
    }

    public long getTTL() {
        return ttl;
    }

    public byte[] encode() throws IOException {
        try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
            packer.packLong(this.getTTL());
            packer.packInt(this.getValue().length);
            packer.writePayload(this.getValue());
            return packer.toByteArray();
        }
    }
}
