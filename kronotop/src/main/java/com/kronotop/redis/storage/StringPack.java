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

package com.kronotop.redis.storage;

import com.kronotop.redis.string.StringValue;

import java.io.IOException;
import java.nio.ByteBuffer;

public record StringPack(String key, StringValue stringValue) implements DataStructurePack {
    public static final byte MAGIC = 0x53;
    public static int HEADER_SIZE = 17;

    public static ByteBuffer pack(String key, StringValue stringValue) {
        // HEADER SIZE: 1 (Magic) + 8 (TTL) + 4 (Key Size) + 4 (Value Size) = 17
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + key.length() + stringValue.value().length);
        buffer.put(MAGIC);
        buffer.putLong(stringValue.ttl());
        buffer.putInt(key.length());
        buffer.put(key.getBytes());
        buffer.putInt(stringValue.value().length);
        buffer.put(stringValue.value());
        buffer.flip();
        return buffer;
    }

    public static StringPack unpack(ByteBuffer buffer) throws IOException {
        if (buffer.remaining() == 0) {
            throw new IOException("buffer cannot be empty");
        }
        byte magic = buffer.get();
        if (magic != MAGIC) {
            throw new IOException("value is not a String");
        }

        long ttl = buffer.getLong();

        int keyLength = buffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);

        int valueLength = buffer.getInt();
        byte[] value = new byte[valueLength];
        buffer.get(value);
        StringValue stringValue = new StringValue(value, ttl);

        return new StringPack(new String(keyBytes), stringValue);
    }

    @Override
    public ByteBuffer pack() {
        return StringPack.pack(key, stringValue);
    }
}
