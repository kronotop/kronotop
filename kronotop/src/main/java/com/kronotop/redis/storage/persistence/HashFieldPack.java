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

package com.kronotop.redis.storage.persistence;

import com.kronotop.redis.hash.HashFieldValue;

import java.io.IOException;
import java.nio.ByteBuffer;

public record HashFieldPack(String key, String field, HashFieldValue hashFieldValue) implements DataStructurePack {
    // Magic (1) + Key Length(4) + Field Length (4) + Value Length (8) + TTL (8) = 25
    public static int HEADER_SIZE = 25;
    public static byte MAGIC = 0x48;

    public static HashFieldPack unpack(ByteBuffer buffer) throws IOException {
        if (buffer.remaining() == 0) {
            throw new IOException("buffer cannot be empty");
        }
        byte magic = buffer.get();
        if (magic != MAGIC) {
            throw new IOException("value is not a String");
        }

        long ttl = buffer.getLong();

        // key section starts
        int keyLength = buffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);

        int fieldLength = buffer.getInt();
        byte[] fieldBytes = new byte[fieldLength];
        buffer.get(fieldBytes);
        // key section ends

        int valueLength = buffer.getInt();
        byte[] value = new byte[valueLength];
        buffer.get(value);

        return new HashFieldPack(new String(keyBytes), new String(fieldBytes), new HashFieldValue(value, ttl));
    }

    public static ByteBuffer pack(String key, String field, HashFieldValue hashField) {
        int capacity = HEADER_SIZE + key.length() + field.length() + hashField.value().length;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.put(MAGIC);

        buffer.putLong(hashField.ttl());

        // key section starts
        buffer.putInt(key.length());
        buffer.put(key.getBytes());

        buffer.putInt(field.length());
        buffer.put(field.getBytes());
        // key section ends

        buffer.putInt(hashField.value().length);
        buffer.put(hashField.value());

        buffer.flip();
        return buffer;
    }

    @Override
    public ByteBuffer pack() {
        return HashFieldPack.pack(key, field, hashFieldValue);
    }
}
