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

package com.kronotop.volume.replication;

import com.kronotop.volume.OperationKind;

import java.nio.ByteBuffer;

public record SegmentLogValue(OperationKind kind, long prefix, long position, long length) {
    public static int SIZE = 25;

    public static SegmentLogValue decode(ByteBuffer buffer) {
        byte kind = buffer.get();
        long prefix = buffer.getLong();
        long position = buffer.getLong();
        long length = buffer.getLong();
        return new SegmentLogValue(OperationKind.valueOf(kind), prefix, position, length);
    }

    public ByteBuffer encode() {
        // OperationKind: 1 byte
        // Prefix: 8 bytes
        // Position: 8 bytes
        // Length: 8 bytes
        return ByteBuffer.allocate(SIZE).put(kind.getValue()).putLong(prefix).putLong(position).putLong(length).flip();
    }
}
