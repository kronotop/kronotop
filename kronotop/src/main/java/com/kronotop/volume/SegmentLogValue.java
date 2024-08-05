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

public record SegmentLogValue(OperationKind kind, long position, long length) {

    public static SegmentLogValue decode(ByteBuffer buffer) {
        byte kind = buffer.get();
        long position = buffer.getLong();
        long length = buffer.getLong();
        return new SegmentLogValue(OperationKind.valueOf(kind), position, length);
    }

    public ByteBuffer encode() {
        // OperationKind: 1 byte
        // Position: 8 bytes
        // Length: 8 bytes
        return ByteBuffer.allocate(17).put(kind.getValue()).putLong(position).putLong(length).flip();
    }
}
