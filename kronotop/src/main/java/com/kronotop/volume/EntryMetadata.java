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

public record EntryMetadata(String segment, long position, long length) {

    public static EntryMetadata decode(ByteBuffer buffer) {
        byte[] rawSegment = new byte[19];
        buffer.get(rawSegment);
        String segment = new String(rawSegment);
        long position = buffer.getLong();
        long length = buffer.getLong();
        return new EntryMetadata(segment, position, length);
    }

    public ByteBuffer encode() {
        return ByteBuffer.allocate(35).put(segment.getBytes()).putLong(position).putLong(length).flip();
    }
}
