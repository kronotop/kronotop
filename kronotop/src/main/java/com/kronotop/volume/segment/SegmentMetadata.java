/*
 * Copyright (c) 2023-2025 Kronotop
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

package com.kronotop.volume.segment;

import java.nio.ByteBuffer;

class SegmentMetadata {
    public static final int HEADER_SIZE = 24;
    private final long id;
    private final long size;
    private final ByteBuffer buffer;
    private long position;

    SegmentMetadata(long id, long size) {
        this.id = id;
        this.size = size;
        this.buffer = ByteBuffer.allocate(HEADER_SIZE);
    }

    static SegmentMetadata decode(ByteBuffer buffer) {
        buffer.flip();
        long id = buffer.getLong();
        long size = buffer.getLong();
        SegmentMetadata segmentMetadata = new SegmentMetadata(id, size);
        segmentMetadata.setPosition(buffer.getLong());
        return segmentMetadata;
    }

    long getId() {
        return id;
    }

    long getSize() {
        return size;
    }

    long getPosition() {
        return position;
    }

    void setPosition(long position) {
        this.position = position;
    }

    ByteBuffer encode() {
        buffer.clear();
        buffer.putLong(id);
        buffer.putLong(size);
        buffer.putLong(position);
        buffer.flip();
        return buffer;
    }
}
