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

public class SegmentMetadata {
    public static final int HEADER_SIZE = 32;
    private final long id;
    private final long size;
    private final ByteBuffer buffer;
    private long position;
    private long wastedBytes;

    public SegmentMetadata(long id, long size) {
        this.id = id;
        this.size = size;
        this.buffer = ByteBuffer.allocate(HEADER_SIZE);
    }

    public static SegmentMetadata decode(ByteBuffer buffer) {
        buffer.flip();
        long id = buffer.getLong();
        long size = buffer.getLong();
        SegmentMetadata segmentMetadata = new SegmentMetadata(id, size);
        segmentMetadata.setPosition(buffer.getLong());
        segmentMetadata.setWastedBytes(buffer.getLong());
        return segmentMetadata;
    }

    public long getId() {
        return id;
    }

    public long getSize() {
        return size;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public long getWastedBytes() {
        return wastedBytes;
    }

    public void setWastedBytes(long wastedBytes) {
        this.wastedBytes = wastedBytes;
    }

    public ByteBuffer encode() {
        buffer.clear();
        buffer.putLong(id);
        buffer.putLong(size);
        buffer.putLong(position);
        buffer.putLong(wastedBytes);
        buffer.flip();
        return buffer;
    }
}
