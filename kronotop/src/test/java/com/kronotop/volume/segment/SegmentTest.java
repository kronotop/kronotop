/*
 * Copyright (c) 2023-2025 Burak Sezer
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

import com.kronotop.BaseMetadataStoreTest;
import com.kronotop.volume.NotEnoughSpaceException;
import com.kronotop.volume.VolumeConfiguration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class SegmentTest extends BaseMetadataStoreTest {
    Random random = new Random();

    private ByteBuffer randomBytes(int size) {
        byte[] b = new byte[size];
        random.nextBytes(b);
        return ByteBuffer.wrap(b);
    }

    private SegmentConfig getSegmentConfig() {
        String dataDir = context.getConfig().getString("data_dir");
        long size = VolumeConfiguration.segmentSize;
        return new SegmentConfig(1, dataDir, size);
    }

    @Test
    void test_append() throws IOException {
        Segment segment = new Segment(getSegmentConfig());
        try {
            ByteBuffer buffer = ByteBuffer.allocate(6).put("foobar".getBytes()).flip();
            assertDoesNotThrow(() -> segment.append(buffer));
        } finally {
            segment.close();
        }
    }

    @Test
    void test_append_NotEnoughSpaceException() throws IOException {
        Segment segment = new Segment(getSegmentConfig());
        try {
            long bufferSize = 100480;
            long segmentSize = VolumeConfiguration.segmentSize;
            long numIterations = segmentSize / bufferSize;

            for (int i = 1; i <= numIterations; i++) {
                assertDoesNotThrow(() -> segment.append(randomBytes((int) bufferSize)));
            }
            assertThrows(NotEnoughSpaceException.class, () -> segment.append(randomBytes((int) bufferSize)));
        } finally {
            segment.close();
        }
    }

    @Test
    void test_get() throws IOException, NotEnoughSpaceException {
        Segment segment = new Segment(getSegmentConfig());
        try {
            ByteBuffer buffer = ByteBuffer.allocate(6).put("foobar".getBytes()).flip();
            SegmentAppendResult segmentEntryMetadata = segment.append(buffer);
            ByteBuffer result = segment.get(segmentEntryMetadata.position(), segmentEntryMetadata.length());
            assertArrayEquals(buffer.array(), result.array());
        } finally {
            segment.close();
        }
    }

    @Test
    void test_getFreeBytes() throws IOException {
        Segment segment = new Segment(getSegmentConfig());
        try {
            long bufferSize = 10;
            int numIterations = 2;
            long total = 0;
            for (int i = 0; i < numIterations; i++) {
                assertDoesNotThrow(() -> segment.append(randomBytes((int) bufferSize)));
                total += bufferSize;
            }
            long segmentSize = VolumeConfiguration.segmentSize;
            assertEquals(segmentSize - total, segment.getFreeBytes());
        } finally {
            segment.close();
        }
    }

    @Test
    void test_flush() throws IOException {
        Segment segment = new Segment(getSegmentConfig());
        try {
            ByteBuffer buffer = ByteBuffer.allocate(6).put("foobar".getBytes()).flip();
            assertDoesNotThrow(() -> segment.append(buffer));
            assertDoesNotThrow(segment::flush);
        } finally {
            segment.close();
        }
    }

    @Test
    void test_close() throws IOException {
        Segment segment = new Segment(getSegmentConfig());
        ByteBuffer buffer = ByteBuffer.allocate(6).put("foobar".getBytes()).flip();
        assertDoesNotThrow(() -> segment.append(buffer));
        assertDoesNotThrow(segment::close);
    }

    @Test
    void test_reopen() throws IOException, NotEnoughSpaceException {
        SegmentAppendResult segmentEntryMetadata;
        ByteBuffer expected = ByteBuffer.allocate(6).put("foobar".getBytes()).flip();
        {
            Segment segment = new Segment(getSegmentConfig());
            segmentEntryMetadata = segment.append(expected);
            segment.close();
        }
        {
            Segment segment = new Segment(getSegmentConfig());
            try {
                ByteBuffer result = segment.get(segmentEntryMetadata.position(), segmentEntryMetadata.length());
                assertArrayEquals(expected.array(), result.array());
            } finally {
                segment.close();
            }
        }
    }

    @Test
    void test_insert() throws IOException {
        Segment segment = new Segment(getSegmentConfig());
        try {
            byte[] data = "foobar".getBytes();
            ByteBuffer buffer = ByteBuffer.allocate(data.length).put(data).flip();
            assertDoesNotThrow(() -> segment.insert(buffer, 100));
            ByteBuffer buf = segment.get(100, data.length);
            assertArrayEquals(data, buf.array());
        } finally {
            segment.close();
        }
    }
}