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

import com.kronotop.BaseMetadataStoreTest;
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

    @Test
    void test_append() throws IOException {
        Segment segment = new Segment(context, 1);
        try {
            ByteBuffer buffer = ByteBuffer.allocate(6).put("foobar".getBytes()).flip();
            assertDoesNotThrow(() -> segment.append(buffer));
        } finally {
            segment.close();
        }
    }

    @Test
    void test_append_NotEnoughSpaceException() throws IOException {
        Segment segment = new Segment(context, 1);
        try {
            long bufferSize = 100480;
            long segmentSize = context.getConfig().getLong("volumes.segment_size");
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
        Segment segment = new Segment(context, 1);
        try {
            ByteBuffer buffer = ByteBuffer.allocate(6).put("foobar".getBytes()).flip();
            EntryMetadata entryMetadata = segment.append(buffer);

            assertEquals(segment.getName(), entryMetadata.segment());
            ByteBuffer result = segment.get(entryMetadata.position(), entryMetadata.length());
            assertArrayEquals(buffer.array(), result.array());
        } finally {
            segment.close();
        }
    }

    @Test
    void test_getFreeBytes() throws IOException {
        Segment segment = new Segment(context, 1);
        try {
            long bufferSize = 10;
            int numIterations = 2;
            long total = 0;
            for (int i = 0; i < numIterations; i++) {
                assertDoesNotThrow(() -> segment.append(randomBytes((int) bufferSize)));
                total += bufferSize;
            }
            long segmentSize = context.getConfig().getLong("volumes.segment_size");
            assertEquals(segmentSize - total, segment.getFreeBytes());
        } finally {
            segment.close();
        }
    }

    @Test
    void test_flush() throws IOException {
        Segment segment = new Segment(context, 1);
        try {
            ByteBuffer buffer = ByteBuffer.allocate(6).put("foobar".getBytes()).flip();
            assertDoesNotThrow(() -> segment.append(buffer));
            assertDoesNotThrow(() -> segment.flush(true));
        } finally {
            segment.close();
        }
    }

    @Test
    void test_close() throws IOException {
        Segment segment = new Segment(context, 1);
        ByteBuffer buffer = ByteBuffer.allocate(6).put("foobar".getBytes()).flip();
        assertDoesNotThrow(() -> segment.append(buffer));
        assertDoesNotThrow(segment::close);
    }

    @Test
    void test_reopen() throws IOException, NotEnoughSpaceException {
        EntryMetadata entryMetadata;
        ByteBuffer expected = ByteBuffer.allocate(6).put("foobar".getBytes()).flip();
        {
            Segment segment = new Segment(context, 1);
            entryMetadata = segment.append(expected);
            segment.close();
        }
        {
            Segment segment = new Segment(context, 1);
            try {
                ByteBuffer result = segment.get(entryMetadata.position(), entryMetadata.length());
                assertArrayEquals(expected.array(), result.array());
            } finally {
                segment.close();
            }
        }
    }
}