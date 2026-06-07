/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume.segment;

import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.volume.EntryOutOfBoundException;
import com.kronotop.volume.VolumeConfiguration;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class MappedSegmentTest extends BaseStandaloneInstanceTest {
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
    void shouldReadDataWrittenByFileSegment() throws Exception {
        // Behavior: MappedSegment reads back entries written by FileSegment with exact byte fidelity.
        SegmentConfig config = getSegmentConfig();
        byte[] helloBytes = "hello".getBytes();
        byte[] worldBytes = "world".getBytes();
        SegmentAppendResult helloResult;
        SegmentAppendResult worldResult;

        try (FileSegment writer = new FileSegment(config)) {
            helloResult = writer.append(ByteBuffer.wrap(helloBytes));
            worldResult = writer.append(ByteBuffer.wrap(worldBytes));
        }

        try (MappedSegment reader = new MappedSegment(config)) {
            ByteBuffer helloBuf = reader.get(helloResult.position(), helloResult.length());
            byte[] helloActual = new byte[helloBuf.remaining()];
            helloBuf.get(helloActual);
            assertArrayEquals(helloBytes, helloActual);

            ByteBuffer worldBuf = reader.get(worldResult.position(), worldResult.length());
            byte[] worldActual = new byte[worldBuf.remaining()];
            worldBuf.get(worldActual);
            assertArrayEquals(worldBytes, worldActual);
        }
    }

    @Test
    void shouldReadLargeDataWrittenByFileSegment() throws Exception {
        // Behavior: MappedSegment correctly reads large entries spanning multiple OS pages.
        SegmentConfig config = getSegmentConfig();
        ByteBuffer largeData = randomBytes(100_480);
        byte[] expected = largeData.array().clone();
        SegmentAppendResult result;

        try (FileSegment writer = new FileSegment(config)) {
            result = writer.append(largeData);
        }

        try (MappedSegment reader = new MappedSegment(config)) {
            ByteBuffer buf = reader.get(result.position(), result.length());
            byte[] actual = new byte[buf.remaining()];
            buf.get(actual);
            assertArrayEquals(expected, actual);
        }
    }

    @Test
    void shouldReadMultipleEntriesSequentially() throws Exception {
        // Behavior: All sequentially appended entries are individually readable through MappedSegment.
        SegmentConfig config = getSegmentConfig();
        int entryCount = 50;
        List<byte[]> entries = new ArrayList<>();
        List<SegmentAppendResult> results = new ArrayList<>();

        try (FileSegment writer = new FileSegment(config)) {
            for (int i = 0; i < entryCount; i++) {
                ByteBuffer data = randomBytes(256);
                entries.add(data.array().clone());
                results.add(writer.append(data));
            }
        }

        try (MappedSegment reader = new MappedSegment(config)) {
            for (int i = 0; i < entryCount; i++) {
                ByteBuffer buf = reader.get(results.get(i).position(), results.get(i).length());
                byte[] actual = new byte[buf.remaining()];
                buf.get(actual);
                assertArrayEquals(entries.get(i), actual);
            }
        }
    }

    @Test
    void shouldThrowEntryOutOfBoundException() throws Exception {
        // Behavior: Reading beyond segment boundaries throws EntryOutOfBoundException.
        SegmentConfig config = getSegmentConfig();
        try (FileSegment writer = new FileSegment(config)) {
            writer.append(ByteBuffer.wrap("data".getBytes()));
        }

        try (MappedSegment reader = new MappedSegment(config)) {
            long size = reader.getSize();
            assertThrows(EntryOutOfBoundException.class, () -> reader.get(size - 1, 2));
        }
    }

    @Test
    void shouldReturnCorrectConfig() throws Exception {
        // Behavior: getConfig() returns the SegmentConfig passed to the constructor.
        SegmentConfig config = getSegmentConfig();
        try (FileSegment writer = new FileSegment(config)) {
            writer.append(ByteBuffer.wrap("data".getBytes()));
        }

        try (MappedSegment reader = new MappedSegment(config)) {
            assertEquals(config, reader.getConfig());
        }
    }

    @Test
    void shouldReturnCorrectId() throws Exception {
        // Behavior: id() returns the segment id from the config.
        SegmentConfig config = getSegmentConfig();
        try (FileSegment writer = new FileSegment(config)) {
            writer.append(ByteBuffer.wrap("data".getBytes()));
        }

        try (MappedSegment reader = new MappedSegment(config)) {
            assertEquals(config.id(), reader.id());
        }
    }

    @Test
    void shouldReturnCorrectSize() throws Exception {
        // Behavior: getSize() equals the pre-allocated segment size.
        SegmentConfig config = getSegmentConfig();
        try (FileSegment writer = new FileSegment(config)) {
            writer.append(ByteBuffer.wrap("data".getBytes()));
        }

        try (MappedSegment reader = new MappedSegment(config)) {
            assertEquals(VolumeConfiguration.segmentSize, reader.getSize());
        }
    }

    @Test
    void shouldCloseMappedSegment() throws Exception {
        // Behavior: MappedSegment closes without error.
        SegmentConfig config = getSegmentConfig();
        try (FileSegment writer = new FileSegment(config)) {
            writer.append(ByteBuffer.wrap("data".getBytes()));
        }

        MappedSegment reader = new MappedSegment(config);
        assertDoesNotThrow(reader::close);
    }

    @Test
    void shouldDestroySegmentFile() throws Exception {
        // Behavior: destroy() closes the segment and deletes the underlying file.
        SegmentConfig config = getSegmentConfig();
        try (FileSegment writer = new FileSegment(config)) {
            writer.append(ByteBuffer.wrap("data".getBytes()));
        }

        Path segmentPath = SegmentUtil.getFilePath(config.dataDir(), config.id());
        assertTrue(Files.exists(segmentPath));

        MappedSegment reader = new MappedSegment(config);
        reader.destroy();

        assertFalse(Files.exists(segmentPath));
    }

    @Test
    void shouldReturnReadOnlyByteBuffer() throws Exception {
        // Behavior: ByteBuffer returned by get() is read-only.
        SegmentConfig config = getSegmentConfig();
        try (FileSegment writer = new FileSegment(config)) {
            writer.append(ByteBuffer.wrap("data".getBytes()));
        }

        try (MappedSegment reader = new MappedSegment(config)) {
            ByteBuffer buf = reader.get(0, 4);
            assertTrue(buf.isReadOnly());
            assertThrows(ReadOnlyBufferException.class, () -> buf.put((byte) 0));
        }
    }
}
