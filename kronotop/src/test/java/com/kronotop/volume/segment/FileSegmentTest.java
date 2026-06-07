/*
 * Copyright (c) 2023-2026 Burak Sezer
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

import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.volume.NotEnoughSpaceException;
import com.kronotop.volume.VolumeConfiguration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class FileSegmentTest extends BaseStandaloneInstanceTest {
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
    void shouldAppendDataToSegment() throws IOException {
        try (FileSegment segment = new FileSegment(getSegmentConfig())) {
            ByteBuffer buffer = ByteBuffer.allocate(6).put("foobar".getBytes()).flip();
            assertDoesNotThrow(() -> segment.append(buffer));
        }
    }

    @Test
    void shouldThrowNotEnoughSpaceExceptionWhenSegmentFull() throws IOException {
        try (FileSegment segment = new FileSegment(getSegmentConfig())) {
            long bufferSize = 100480;
            long segmentSize = VolumeConfiguration.segmentSize;
            long numIterations = segmentSize / bufferSize;

            for (int i = 1; i <= numIterations; i++) {
                assertDoesNotThrow(() -> segment.append(randomBytes((int) bufferSize)));
            }
            assertThrows(NotEnoughSpaceException.class, () -> segment.append(randomBytes((int) bufferSize)));
        }
    }

    @Test
    void shouldCalculateFreeBytesCorrectly() throws IOException {
        try (FileSegment segment = new FileSegment(getSegmentConfig())) {
            long bufferSize = 10;
            int numIterations = 2;
            long total = 0;
            for (int i = 0; i < numIterations; i++) {
                assertDoesNotThrow(() -> segment.append(randomBytes((int) bufferSize)));
                total += bufferSize;
            }
            long segmentSize = VolumeConfiguration.segmentSize;
            assertEquals(segmentSize - total, segment.getFreeBytes());
        }
    }

    @Test
    void shouldFlushSegment() throws IOException {
        try (FileSegment segment = new FileSegment(getSegmentConfig())) {
            ByteBuffer buffer = ByteBuffer.allocate(6).put("foobar".getBytes()).flip();
            assertDoesNotThrow(() -> segment.append(buffer));
            assertDoesNotThrow(segment::flush);
        }
    }

    @Test
    void shouldInsertDataAtSpecificPosition() throws IOException {
        // Behavior: insert writes data at the specified position, verifiable by reading the file directly
        SegmentConfig config = getSegmentConfig();
        try (FileSegment segment = new FileSegment(config)) {
            byte[] data = "foobar".getBytes();
            ByteBuffer buffer = ByteBuffer.allocate(data.length).put(data).flip();
            assertDoesNotThrow(() -> segment.insert(buffer, 100));
            segment.flush();

            Path filePath = SegmentUtil.getFilePath(config.dataDir(), config.id());
            try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
                ByteBuffer buf = ByteBuffer.allocate(data.length);
                raf.getChannel().read(buf, 100);
                buf.flip();
                assertEquals(ByteBuffer.wrap(data), buf);
            }
        }
    }

    @Test
    void shouldRethrowIOExceptionWhenFsyncFails() throws IOException {
        // Behavior: when the underlying fsync fails, flush() propagates the IOException instead of swallowing it
        try (FailingSyncSegment segment = new FailingSyncSegment(getSegmentConfig())) {
            ByteBuffer buffer = ByteBuffer.allocate(6).put("foobar".getBytes()).flip();
            assertDoesNotThrow(() -> segment.append(buffer));
            assertThrows(IOException.class, segment::flush);

            // Let the try-with-resources close flush cleanly; the segment is still dirty.
            segment.failSync = false;
        }
    }

    @Test
    void shouldKeepSegmentDirtyWhenFsyncFails() throws IOException {
        // Behavior: a failed fsync preserves the flush counter, so the next flush retries the sync
        try (FailingSyncSegment segment = new FailingSyncSegment(getSegmentConfig())) {
            ByteBuffer buffer = ByteBuffer.allocate(6).put("foobar".getBytes()).flip();
            assertDoesNotThrow(() -> segment.append(buffer));

            assertThrows(IOException.class, segment::flush);
            assertEquals(1, segment.syncAttempts);

            // The segment is still dirty: the next flush must attempt the sync again.
            segment.failSync = false;
            assertDoesNotThrow(segment::flush);
            assertEquals(2, segment.syncAttempts);
        }
    }

    /**
     * Test double that simulates an fsync failure without inducing a real device-level error.
     */
    static class FailingSyncSegment extends FileSegment {
        boolean failSync = true;
        int syncAttempts = 0;

        FailingSyncSegment(SegmentConfig config) throws IOException {
            super(config);
        }

        @Override
        protected void sync() throws IOException {
            syncAttempts++;
            if (failSync) {
                throw new IOException("simulated fsync failure");
            }
            super.sync();
        }
    }
}