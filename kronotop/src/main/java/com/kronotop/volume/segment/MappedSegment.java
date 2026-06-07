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

import com.kronotop.KronotopException;
import com.kronotop.volume.EntryOutOfBoundException;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A read-only, memory-mapped implementation of {@link ReadableSegment}.
 *
 * <p>Opens an existing segment file (previously written by {@link FileSegment}) and maps it
 * into memory via {@code mmap} for zero-copy reads. The entire file is mapped once at
 * construction time; individual {@link #get} calls return slices of the mapped region
 * without any heap allocation or data copying.</p>
 *
 * <p>Thread-safe: uses {@link Arena#ofShared()} so any thread can read concurrently.</p>
 */
public class MappedSegment implements ReadableSegment {
    private final SegmentConfig config;
    private final Arena arena;
    private final MemorySegment mappedMemory;
    private final long size;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public MappedSegment(SegmentConfig config) throws IOException {
        this.config = config;
        Path path = SegmentUtil.getFilePath(config.dataDir(), config.id());

        this.arena = Arena.ofShared();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            this.size = channel.size();
            this.mappedMemory = channel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
        } catch (IOException e) {
            arena.close();
            throw e;
        }
    }

    @Override
    public SegmentConfig getConfig() {
        return config;
    }

    @Override
    public long id() {
        return config.id();
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public ByteBuffer get(long position, long length) throws IOException {
        if (position + length > size) {
            String message = String.format("position: %d, length: %d but size: %d", position, length, size);
            throw new EntryOutOfBoundException(message);
        }
        return mappedMemory.asSlice(position, length).asByteBuffer().asReadOnlyBuffer();
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            arena.close();
        }
    }

    @Override
    public String destroy() throws IOException {
        close();
        Path path = SegmentUtil.getFilePath(config.dataDir(), config.id());
        if (!path.toFile().delete()) {
            throw new KronotopException("File could not be deleted: " + path);
        }
        return path.toString();
    }
}
