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

package com.kronotop.volume.blob;

import com.apple.foundationdb.Transaction;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.Prefix;
import com.kronotop.volume.Volume;
import com.kronotop.volume.VolumeSession;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Splits a memory-mapped file into fixed-size chunks and appends each chunk, prefixed with a {@link BlobChunkHeader},
 * as one Volume entry. The caller owns the transaction and the commit of every chunk.
 */
public final class BlobChunkWriter implements AutoCloseable {
    private final Volume volume;
    private final Prefix prefix;
    private final String name;
    private final int chunkSize;
    private final int chunkCount;
    private final Arena arena;
    private final MemorySegment mappedMemory;

    /**
     * Maps the file and computes the chunk count.
     *
     * @param volume    target volume
     * @param prefix    prefix the chunks are appended under
     * @param path      file to split
     * @param name      name of the file, stored in every chunk header.
     * @param chunkSize payload size of a chunk in bytes
     * @throws IOException              if the file cannot be mapped
     * @throws IllegalArgumentException if the file is empty or a chunk would exceed {@link Volume#ENTRY_SIZE_LIMIT}
     */
    public BlobChunkWriter(Volume volume, Prefix prefix, Path path, String name, int chunkSize) throws IOException {
        this.prefix = prefix;
        this.volume = volume;
        this.name = name;
        this.chunkSize = chunkSize;
        int headerLength = new BlobChunkHeader(name, 0, 1).encode().length;
        if (headerLength + chunkSize > Volume.ENTRY_SIZE_LIMIT) {
            throw new IllegalArgumentException("chunk size exceeds entry size limit");
        }
        this.arena = Arena.ofShared();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long size = channel.size();
            if (size == 0) {
                throw new IllegalArgumentException("empty file: " + path);
            }
            this.chunkCount = Math.toIntExact(Math.ceilDiv(size, chunkSize));
            this.mappedMemory = channel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
        } catch (IOException | RuntimeException e) {
            arena.close();
            throw e;
        }
    }

    public int chunkCount() {
        return chunkCount;
    }

    /**
     * Appends one chunk to the volume in the given transaction.
     *
     * @param tr         transaction the append is recorded in; the caller commits it
     * @param chunkIndex zero based chunk to write
     * @return result of the append
     * @throws IOException               if the append fails
     * @throws IndexOutOfBoundsException if chunkIndex is outside [0, chunkCount)
     */
    public AppendResult write(Transaction tr, int chunkIndex) throws IOException {
        if (chunkIndex < 0 || chunkIndex >= chunkCount) {
            throw new IndexOutOfBoundsException("chunk index " + chunkIndex + " out of range for " + chunkCount + " chunks");
        }
        long offset = (long) chunkIndex * chunkSize;
        int length = (int) Math.min(chunkSize, mappedMemory.byteSize() - offset);
        byte[] header = new BlobChunkHeader(name, offset, chunkCount).encode();
        ByteBuffer buffer = ByteBuffer.allocate(header.length + length);
        buffer.put(header);
        MemorySegment.copy(mappedMemory, ValueLayout.JAVA_BYTE, offset, buffer.array(), header.length, length);
        buffer.position(header.length + length).flip();
        return volume.append(new VolumeSession(tr, prefix), buffer);
    }

    @Override
    public void close() {
        arena.close();
    }
}
