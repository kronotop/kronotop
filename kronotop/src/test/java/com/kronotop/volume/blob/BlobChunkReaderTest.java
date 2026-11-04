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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class BlobChunkReaderTest extends BaseBlobChunkTest {

    @Test
    void shouldSeparateFilesAndPlaceChunksByOffset(@TempDir Path src, @TempDir Path out) throws Exception {
        // Behavior: two files share one prefix: "a.bin" is 2560 bytes (3 chunks) and "b.bin" is 1536 bytes (2 chunks).
        // Their chunks are written mixed and out of order, each in its own transaction: a2, b1, a0, b0, a1. Reading the
        // prefix into a directory produces both files, and each one is byte for byte equal to its original.
        Path a = writeRandomFile(src, "a.bin", 2560);
        Path b = writeRandomFile(src, "b.bin", 1536);
        try (BlobChunkWriter writerA = new BlobChunkWriter(volume, prefix, a, "a.bin", CHUNK_SIZE);
             BlobChunkWriter writerB = new BlobChunkWriter(volume, prefix, b, "b.bin", CHUNK_SIZE)) {
            writeChunk(writerA, 2);
            writeChunk(writerB, 1);
            writeChunk(writerA, 0);
            writeChunk(writerB, 0);
            writeChunk(writerA, 1);
        }

        readInto(out);

        assertArrayEquals(Files.readAllBytes(a), Files.readAllBytes(out.resolve("a.bin")));
        assertArrayEquals(Files.readAllBytes(b), Files.readAllBytes(out.resolve("b.bin")));
    }

    @Test
    void shouldFailOnIncompleteFile(@TempDir Path src, @TempDir Path out) throws Exception {
        // Behavior: only chunks 0 and 2 of a 3 chunk file are written. Reading the prefix succeeds, but closing the
        // directory sink throws IllegalStateException and the message names the incomplete file.
        Path path = writeRandomFile(src, "a.bin", 2560);
        try (BlobChunkWriter writer = new BlobChunkWriter(volume, prefix, path, "a.bin", CHUNK_SIZE)) {
            writeChunk(writer, 0);
            writeChunk(writer, 2);
        }

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> readInto(out));
        assertTrue(e.getMessage().contains("a.bin"));
    }
}
