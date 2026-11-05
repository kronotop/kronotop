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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class BlobChunkWriterTest extends BaseBlobChunkTest {
    private static final String NAME = "vector-index.bin";
    private static final int FILE_SIZE = 2560;

    @Test
    void shouldWriteAllChunksAndRoundTrip(@TempDir Path src, @TempDir Path out) throws Exception {
        // Behavior: a 2560 byte file is split with chunkSize 1024, so chunkCount is 3. Every chunk is written in its own
        // transaction and committed. Reading the prefix back into a directory in pages of 2 recreates the file, and its
        // bytes are equal to the original.
        Path path = writeRandomFile(src, NAME, FILE_SIZE);
        try (BlobChunkWriter writer = new BlobChunkWriter(volume, prefix, path, NAME, CHUNK_SIZE)) {
            assertEquals(3, writer.chunkCount());
            for (int i = 0; i < writer.chunkCount(); i++) {
                writeChunk(writer, i);
            }
        }

        readAll(out, 2);

        assertArrayEquals(Files.readAllBytes(path), Files.readAllBytes(out.resolve(NAME)));
    }

    @Test
    void shouldRejectChunkIndexOutOfRange(@TempDir Path dir) throws Exception {
        // Behavior: a writer over a 2560 byte file with chunkSize 1024 has 3 chunks. write with index 3 and with index -1
        // throws IndexOutOfBoundsException, and the transaction is never committed, so nothing reaches the volume.
        Path path = writeRandomFile(dir, NAME, FILE_SIZE);
        try (BlobChunkWriter writer = new BlobChunkWriter(volume, prefix, path, NAME, CHUNK_SIZE);
             Transaction tr = context.getTransactionManager().create()) {
            assertThrows(IndexOutOfBoundsException.class, () -> writer.write(tr, 3));
            assertThrows(IndexOutOfBoundsException.class, () -> writer.write(tr, -1));
        }
    }
}
