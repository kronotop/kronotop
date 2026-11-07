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
import com.apple.foundationdb.tuple.Versionstamp;
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
        // prefix into a directory in pages of 2 produces both files, and each one is byte for byte equal to its original.
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

        readAll(out, 2);

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

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> readAll(out, 2));
        assertTrue(e.getMessage().contains("a.bin"));
    }

    @Test
    void shouldPageThroughAllChunks(@TempDir Path src, @TempDir Path out) throws Exception {
        // Behavior: a 5120 byte file is written as 5 chunks of 1024 bytes. read is called four times with limit 2,
        // each call in its own transaction. The first two calls each feed 2 chunks and return a key, the third feeds
        // the last chunk and returns a key, the fourth returns null. The output file is byte for byte equal to the
        // original.
        Path path = writeRandomFile(src, "a.bin", 5120);
        try (BlobChunkWriter writer = new BlobChunkWriter(volume, prefix, path, "a.bin", CHUNK_SIZE)) {
            for (int i = 0; i < writer.chunkCount(); i++) {
                writeChunk(writer, i);
            }
        }

        BlobChunkReader reader = new BlobChunkReader(volume, prefix);
        try (DirectoryBlobChunkSink sink = new DirectoryBlobChunkSink(out)) {
            Versionstamp cursor = null;
            for (int page = 0; page < 3; page++) {
                try (Transaction tr = context.getTransactionManager().create()) {
                    cursor = reader.read(tr, cursor, 2, sink);
                }
                assertNotNull(cursor, "page " + page);
            }
            try (Transaction tr = context.getTransactionManager().create()) {
                assertNull(reader.read(tr, cursor, 2, sink));
            }
        }

        assertArrayEquals(Files.readAllBytes(path), Files.readAllBytes(out.resolve("a.bin")));
    }

    @Test
    void shouldRestoreFileWhenBatchIsDuplicatedMidFile(@TempDir Path src, @TempDir Path out) throws Exception {
        // Behavior: a 4096 byte file (4 chunks) is written as 0, 1, 0, 1, 2, 3, each chunk in its own transaction. This
        // is what a retried batch leaves behind after commit_unknown_result. Reading the prefix in pages of 2 does not
        // close the file early on the repeated chunks, does not throw, and the output file is byte for byte equal to
        // the original.
        Path path = writeRandomFile(src, "a.bin", 4096);
        try (BlobChunkWriter writer = new BlobChunkWriter(volume, prefix, path, "a.bin", CHUNK_SIZE)) {
            writeChunk(writer, 0);
            writeChunk(writer, 1);
            writeChunk(writer, 0);
            writeChunk(writer, 1);
            writeChunk(writer, 2);
            writeChunk(writer, 3);
        }

        readAll(out, 2);

        assertArrayEquals(Files.readAllBytes(path), Files.readAllBytes(out.resolve("a.bin")));
    }

    @Test
    void shouldIgnoreDuplicateChunkAfterFileIsComplete(@TempDir Path src, @TempDir Path out) throws Exception {
        // Behavior: a 3072 byte file (3 chunks) is written as 0, 1, 2, 0. The file is complete after the third chunk.
        // The late copy of chunk 0 is ignored: it does not reopen and truncate the finished file, reading in pages of 2
        // does not throw, and the output file is byte for byte equal to the original.
        Path path = writeRandomFile(src, "a.bin", 3072);
        try (BlobChunkWriter writer = new BlobChunkWriter(volume, prefix, path, "a.bin", CHUNK_SIZE)) {
            writeChunk(writer, 0);
            writeChunk(writer, 1);
            writeChunk(writer, 2);
            writeChunk(writer, 0);
        }

        readAll(out, 2);

        assertArrayEquals(Files.readAllBytes(path), Files.readAllBytes(out.resolve("a.bin")));
    }

    @Test
    void shouldStillFailWhenDuplicatesDoNotCoverAllChunks(@TempDir Path src, @TempDir Path out) throws Exception {
        // Behavior: a 3072 byte file (3 chunks) is written as 0, 0, 2. Three chunks arrive but chunk 1 never does.
        // Duplicates do not count toward completion, so closing the directory sink throws IllegalStateException and
        // the message names the incomplete file.
        Path path = writeRandomFile(src, "a.bin", 3072);
        try (BlobChunkWriter writer = new BlobChunkWriter(volume, prefix, path, "a.bin", CHUNK_SIZE)) {
            writeChunk(writer, 0);
            writeChunk(writer, 0);
            writeChunk(writer, 2);
        }

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> readAll(out, 2));
        assertTrue(e.getMessage().contains("a.bin"));
    }
}
