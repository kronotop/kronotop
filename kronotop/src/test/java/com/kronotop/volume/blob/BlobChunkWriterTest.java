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
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.BaseVolumeIntegrationTest;
import com.kronotop.volume.Prefix;
import com.kronotop.volume.VolumeSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class BlobChunkWriterTest extends BaseVolumeIntegrationTest {
    private static final String NAME = "vector-index.bin";
    private static final int CHUNK_SIZE = 1024;
    private static final int FILE_SIZE = 2560;
    private final Prefix prefix = Prefix.fromBytes(
            UUID.randomUUID().toString().getBytes()
    );

    private Path writeRandomFile(Path dir) throws IOException {
        Path path = dir.resolve(NAME);
        Files.write(path, randomBytes(FILE_SIZE).array());
        return path;
    }

    @Test
    void shouldWriteAllChunksAndRoundTrip(@TempDir Path dir) throws Exception {
        // Behavior: a 2560 byte file is split with chunkSize 1024, so chunkCount is 3. Every chunk is written in its own
        // transaction and committed. Reading each entry back gives a header with the blob name, the chunk index and
        // chunkCount 3; the payloads concatenated in index order are equal to the original file, and the last chunk
        // carries the remaining 512 bytes.
        Path path = writeRandomFile(dir);
        byte[] expected = Files.readAllBytes(path);

        List<Versionstamp> keys = new ArrayList<>();
        try (BlobChunkWriter writer = new BlobChunkWriter(volume, prefix, path, NAME, CHUNK_SIZE)) {
            assertEquals(3, writer.chunkCount());
            for (int i = 0; i < writer.chunkCount(); i++) {
                try (Transaction tr = context.getTransactionManager().create()) {
                    AppendResult result = writer.write(tr, i);
                    tr.commit().join();
                    keys.add(result.getVersionstampedKeys()[0]);
                }
            }
        }

        ByteBuffer actual = ByteBuffer.allocate(FILE_SIZE);
        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            for (int i = 0; i < keys.size(); i++) {
                ByteBuffer entry = volume.get(session, keys.get(i));
                BlobChunkHeader header = BlobChunkHeader.readHeader(entry);
                assertEquals(new BlobChunkHeader(NAME, i, 3), header);
                assertEquals(i == 2 ? 512 : CHUNK_SIZE, entry.remaining());
                actual.put(entry);
            }
        }
        assertArrayEquals(expected, actual.array());
    }

    @Test
    void shouldRejectChunkIndexOutOfRange(@TempDir Path dir) throws Exception {
        // Behavior: a writer over a 2560 byte file with chunkSize 1024 has 3 chunks. write with index 3 and with index -1
        // throws IndexOutOfBoundsException, and the transaction is never committed, so nothing reaches the volume.
        Path path = writeRandomFile(dir);
        try (BlobChunkWriter writer = new BlobChunkWriter(volume, prefix, path, NAME, CHUNK_SIZE);
             Transaction tr = context.getTransactionManager().create()) {
            assertThrows(IndexOutOfBoundsException.class, () -> writer.write(tr, 3));
            assertThrows(IndexOutOfBoundsException.class, () -> writer.write(tr, -1));
        }
    }
}
