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
import com.kronotop.volume.BaseVolumeIntegrationTest;
import com.kronotop.volume.Prefix;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

class BaseBlobChunkTest extends BaseVolumeIntegrationTest {
    protected static final int CHUNK_SIZE = 1024;
    protected final Prefix prefix = new Prefix(UUID.randomUUID().toString().getBytes());

    protected Path writeRandomFile(Path dir, String name, int size) throws IOException {
        Path path = dir.resolve(name);
        Files.write(path, randomBytes(size).array());
        return path;
    }

    protected void writeChunk(BlobChunkWriter writer, int chunkIndex) throws IOException {
        try (Transaction tr = context.getTransactionManager().create()) {
            writer.write(tr, chunkIndex);
            tr.commit().join();
        }
    }

    protected void readInto(Path outDir) throws Exception {
        try (Transaction tr = context.getTransactionManager().create()) {
            try (DirectoryBlobChunkSink sink = new DirectoryBlobChunkSink(outDir)) {
                new BlobChunkReader(volume, prefix).read(tr, sink);
            }
        }
    }
}
