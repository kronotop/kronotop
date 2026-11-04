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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

/**
 * Writes chunks into files under a directory. Every chunk goes to the file named in its header, at the offset in its
 * header, so chunks may arrive in any order. A file is closed when all of its chunks have arrived.
 */
public final class DirectoryBlobChunkSink implements BlobChunkSink {
    private final Path directory;
    private final Map<String, OpenFile> files = new HashMap<>();

    public DirectoryBlobChunkSink(Path directory) {
        this.directory = directory;
    }

    @Override
    public void accept(BlobChunkHeader header, ByteBuffer payload) throws IOException {
        OpenFile file = files.get(header.name());
        if (file == null) {
            FileChannel channel = FileChannel.open(
                    directory.resolve(header.name()),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING
            );
            file = new OpenFile(channel, header.chunkCount());
            files.put(header.name(), file);
        }
        long position = header.offset();
        while (payload.hasRemaining()) {
            position += file.channel.write(payload, position);
        }
        file.remaining--;
        if (file.remaining == 0) {
            files.remove(header.name());
            file.channel.force(true);
            file.channel.close();
        }
    }

    /**
     * Closes the files that are still open.
     *
     * @throws IOException           if a file cannot be closed
     * @throws IllegalStateException if some files did not receive all of their chunks
     */
    @Override
    public void close() throws IOException {
        for (OpenFile file : files.values()) {
            file.channel.close();
        }
        if (!files.isEmpty()) {
            throw new IllegalStateException("incomplete files: " + files.keySet());
        }
    }

    private static final class OpenFile {
        private final FileChannel channel;
        private int remaining;

        private OpenFile(FileChannel channel, int remaining) {
            this.channel = channel;
            this.remaining = remaining;
        }
    }
}
