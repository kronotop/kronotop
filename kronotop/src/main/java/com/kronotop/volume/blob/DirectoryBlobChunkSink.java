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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Writes chunks into files under a directory. Every chunk goes to the file named in its header, at the offset in its
 * header, so chunks may arrive in any order. A chunk that arrives more than once is written again at the same offset
 * and counts once. A file is closed when every offset has arrived, and chunks for a closed file are ignored.
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
        } else if (file.done) {
            return;
        }
        long position = header.offset();
        while (payload.hasRemaining()) {
            position += file.channel.write(payload, position);
        }
        file.offsets.add(header.offset());
        if (file.offsets.size() == file.chunkCount) {
            file.channel.force(true);
            file.channel.close();
            file.done = true;
        }
    }

    /**
     * Closes the files that are still open.
     *
     * @throws IOException           if a file cannot be closed
     * @throws IncompleteBlobException if some files did not receive all of their chunks
     */
    @Override
    public void close() throws IOException {
        List<String> incomplete = new ArrayList<>();
        for (Map.Entry<String, OpenFile> entry : files.entrySet()) {
            if (entry.getValue().done) {
                continue;
            }
            entry.getValue().channel.close();
            incomplete.add(entry.getKey());
        }
        if (!incomplete.isEmpty()) {
            throw new IncompleteBlobException("incomplete files: " + incomplete);
        }
    }

    private static final class OpenFile {
        private final FileChannel channel;
        private final int chunkCount;
        // Explicit deduplication: a retried batch appends the same chunk twice.
        private final Set<Long> offsets = new HashSet<>();
        private boolean done;

        private OpenFile(FileChannel channel, int chunkCount) {
            this.channel = channel;
            this.chunkCount = chunkCount;
        }
    }
}
