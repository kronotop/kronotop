/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.volume;

import com.google.common.base.Strings;
import com.kronotop.Context;
import com.kronotop.common.KronotopException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class Segment {
    private static final Logger LOGGER = LoggerFactory.getLogger(Segment.class);
    private final Context context;
    private final String name;
    private final SegmentMetadata metadata;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final RandomAccessFile segmentFile;
    private final RandomAccessFile metadataFile;

    Segment(Context context, long id) throws IOException {
        this.context = context;
        this.name = Strings.padStart(Long.toString(id), 19, '0');
        this.metadataFile = createOrOpenSegmentMetadataFile();
        this.metadata = createOrDecodeSegmentMetadata(id);
        this.segmentFile = createOrOpenSegmentFile();
    }

    String getName() {
        return name;
    }

    long getFreeBytes() {
        lock.readLock().lock();
        try {
            return metadata.getSize() - metadata.getPosition();
        } finally {
            lock.readLock().unlock();
        }
    }

    private SegmentMetadata createOrDecodeSegmentMetadata(long id) throws IOException {
        long size = context.getConfig().getLong("volumes.segment_size");
        if (this.metadataFile.getChannel().size() == 0) {
            // Empty file. Create a new SegmentMetadata.
            return new SegmentMetadata(id, size);
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(SegmentMetadata.HEADER_SIZE);
            this.metadataFile.getChannel().read(buffer);
            return SegmentMetadata.decode(buffer);
        }
    }

    private RandomAccessFile createOrOpenSegmentMetadataFile() throws IOException {
        String rootPath = context.getConfig().getString("volumes.root_path");
        Path path = Path.of(rootPath, "segments", name + ".metadata");
        Files.createDirectories(path.getParent());
        try {
            return new RandomAccessFile(path.toFile(), "rw");
        } catch (FileNotFoundException e) {
            // This should not be possible.
            throw new KronotopException(e);
        }
    }

    private RandomAccessFile createOrOpenSegmentFile() throws IOException {
        String rootPath = context.getConfig().getString("volumes.root_path");
        Path path = Path.of(rootPath, "segments", getName());
        Files.createDirectories(path.getParent());
        try {
            RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw");
            if (file.length() < metadata.getSize()) {
                // Do not truncate the file, only extend it.
                file.setLength(metadata.getSize());
            }
            return file;
        } catch (FileNotFoundException e) {
            // This should not be possible.
            throw new KronotopException(e);
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private long forwardMetadataPosition(int length) throws NotEnoughSpaceException, IOException {
        lock.writeLock().lock();
        try {
            long position = metadata.getPosition();
            if (position + length > metadata.getSize()) {
                throw new NotEnoughSpaceException();
            }
            metadata.setPosition(position + length);
            ByteBuffer buffer = metadata.encode();
            metadataFile.getChannel().write(buffer, 0);
            return position;
        } finally {
            lock.writeLock().unlock();
        }
    }

    EntryMetadata append(ByteBuffer entry) throws NotEnoughSpaceException, IOException {
        long position = forwardMetadataPosition(entry.position());
        int length = segmentFile.getChannel().write(entry, position);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("%d bytes has been written to segment %s", length, getName()));
        }
        return new EntryMetadata(getName(), position, length);
    }

    ByteBuffer get(long position, long length) throws IOException {
        if (position + length > metadata.getSize()) {
            throw new RuntimeException("metadata mismatch");
        }
        ByteBuffer buffer = ByteBuffer.allocate((int) length);
        int nr = segmentFile.getChannel().read(buffer, position);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("%d bytes has been read from segment %s", nr, getName()));
        }
        return buffer;
    }

    void flush() throws IOException {
        segmentFile.getChannel().force(true);
        metadataFile.getChannel().force(true);
    }

    void close() throws IOException {
        flush();
        segmentFile.close();
        metadataFile.close();
    }
}
