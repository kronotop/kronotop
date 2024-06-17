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

public class Segment {
    private static final Logger LOGGER = LoggerFactory.getLogger(Volume.class);
    private final String rootPath;
    private final SegmentMetadata metadata;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final RandomAccessFile segment;
    private final RandomAccessFile metadataFile;

    public Segment(Context context, long id) throws IOException {
        this.rootPath = context.getConfig().getString("volumes.root_path");
        long size = context.getConfig().getLong("volumes.segment_size");
        this.metadata = new SegmentMetadata(id, size);
        this.segment = createOrOpenSegment();
        this.metadataFile = createOrOpenSegmentMetadata();
        System.out.println(this.metadataFile.length());
    }

    public String getName() {
        return Strings.padStart(Long.toString(metadata.getId()), 19, '0');
    }

    public SegmentMetadata getMetadata() {
        return metadata;
    }

    private RandomAccessFile createOrOpenSegmentMetadata() throws IOException {
        Path path = Path.of(rootPath, "segments", getName() + ".metadata");
        Files.createDirectories(path.getParent());
        try {
            return new RandomAccessFile(path.toFile(), "rw");
        } catch (FileNotFoundException e) {
            // This should not be possible.
            throw new KronotopException(e);
        }
    }

    private RandomAccessFile createOrOpenSegment() throws IOException {
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

    private void updateSegmentMetadata() throws IOException {
        ByteBuffer buffer = metadata.encode();
        metadataFile.getChannel().write(buffer, 0);
    }

    public EntryMetadata append(ByteBuffer entry) throws NotEnoughSpaceException, IOException {
        lock.writeLock().lock();
        try {
            long position = metadata.getCurrentPosition();
            if (position + entry.position() > metadata.getSize()) {
                throw new NotEnoughSpaceException();
            }
            int length = segment.getChannel().write(entry, position);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format("%d bytes has been written to segment %s", length, getName()));
            }
            metadata.setCurrentPosition(position + length);
            updateSegmentMetadata();
            return new EntryMetadata(getName(), position, length);
        } finally {
            lock.writeLock().unlock();
        }
    }

    ByteBuffer get(long position, long length) throws IOException {
        lock.readLock().lock();
        try {
            if (position + length > metadata.getSize()) {
                throw new RuntimeException("metadata mismatch");
            }
            ByteBuffer buffer = ByteBuffer.allocate((int) length);
            int nr = segment.getChannel().read(buffer, position);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format("%d bytes has been read from segment %s", nr, getName()));
            }
            return buffer;
        } finally {
            lock.readLock().unlock();
        }
    }
}
