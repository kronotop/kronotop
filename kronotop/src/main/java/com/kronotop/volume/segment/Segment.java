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

package com.kronotop.volume.segment;

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import com.kronotop.common.KronotopException;
import com.kronotop.volume.EntryOutOfBoundException;
import com.kronotop.volume.NotEnoughSpaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Segment {
    public static final int SEGMENT_NAME_SIZE = 19;
    private static final Logger LOGGER = LoggerFactory.getLogger(Segment.class);
    private final SegmentConfig config;
    private final String name;
    private final SegmentMetadata metadata;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final RandomAccessFile segmentFile;
    private final RandomAccessFile metadataFile;
    private volatile boolean flushed = true;

    public Segment(SegmentConfig config) throws IOException {
        this.config = config;
        this.name = generateName(config.id());
        this.metadataFile = createOrOpenSegmentMetadataFile();
        this.metadata = createOrDecodeSegmentMetadata(config.id());
        this.segmentFile = createOrOpenSegmentFile();
    }

    /**
     * Extracts the segment ID from a given name.
     *
     * @param name the name from which to extract the segment ID
     * @return the extracted segment ID
     */
    public static long extractIdFromName(String name) {
        String segmentId = CharMatcher.is('0').trimLeadingFrom(name);
        if (segmentId.isEmpty()) {
            return 0L;
        }
        try {
            return Long.parseLong(segmentId);
        } catch (NumberFormatException e) {
            throw new KronotopException("Invalid segment ID: " + segmentId, e);
        }
    }

    public static String generateName(long id) {
        return Strings.padStart(Long.toString(id), 19, '0');
    }

    public String getName() {
        return name;
    }

    public long getFreeBytes() {
        lock.readLock().lock();
        try {
            return metadata.getSize() - metadata.getPosition();
        } finally {
            lock.readLock().unlock();
        }
    }

    public long getSize() {
        // No need to use lock here. Size is a final property.
        return metadata.getSize();
    }

    private void setFlushed(boolean flushed) {
        this.flushed = flushed;
    }

    private SegmentMetadata createOrDecodeSegmentMetadata(long id) throws IOException {
        if (this.metadataFile.getChannel().size() == 0) {
            // Empty file. Create a new SegmentMetadata.
            return new SegmentMetadata(id, config.size());
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(SegmentMetadata.HEADER_SIZE);
            this.metadataFile.getChannel().read(buffer);
            return SegmentMetadata.decode(buffer);
        }
    }

    private RandomAccessFile createOrOpenSegmentMetadataFile() throws IOException {
        Path path = Path.of(config.dataDir(), "segments", name + ".metadata");
        Files.createDirectories(path.getParent());
        try {
            return new RandomAccessFile(path.toFile(), "rw");
        } catch (FileNotFoundException e) {
            // This should not be possible.
            throw new KronotopException(e);
        }
    }

    private RandomAccessFile createOrOpenSegmentFile() throws IOException {
        Path path = Path.of(config.dataDir(), "segments", getName());
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

    public SegmentAppendResult append(ByteBuffer entry) throws NotEnoughSpaceException, IOException {
        try {
            long position = forwardMetadataPosition(entry.remaining());
            int length = segmentFile.getChannel().write(entry, position);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("{} bytes has been written to segment {}", length, getName());
            }
            return new SegmentAppendResult(position, length);
        } finally {
            // Now this segment requires a flush.
            setFlushed(false);
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void updateMetadataPosition(long position) throws NotEnoughSpaceException, IOException {
        lock.writeLock().lock();
        try {
            if (position > metadata.getSize()) {
                throw new NotEnoughSpaceException();
            }
            metadata.setPosition(position);
            ByteBuffer buffer = metadata.encode();
            metadataFile.getChannel().write(buffer, 0);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void insert(ByteBuffer entry, long position) throws IOException, NotEnoughSpaceException {
        try {
            int length = segmentFile.getChannel().write(entry, position);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("{} bytes has been inserted to segment {}", length, getName());
            }
            updateMetadataPosition(position);
        } finally {
            // Now this segment requires a flush.
            setFlushed(false);
        }
    }

    public ByteBuffer get(long position, long length) throws IOException {
        if (position + length > metadata.getSize()) {
            String message = String.format("position: %d, length: %d but size: %d", position, length, metadata.getSize());
            throw new EntryOutOfBoundException(message);
        }
        ByteBuffer buffer = ByteBuffer.allocate((int) length);
        int nr = segmentFile.getChannel().read(buffer, position);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{} bytes has been read from segment {}", nr, getName());
        }
        return buffer.flip();
    }

    public synchronized void flush(boolean metaData) throws IOException {
        if (flushed) {
            // Already flushed
            return;
        }
        try {
            segmentFile.getChannel().force(metaData);
        } catch (ClosedChannelException e) {
            // Ignore it and continue flushing the other file.
        }
        try {
            metadataFile.getChannel().force(metaData);
        } catch (ClosedChannelException e) {
            // Ignore it.
        }
        setFlushed(true);
    }

    public void close() throws IOException {
        flush(true);
        segmentFile.close();
        metadataFile.close();
    }
}
