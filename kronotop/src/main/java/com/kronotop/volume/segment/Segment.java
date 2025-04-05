/*
 * Copyright (c) 2023-2025 Burak Sezer
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
import com.kronotop.KronotopException;
import com.kronotop.volume.EntryOutOfBoundException;
import com.kronotop.volume.NotEnoughSpaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a segment of data with capabilities for managing and manipulating
 * binary data, appending entries, retrieving data, and ensuring persistence
 * in the storage system. The Segment class is designed to handle efficient
 * storage and retrieval of fixed-size or variable-sized entries using a
 * configurable segment file.
 */
public class Segment {
    public static final int SEGMENT_NAME_SIZE = 19;
    public static final String SEGMENTS_DIRECTORY = "segments";
    private static final Logger LOGGER = LoggerFactory.getLogger(Segment.class);
    private final SegmentConfig config;
    private final String name;
    private final long size;
    private final Object flushLock = new Object();
    private final RandomAccessFile file;
    private final AtomicInteger flushCounter = new AtomicInteger(0);
    private final AtomicLong atomicPosition = new AtomicLong(0);

    public Segment(SegmentConfig config) throws IOException {
        // for clarity
        this(config, 0L);
    }

    public Segment(SegmentConfig config, long position) throws IOException {
        this.config = config;
        this.name = generateName(config.id());
        this.file = createOrOpenSegmentFile();
        this.size = this.file.length();
        this.atomicPosition.set(position);
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

    /**
     * Generates a name for a segment based on the provided identifier.
     * The generated name is a zero-padded string representation of the given id,
     * with a length of 19 characters.
     *
     * @param id the unique identifier for which the segment name is generated
     * @return the zero-padded string representation of the id
     */
    public static String generateName(long id) {
        return Strings.padStart(Long.toString(id), 19, '0');
    }

    /**
     * Retrieves the configuration of the segment.
     *
     * @return the configuration of the segment as a {@code SegmentConfig} object
     */
    public SegmentConfig getConfig() {
        return config;
    }

    /**
     * Retrieves the name of the segment.
     *
     * @return the name of the segment
     */
    public String getName() {
        return name;
    }

    /**
     * Calculates the remaining free space in the segment file.
     *
     * @return the number of free bytes available in the segment file
     */
    public long getFreeBytes() {
        return getSize() - atomicPosition.get();
    }

    /**
     * Retrieves the size of the segment file.
     *
     * @return the size of the segment file in bytes
     */
    public long getSize() {
        return size;
    }

    private Path getSegmentFilePath() {
        return Path.of(config.dataDir(), SEGMENTS_DIRECTORY, getName());
    }

    private RandomAccessFile createOrOpenSegmentFile() throws IOException {
        Path path = getSegmentFilePath();
        Files.createDirectories(path.getParent());
        try {
            RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw");
            if (file.length() < config.size()) {
                // Do not truncate the file, only extend it.
                file.setLength(config.size());
            }
            return file;
        } catch (FileNotFoundException e) {
            // This should not be possible.
            throw new KronotopException(e);
        }
    }

    /**
     * Advances the metadata position in the segment configuration by the specified length.
     * If the resulting position exceeds the total size of the segment, a {@code NotEnoughSpaceException}
     * is thrown, indicating insufficient space.
     *
     * @param length the number of bytes by which the metadata position should be advanced
     * @return the updated metadata position after adding the specified length
     * @throws NotEnoughSpaceException if there is insufficient space to advance the metadata position
     */
    private long forwardMetadataPosition(int length) throws NotEnoughSpaceException {
        try {
            return atomicPosition.getAndUpdate(position -> {
                if (position + length > size) {
                    throw new RuntimeException(new NotEnoughSpaceException());
                }
                return position + length;
            });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof NotEnoughSpaceException) {
                throw (NotEnoughSpaceException) e.getCause();
            }
            throw e;
        }
    }

    /**
     * Appends the specified entry to the segment.
     * This method writes the data represented by the provided {@code ByteBuffer}
     * to the segment file and updates the metadata position accordingly.
     * If successful, a {@code SegmentAppendResult} containing the position
     * and the length of the appended entry is returned. Additionally, the method
     * marks the segment as requiring a flush to ensure data persistence.
     *
     * @param entry the data to append, represented as a {@code ByteBuffer}
     * @return a {@code SegmentAppendResult} containing the position and length of the appended entry
     * @throws NotEnoughSpaceException if there is insufficient space to append the entry
     * @throws IOException             if an I/O error occurs during the append operation
     */
    public SegmentAppendResult append(ByteBuffer entry) throws NotEnoughSpaceException, IOException {
        try {
            long position = forwardMetadataPosition(entry.remaining());
            int length = file.getChannel().write(entry, position);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("{} bytes has been written to segment {}", length, getName());
            }
            return new SegmentAppendResult(position, length);
        } finally {
            // Now this segment requires a flush.
            flushCounter.incrementAndGet();
        }
    }

    /**
     * Inserts a {@code ByteBuffer} entry into the segment at the specified position.
     * This method writes data to the segment file and marks the segment as requiring a flush.
     *
     * @param entry    the data to insert, represented as a {@code ByteBuffer}
     * @param position the position in the segment where the data should be written
     * @throws IOException             if an I/O error occurs during the write operation
     * @throws NotEnoughSpaceException if there is insufficient space to insert the entry
     */
    public void insert(ByteBuffer entry, long position) throws IOException, NotEnoughSpaceException {
        try {
            int length = file.getChannel().write(entry, position);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("{} bytes has been inserted to segment {}", length, getName());
            }
        } finally {
            // Now this segment requires a flush.
            flushCounter.incrementAndGet();
        }
    }

    /**
     * Reads a portion of the segment's data from the specified position with the given length.
     *
     * @param position the starting position in the segment from where the data should be read
     * @param length   the number of bytes to read from the segment
     * @return a {@code ByteBuffer} containing the data read from the segment
     * @throws IOException              if an I/O error occurs during reading
     * @throws EntryOutOfBoundException if the specified range (position + length) exceeds the size of the segment
     */
    public ByteBuffer get(long position, long length) throws IOException {
        if (position + length > config.size()) {
            String message = String.format("position: %d, length: %d but size: %d", position, length, config.size());
            throw new EntryOutOfBoundException(message);
        }
        ByteBuffer buffer = ByteBuffer.allocate((int) length);
        int nr = file.getChannel().read(buffer, position);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{} bytes has been read from segment {}", nr, getName());
        }
        return buffer.flip();
    }

    /**
     * Ensures that any pending changes to the segment data are written to the storage
     * and the file descriptor is synchronized. This method performs the flush operation
     * in a thread-safe manner, using a write lock to protect the process. If no changes
     * need to be flushed (i.e., the flush counter is zero), the method returns immediately.
     * <p>
     * During the flush operation, this method attempts to synchronize the underlying
     * file descriptor. If this operation fails, an error is logged, and the pending
     * flush count is not updated. On successful synchronization, the flush counter
     * is decremented by the number of pending changes that were written to the storage.
     *
     * @throws IOException if an I/O error occurs while synchronizing the file descriptor
     */
    public void flush() throws IOException {
        if (flushCounter.get() == 0) {
            // Already flushed
            return;
        }

        synchronized (flushLock) {
            int count = flushCounter.get();
            if (count == 0) {
                return;
            }
            boolean success = true;
            try {
                file.getFD().sync();
            } catch (Exception e) {
                LOGGER.error("Calling sync failed", e);
                success = false;
            }
            if (success) {
                flushCounter.updateAndGet(waiting -> waiting - count);
            }
        }
    }

    /**
     * Closes the segment by ensuring all pending data is flushed to storage
     * and then closing the underlying file descriptor.
     * <p>
     * This method first invokes the {@code flush()} method to guarantee that
     * any unwritten changes to the segment or metadata are persisted to the
     * underlying storage. After completing the flush, it closes the file
     * descriptor associated with the segment file.
     * <p>
     * If any I/O errors occur during the flush or file closing process, they
     * are propagated to the caller as {@code IOException}.
     *
     * @throws IOException if an error occurs during the flush operation or
     *                     while closing the file descriptor
     */
    public void close() throws IOException {
        flush();
        file.close();
    }

    /**
     * Deletes the segment file associated with this segment.
     * This method first ensures that the segment is properly closed
     * by invoking the {@code close()} method. After that, it attempts
     * to delete the file identified by the segment's file path.
     * If the file cannot be deleted, a {@code KronotopException} is thrown.
     *
     * @return the string representation of the path of the deleted file
     * @throws IOException       if an I/O error occurs during the close operation
     * @throws KronotopException if the file cannot be deleted
     */
    public String delete() throws IOException {
        close();

        Path path = getSegmentFilePath();
        if (!path.toFile().delete()) {
            throw new KronotopException("File could not be deleted: " + path);
        }
        return path.toString();
    }
}
