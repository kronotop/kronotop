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
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;

/**
 * Represents a file segment used to store and manage data in a structured manner.
 * The segment manages its metadata and file operations, including appending, inserting,
 * retrieving, and managing data consistency with locking mechanisms.
 */
public class Segment {
    public static final int SEGMENT_NAME_SIZE = 19;
    public static final String SEGMENTS_DIRECTORY = "segments";
    public static final String SEGMENT_METADATA_FILE_EXTENSION = "metadata";
    private static final Logger LOGGER = LoggerFactory.getLogger(Segment.class);
    private final SegmentConfig config;
    private final String name;
    private final SegmentMetadata metadata;
    private final StampedLock lock = new StampedLock();
    private final StampedLock flushLock = new StampedLock();
    private final RandomAccessFile segmentFile;
    private final RandomAccessFile metadataFile;
    private final AtomicInteger flushCounter = new AtomicInteger(0);

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

    public SegmentConfig getConfig() {
        return config;
    }

    public String getName() {
        return name;
    }

    public long getFreeBytes() {
        long stamp = lock.readLock();
        try {
            return metadata.getSize() - metadata.getPosition();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public long getSize() {
        // No need to use lock here. Size is a final property.
        return metadata.getSize();
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

    private Path getSegmentMetadataFilePath() {
        return Path.of(config.dataDir(), SEGMENTS_DIRECTORY, getName() + "." + SEGMENT_METADATA_FILE_EXTENSION);
    }

    private Path getSegmentFilePath() {
        return Path.of(config.dataDir(), SEGMENTS_DIRECTORY, getName());
    }

    private RandomAccessFile createOrOpenSegmentMetadataFile() throws IOException {
        Path path = getSegmentMetadataFilePath();
        Files.createDirectories(path.getParent());
        try {
            return new RandomAccessFile(path.toFile(), "rw");
        } catch (FileNotFoundException e) {
            // This should not be possible.
            throw new KronotopException(e);
        }
    }

    private RandomAccessFile createOrOpenSegmentFile() throws IOException {
        Path path = getSegmentFilePath();
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
        long stamp = lock.writeLock();
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
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Appends the given entry to the segment file.
     * <p>
     * This method writes the provided {@code ByteBuffer} entry to the next available position
     * in the segment file and updates the segment metadata accordingly. If the operation is
     * successful, the segment's flushed state is set to {@code false}, indicating that
     * the segment requires a flush for data persistence.
     *
     * @param entry the data to append, represented as a {@code ByteBuffer}
     * @return a {@code SegmentAppendResult} containing the position where the entry was written
     * and the length of the appended data
     * @throws NotEnoughSpaceException if there is insufficient space in the segment to append the data
     * @throws IOException             if an I/O error occurs during the append operation
     */
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
            flushCounter.incrementAndGet();
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void updateMetadataPosition(long position) throws NotEnoughSpaceException, IOException {
        long stamp = lock.writeLock();
        try {
            if (position > metadata.getSize()) {
                throw new NotEnoughSpaceException();
            }
            metadata.setPosition(position);
            ByteBuffer buffer = metadata.encode();
            metadataFile.getChannel().write(buffer, 0);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Inserts a given entry at a specified position within the segment.
     * <p>
     * This method writes the provided {@code ByteBuffer} entry to the segment file at the given position.
     * It also updates the metadata position to reflect the insertion.
     * If the operation is successful, the segment's flushed state is set to {@code false}, indicating
     * that the segment requires a flush to ensure data persistence.
     *
     * @param entry    the data to insert, represented as a {@code ByteBuffer}
     * @param position the position within the segment where the data will be inserted
     * @throws IOException             if an I/O error occurs during the insertion process
     * @throws NotEnoughSpaceException if there is insufficient space in the segment to accommodate the data
     */
    public void insert(ByteBuffer entry, long position) throws IOException, NotEnoughSpaceException {
        try {
            int length = segmentFile.getChannel().write(entry, position);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("{} bytes has been inserted to segment {}", length, getName());
            }
            updateMetadataPosition(position);
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

    /**
     * Ensures that all data written to the segment and metadata files is persisted to the underlying
     * storage device.
     * <p>
     * This method performs a synchronization operation to flush any modifications to the
     * segment and metadata files onto durable storage. It acquires a write lock to guarantee thread
     * safety during the flushing process. If no data is pending for flush (indicated by the flushCounter
     * being zero), the method returns immediately.
     * <p>
     * The method tries to sync the file descriptors of both the segment and metadata files. If any of
     * these operations fail due to a {@code SyncFailedException}, an error message is logged, and the
     * operation continues to sync the other file(s). The flush operation will only be considered
     * successful if both files are synced without any exceptions.
     * <p>
     * Upon a successful flush, the method updates the flushCounter to reflect the number of flushed
     * operations since the last invocation of this method.
     *
     * @throws IOException if an I/O error occurs during the flush operation
     */
    public void flush() throws IOException {
        if (flushCounter.get() == 0) {
            // Already flushed
            return;
        }

        long stamp = flushLock.writeLock();
        try {
            int count = flushCounter.get();
            if (count == 0) {
                return;
            }
            boolean success = true;
            try {
                segmentFile.getFD().sync();
            } catch (SyncFailedException e) {
                LOGGER.error("Calling sync on failed", e);
                // Continue syncing the other file or files.
                success = false;
            }
            try {
                metadataFile.getFD().sync();
            } catch (SyncFailedException e) {
                LOGGER.error("Calling sync on failed", e);
                success = false;
            }
            if (success) {
                flushCounter.updateAndGet(v -> v - count);
            }
        } finally {
            flushLock.unlockWrite(stamp);
        }
    }

    /**
     * Closes the segment and its associated metadata file to release any system resources held by them.
     * <p>
     * This method ensures that any buffered data is flushed to the segment and metadata files
     * before closing them. It calls the {@code flush()} method to perform the flush operation
     * and then closes both the segment file and metadata file.
     *
     * @throws IOException if an I/O error occurs while flushing or closing the files
     */
    public void close() throws IOException {
        flush();
        segmentFile.close();
        metadataFile.close();
    }

    /**
     * Deletes the segment and its associated metadata files from the storage.
     * <p>
     * This method attempts to delete the segment file and the segment metadata file
     * associated with the current segment. If any of the files cannot be deleted,
     * a {@code KronotopException} is thrown. The method returns a list of file paths
     * that were successfully deleted.
     *
     * @return a list of strings representing the paths of the deleted files
     * @throws IOException       if an I/O error occurs during the closing of the segment
     * @throws KronotopException if a file cannot be deleted
     */
    public List<String> delete() throws IOException {
        close();

        List<String> result = new ArrayList<>();
        for (Path path : List.of(getSegmentFilePath(), getSegmentMetadataFilePath())) {
            if (!path.toFile().delete()) {
                throw new KronotopException("File could not be deleted: " + path);
            }
            result.add(path.toString());
        }
        return result;
    }
}
