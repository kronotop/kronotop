/*
 * Copyright (c) 2023-2026 Burak Sezer
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

import com.kronotop.KronotopException;
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
 * File-backed segment that stores entries in a pre-allocated {@link RandomAccessFile}.
 * Supports concurrent appends with atomic position tracking and batched flush-to-disk.
 */
public class FileSegment implements WritableSegment {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSegment.class);

    /**
     * Configuration for this segment (ID, data directory, size).
     */
    private final SegmentConfig config;

    /**
     * Total size of the segment file in bytes (pre-allocated).
     */
    private final long size;

    /**
     * Lock protecting flush operations to prevent concurrent flushes.
     */
    private final Object flushLock = new Object();

    /**
     * RandomAccessFile handle for reading and writing segment data.
     */
    private final RandomAccessFile file;

    /**
     * Counter tracking the number of pending writes that need to be flushed.
     */
    private final AtomicInteger flushCounter = new AtomicInteger(0);

    /**
     * Current write position in the segment (atomically updated during appends).
     */
    private final AtomicLong atomicPosition = new AtomicLong(0);

    /**
     * Constructs a new FileSegment starting at position 0.
     *
     * <p>Creates or opens a segment file and initializes the write position to 0.
     * Use this constructor for newly created segments.</p>
     *
     * @param config the segment configuration (ID, data directory, size)
     * @throws IOException if an I/O error occurs while creating or opening the segment file
     */
    public FileSegment(SegmentConfig config) throws IOException {
        // for clarity
        this(config, 0L);
    }

    /**
     * Constructs a new FileSegment with a specific starting position.
     *
     * <p>Creates or opens a segment file and initializes the write position to the specified value.
     * Use this constructor when reopening existing segments where the write position needs to be
     * restored from metadata. The file is pre-allocated to the configured size if it doesn't
     * already exist or is smaller.</p>
     *
     * @param config   the segment configuration (ID, data directory, size)
     * @param position the initial write position in bytes (0 for new segments)
     * @throws IOException if an I/O error occurs while creating or opening the segment file
     */
    public FileSegment(SegmentConfig config, long position) throws IOException {
        this.config = config;
        this.file = createOrOpenSegmentFile();
        this.size = this.file.length();
        this.atomicPosition.set(position);
    }

    /**
     * Returns the configuration of this segment.
     *
     * @return the SegmentConfig containing ID, data directory, and size
     */
    public SegmentConfig getConfig() {
        return config;
    }

    public long id() {
        return config.id();
    }

    /**
     * Calculates the remaining free space in the segment.
     *
     * <p>Free bytes are calculated as: total size - current write position.
     * When free bytes reach 0, the segment is full and Volume will create a new segment.</p>
     *
     * @return the number of free bytes available for new entries
     */
    public long getFreeBytes() {
        return getSize() - atomicPosition.get();
    }

    /**
     * Returns the total size of the segment file.
     *
     * @return the pre-allocated size of the segment file in bytes
     */
    public long getSize() {
        return size;
    }

    /**
     * Constructs the file system path for this segment.
     *
     * <p>Path format: {dataDir}/segments/{segmentName}</p>
     *
     * @return the Path to the segment file
     */
    private Path getSegmentFilePath() {
        return Path.of(config.dataDir(), SegmentUtil.DIRECTORY, SegmentUtil.generateFileName(config.id()));
    }

    /**
     * Creates or opens the segment file with the configured size.
     *
     * <p>Creates the segments directory if needed, opens the file with read-write access,
     * and pre-allocates to the configured size (extends but never truncates) to ensure
     * contiguous disk space and avoid fragmentation.</p>
     *
     * @return a RandomAccessFile handle to the segment file
     * @throws IOException if an I/O error occurs during file creation or opening
     */
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
     * Atomically advances the write position by the specified length and returns the old position.
     *
     * <p>Uses an atomic compare-and-swap operation to ensure thread-safe position updates during
     * concurrent append operations. The returned position is the location where the caller should
     * write the entry data.</p>
     *
     * @param length the number of bytes to reserve in the segment
     * @return the position where the entry should be written (before advancing)
     * @throws NotEnoughSpaceException if there is insufficient space to reserve the requested bytes
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
     * Appends an entry to the segment at the next available position.
     *
     * <p>This is the primary write operation for segments. Atomically reserves space, writes the
     * entry data, and increments the flush counter. Thread-safe for concurrent appends without
     * external synchronization.</p>
     *
     * <p>Data is written to the OS page cache but NOT immediately synced to disk; call
     * {@link #flush()} to ensure durability. If the entry doesn't fit, the caller (typically
     * Volume) should create a new segment and retry.</p>
     *
     * @param entry the entry data to append
     * @return a SegmentAppendResult containing the position and length of the appended entry
     * @throws NotEnoughSpaceException if there is insufficient space to append the entry
     * @throws IOException             if an I/O error occurs during the write operation
     */
    public SegmentAppendResult append(ByteBuffer entry) throws NotEnoughSpaceException, IOException {
        try {
            long position = forwardMetadataPosition(entry.remaining());
            int length = 0;
            while (entry.hasRemaining()) {
                length += file.getChannel().write(entry, position + length);
            }
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("{} bytes has been written to segment {}", length, id());
            }
            return new SegmentAppendResult(position, length);
        } finally {
            // Now this segment requires a flush.
            flushCounter.incrementAndGet();
        }
    }

    /**
     * Inserts an entry at a specific position in the segment (used for replication).
     *
     * <p>Unlike {@link #append(ByteBuffer)}, this method does NOT advance the write position.
     * It writes directly to the specified position, which is used during replication to
     * maintain identical segment layouts between primary and standby instances.</p>
     *
     * <p>During replication, the standby reads segment logs from FoundationDB containing exact
     * positions from the primary and uses this method to write entries at those positions,
     * ensuring byte-for-byte identical segment files.</p>
     *
     * @param entry    the entry data to insert
     * @param position the exact position in the segment where the data should be written
     * @throws IOException             if an I/O error occurs during the write operation
     * @throws NotEnoughSpaceException if the position + entry length exceeds the segment size
     */
    public void insert(ByteBuffer entry, long position) throws IOException, NotEnoughSpaceException {
        try {
            int length = 0;
            while (entry.hasRemaining()) {
                length += file.getChannel().write(entry, position + length);
            }
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("{} bytes has been inserted to segment {}", length, id());
            }
        } finally {
            // Now this segment requires a flush.
            flushCounter.incrementAndGet();
        }
    }

    /**
     * Flushes pending writes to disk to ensure durability.
     *
     * <p>Synchronizes pending writes from OS page cache to storage. Uses a flush counter to
     * skip unnecessary sync operations when no writes have occurred since the last flush.
     * Thread-safe with serialized flush operations (concurrent callers wait or skip).</p>
     *
     * <p>After a successful flush, all previous writes are guaranteed to survive system crashes
     * (assuming the storage device honors sync semantics). When the sync fails the exception is
     * propagated and the flush counter is preserved, so the segment stays dirty and the next
     * flush retries the sync.</p>
     *
     * @throws IOException if an I/O error occurs during the sync operation
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
            try {
                sync();
            } catch (IOException e) {
                LOGGER.error("Calling sync failed", e);
                throw e;
            }
            flushCounter.updateAndGet(waiting -> waiting - count);
        }
    }

    /**
     * Synchronizes the underlying file descriptor with the storage device.
     *
     * <p>Isolated so that durability failures can be exercised in tests without
     * inducing a real device-level fsync failure.</p>
     *
     * @throws IOException if the sync operation fails
     */
    protected void sync() throws IOException {
        file.getFD().sync();
    }

    /**
     * Closes the segment by flushing pending writes and closing the file descriptor.
     *
     * <p>After calling close(), the segment can no longer be used for write operations.</p>
     *
     * @throws IOException if an error occurs during the flush operation or while closing the file
     */
    public void close() throws IOException {
        flush();
        file.close();
    }

    /**
     * Destroys the segment file from the disk.
     *
     * <p>Used during cleanup operations to remove stale segments with zero cardinality.
     * Closes the segment first, then deletes the file.</p>
     *
     * <p>Should only be called after ensuring the segment is no longer referenced by any
     * entry metadata in FoundationDB.</p>
     *
     * @return the absolute path of the deleted segment file
     * @throws IOException       if an I/O error occurs during the close operation
     * @throws KronotopException if the file cannot be deleted
     */
    public String destroy() throws IOException {
        close();

        Path path = getSegmentFilePath();
        if (!path.toFile().delete()) {
            throw new KronotopException("File could not be deleted: " + path);
        }
        return path.toString();
    }
}
