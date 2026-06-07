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

package com.kronotop.bucket.vector;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.pipeline.DocumentLocation;
import com.kronotop.volume.EntryMetadata;
import org.bson.types.ObjectId;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static com.kronotop.bucket.vector.OnDiskVectorGraphIndexMetadataWriter.*;

/**
 * Memory-mapped reader for on-disk vector graph index metadata files (*.vmeta).
 * Provides O(1) ordinal-to-metadata lookup and O(log N) ObjectId-to-ordinal lookup.
 * Thread-safe: uses a shared Arena so any thread can read concurrently.
 *
 * <p>The on-disk format uses native byte order. Files are node-local (written and read on the
 * same machine) and the target platform is exclusively little-endian (x86-64, ARM64).</p>
 *
 * <h2>File format</h2>
 *
 * <p>All multi-byte integers are in native byte order. Fields within each section are laid out
 * so that longs fall on 8-byte boundaries and ints on 4-byte boundaries.</p>
 *
 * <pre>{@code
 * HEADER (40 bytes)
 * ──────────────────────────────────────────
 * offset  size  field
 *      0     4  magic (0x4B564D44)
 *      4     4  version (always 1)
 *      8     4  count (number of ObjectId entries)
 *     12     4  maxOrdinal
 *     16     4  objectIdSectionOffset
 *     20    12  latestVersionstamp
 *     32     4  deletedCount
 *     36     4  reserved
 *
 * ORDINAL SECTION — (maxOrdinal + 1) × 64 bytes
 * ──────────────────────────────────────────
 * offset  size  field
 *      0     8  segmentId   (long)
 *      8     8  prefix      (byte[8])
 *     16     8  position    (long)
 *     24     8  length      (long)
 *     32     8  handle      (long)
 *     40     4  shardId     (int)
 *     44     1  alive       (0 = dead, 1 = alive)
 *     45    12  objectId    (byte[12])
 *     57     7  reserved
 *
 * OBJECTID SECTION — count × 16 bytes, sorted by ObjectId (unsigned)
 * ──────────────────────────────────────────
 * offset  size  field
 *      0    12  objectId    (byte[12])
 *     12     4  ordinal     (int)
 * }</pre>
 */
public class OnDiskVectorGraphIndexMetadata implements VectorGraphIndexMetadata, Closeable {
    private static final int OBJECTID_SIZE = 12;
    private static final int VERSIONSTAMP_SIZE = 12;
    private final Arena arena;
    private final MemorySegment mappedMemory;
    private final int count;
    private final int maxOrdinal;
    private final int objectIdSectionOffset;
    private final Versionstamp latestVersionstamp;
    private final AtomicInteger deletedCount;
    private final AtomicInteger flushCounter = new AtomicInteger(0);
    private final Object flushLock = new Object();

    public OnDiskVectorGraphIndexMetadata(Path path) throws IOException {
        this.arena = Arena.ofShared();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            this.mappedMemory = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size(), arena);
        } catch (IOException e) {
            arena.close();
            throw e;
        }

        int magic = mappedMemory.get(ValueLayout.JAVA_INT, 0);
        if (magic != MAGIC) {
            throw new IOException("Invalid vmeta file: bad magic number");
        }
        int version = mappedMemory.get(ValueLayout.JAVA_INT, 4);
        if (version != VERSION) {
            throw new IOException("Unsupported vmeta version: " + version);
        }
        this.count = mappedMemory.get(ValueLayout.JAVA_INT, 8);
        this.maxOrdinal = mappedMemory.get(ValueLayout.JAVA_INT, 12);
        this.objectIdSectionOffset = mappedMemory.get(ValueLayout.JAVA_INT, 16);

        byte[] versionstampBytes = new byte[VERSIONSTAMP_SIZE];
        MemorySegment.copy(mappedMemory, ValueLayout.JAVA_BYTE, 20, versionstampBytes, 0, VERSIONSTAMP_SIZE);
        this.latestVersionstamp = Versionstamp.fromBytes(versionstampBytes);
        this.deletedCount = new AtomicInteger(mappedMemory.get(ValueLayout.JAVA_INT, 32));
    }

    public int getDeletedCount() {
        return deletedCount.get();
    }

    public int getCount() {
        return count;
    }

    public Versionstamp getLatestVersionstamp() {
        return latestVersionstamp;
    }

    /**
     * Looks up the DocumentLocation (ObjectId + shard ID + EntryMetadata) for a given ordinal. Returns null if the
     * ordinal is out of range or the slot is marked as deleted.
     */
    public DocumentLocation findDocumentLocation(int ordinal) {
        if (ordinal < 0 || ordinal > maxOrdinal) {
            return null;
        }
        int offset = HEADER_SIZE + ordinal * ORDINAL_SLOT_SIZE;
        byte alive = mappedMemory.get(ValueLayout.JAVA_BYTE, offset + 44);
        if (alive == 0) {
            return null;
        }
        long segmentId = mappedMemory.get(ValueLayout.JAVA_LONG, offset);
        byte[] prefix = new byte[8];
        MemorySegment.copy(mappedMemory, ValueLayout.JAVA_BYTE, offset + 8, prefix, 0, 8);
        long position = mappedMemory.get(ValueLayout.JAVA_LONG, offset + 16);
        long length = mappedMemory.get(ValueLayout.JAVA_LONG, offset + 24);
        long handle = mappedMemory.get(ValueLayout.JAVA_LONG, offset + 32);
        int shardId = mappedMemory.get(ValueLayout.JAVA_INT, offset + 40);
        byte[] objectIdBytes = new byte[OBJECTID_SIZE];
        MemorySegment.copy(mappedMemory, ValueLayout.JAVA_BYTE, offset + 45, objectIdBytes, 0, OBJECTID_SIZE);
        ObjectId objectId = new ObjectId(objectIdBytes);
        EntryMetadata em = new EntryMetadata(segmentId, prefix, position, length, handle);

        return new DocumentLocation(objectId, shardId, em);
    }

    /**
     * Looks up the ordinal for a given ObjectId using binary search.
     * Returns -1 if the ObjectId is not found.
     */
    public int findOrdinal(ObjectId objectId) {
        byte[] target = objectId.toByteArray();
        int low = 0;
        int high = count - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int entryOffset = objectIdSectionOffset + mid * OBJECTID_ENTRY_SIZE;

            byte[] candidate = new byte[OBJECTID_SIZE];
            MemorySegment.copy(mappedMemory, ValueLayout.JAVA_BYTE, entryOffset, candidate, 0, OBJECTID_SIZE);

            int cmp = Arrays.compareUnsigned(candidate, target);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mappedMemory.get(ValueLayout.JAVA_INT, entryOffset + OBJECTID_SIZE);
            }
        }
        return -1;
    }

    /**
     * Marks the node at the given ordinal as deleted by setting its ALIVE byte to 0.
     */
    public void markDeleted(int ordinal) {
        int offset = HEADER_SIZE + ordinal * ORDINAL_SLOT_SIZE;
        mappedMemory.set(ValueLayout.JAVA_BYTE, offset + 44, (byte) 0);
        deletedCount.incrementAndGet();
        flushCounter.incrementAndGet();
    }

    /**
     * Forces pending alive-byte changes to disk.
     */
    public void flush() {
        if (flushCounter.get() == 0) {
            return;
        }
        synchronized (flushLock) {
            int count = flushCounter.get();
            if (count == 0) {
                return;
            }
            mappedMemory.set(ValueLayout.JAVA_INT, 32, deletedCount.get());
            mappedMemory.force();
            flushCounter.updateAndGet(waiting -> waiting - count);
        }
    }

    @Override
    public void close() {
        arena.close();
    }
}
