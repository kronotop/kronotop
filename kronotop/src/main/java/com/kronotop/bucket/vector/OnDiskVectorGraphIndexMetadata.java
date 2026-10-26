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

import static com.kronotop.bucket.vector.OnDiskVectorGraphIndexMetadataWriter.*;

public class OnDiskVectorGraphIndexMetadata implements VectorGraphIndexMetadata, Closeable {
    private static final int OBJECTID_SIZE = 12;
    private static final int VERSIONSTAMP_SIZE = 12;
    private final Arena arena;
    private final MemorySegment mappedMemory;
    private final int count;
    private final int maxOrdinal;
    private final int objectIdSectionOffset;
    private final Versionstamp latestVersionstamp;

    public OnDiskVectorGraphIndexMetadata(Path path) throws IOException {
        this.arena = Arena.ofShared();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            this.mappedMemory = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
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
    }

    public Versionstamp getLatestVersionstamp() {
        return latestVersionstamp;
    }

    public DocumentLocation findDocumentLocation(int ordinal) {
        if (ordinal < 0 || ordinal > maxOrdinal) {
            return null;
        }
        int offset = HEADER_SIZE + ordinal * ORDINAL_SLOT_SIZE;
        long segmentId = mappedMemory.get(ValueLayout.JAVA_LONG, offset);
        byte[] prefix = new byte[8];
        MemorySegment.copy(mappedMemory, ValueLayout.JAVA_BYTE, offset + 8, prefix, 0, 8);
        long position = mappedMemory.get(ValueLayout.JAVA_LONG, offset + 16);
        long length = mappedMemory.get(ValueLayout.JAVA_LONG, offset + 24);
        long handle = mappedMemory.get(ValueLayout.JAVA_LONG, offset + 32);
        int shardId = mappedMemory.get(ValueLayout.JAVA_INT, offset + 40);
        byte[] objectIdBytes = new byte[OBJECTID_SIZE];
        MemorySegment.copy(mappedMemory, ValueLayout.JAVA_BYTE, offset + 44, objectIdBytes, 0, OBJECTID_SIZE);
        ObjectId objectId = new ObjectId(objectIdBytes);
        EntryMetadata em = new EntryMetadata(segmentId, prefix, position, length, handle);

        return new DocumentLocation(objectId, shardId, em);
    }

    public GraphNodeRef findNodeRef(ObjectId objectId) {
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
                int ordinal = mappedMemory.get(ValueLayout.JAVA_INT, entryOffset + OBJECTID_SIZE);
                byte[] versionstampBytes = new byte[VERSIONSTAMP_SIZE];
                MemorySegment.copy(mappedMemory, ValueLayout.JAVA_BYTE, entryOffset + OBJECTID_SIZE + 4,
                        versionstampBytes, 0, VERSIONSTAMP_SIZE);
                return new GraphNodeRef(ordinal, Versionstamp.fromBytes(versionstampBytes));
            }
        }
        return null;
    }

    @Override
    public void close() {
        arena.close();
    }
}
