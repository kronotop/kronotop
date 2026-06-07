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
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Writes {@link OnHeapVectorGraphIndexMetadata} to an immutable on-disk file (*.vmeta).
 */
public class OnDiskVectorGraphIndexMetadataWriter {
    static final int MAGIC = 0x4B564D44;
    static final int VERSION = 1;
    static final int HEADER_SIZE = 40;
    static final int ORDINAL_SLOT_SIZE = 64;
    static final int OBJECTID_ENTRY_SIZE = 16;
    private static final Logger logger = LoggerFactory.getLogger(OnDiskVectorGraphIndexMetadataWriter.class);

    private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer);
            if (written > 0 || logger.isDebugEnabled()) {
                logger.debug("Wrote {} bytes to FileChannel", written);
            }
        }
    }

    /**
     * Writes the metadata to the disk in a single immutable file.
     */
    public static void write(Path path, OnHeapVectorGraphIndexMetadata metadata) throws IOException {
        Map<ObjectId, Integer> objectIds = metadata.getObjectIds();
        Map<Integer, DocumentLocation> ordinals = metadata.getOrdinals();

        int count = objectIds.size();
        int maxOrdinal = -1;
        for (int ord : ordinals.keySet()) {
            if (ord > maxOrdinal) {
                maxOrdinal = ord;
            }
        }

        int ordinalSlots = maxOrdinal + 1;
        int objectIdSectionOffset = HEADER_SIZE + ordinalSlots * ORDINAL_SLOT_SIZE;

        Path tmpPath = path.resolveSibling(path.getFileName() + ".tmp");
        try (FileChannel channel = FileChannel.open(tmpPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            // Write header
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.nativeOrder());
            header.putInt(MAGIC);
            header.putInt(VERSION);
            header.putInt(count);
            header.putInt(maxOrdinal);
            header.putInt(objectIdSectionOffset);
            Versionstamp latestVersionstamp = metadata.getLatestVersionstamp();
            if (latestVersionstamp != null) {
                header.put(latestVersionstamp.getBytes());
            } else {
                header.put(new byte[12]);
            }
            header.putInt(0); // deletedCount
            header.putInt(0); // reserved
            header.flip();
            writeFully(channel, header);

            // Write ordinal -> DocumentLocation section
            ByteBuffer ordinalSection = ByteBuffer.allocate(ordinalSlots * ORDINAL_SLOT_SIZE).order(ByteOrder.nativeOrder());
            for (int i = 0; i < ordinalSlots; i++) {
                DocumentLocation entry = ordinals.get(i);
                if (entry != null) {
                    ordinalSection.putLong(entry.entryMetadata().segmentId());
                    ordinalSection.put(entry.entryMetadata().prefix());
                    ordinalSection.putLong(entry.entryMetadata().position());
                    ordinalSection.putLong(entry.entryMetadata().length());
                    ordinalSection.putLong(entry.entryMetadata().handle());
                    ordinalSection.putInt(entry.shardId());
                    ordinalSection.put((byte) 1); // alive
                    ordinalSection.put(entry.objectId().toByteArray());
                    ordinalSection.position(ordinalSection.position() + 7); // padding
                } else {
                    ordinalSection.position(ordinalSection.position() + ORDINAL_SLOT_SIZE);
                }
            }
            ordinalSection.flip();
            writeFully(channel, ordinalSection);

            // Write ObjectId -> ordinal section (sorted by ObjectId bytes)
            List<Map.Entry<ObjectId, Integer>> sortedEntries = new ArrayList<>(objectIds.entrySet());
            sortedEntries.sort((a, b) -> Arrays.compareUnsigned(a.getKey().toByteArray(), b.getKey().toByteArray()));

            ByteBuffer objectIdSection = ByteBuffer.allocate(count * OBJECTID_ENTRY_SIZE).order(ByteOrder.nativeOrder());
            for (Map.Entry<ObjectId, Integer> entry : sortedEntries) {
                objectIdSection.put(entry.getKey().toByteArray());
                objectIdSection.putInt(entry.getValue());
            }
            objectIdSection.flip();
            writeFully(channel, objectIdSection);

            channel.force(true);
        }
        Files.move(tmpPath, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }
}
