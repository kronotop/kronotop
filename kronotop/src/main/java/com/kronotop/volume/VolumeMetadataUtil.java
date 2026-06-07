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

package com.kronotop.volume;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.hash.HashCode;
import com.kronotop.internal.UUIDUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Provides static helpers for reading and initializing volume metadata stored as individual FoundationDB key-value pairs.
 */
public class VolumeMetadataUtil {
    static final byte[] NULL_BYTES = new byte[]{};

    /**
     * Loads existing volume metadata or initializes a new volume with a generated ID, READWRITE status, and segment 0.
     *
     * @param tr       the transaction to use for reading and writing
     * @param subspace the volume subspace containing the metadata keys
     * @return the loaded or newly created volume metadata
     */
    public static VolumeMetadata createOrOpen(Transaction tr, VolumeSubspace subspace) {
        byte[] idKey = subspace.packVolumeIdKey();
        byte[] idBytes = tr.get(idKey).join();
        if (idBytes != null) {
            // Already initialized
            long volumeId = ByteBuffer.wrap(idBytes).getLong();

            VolumeStatus status = VolumeMetadataUtil.readVolumeStatus(tr, subspace);
            List<Long> segmentIds = VolumeMetadataUtil.loadSegmentIds(tr, subspace);
            return new VolumeMetadata(volumeId, status, segmentIds);
        }

        // Volume ID
        UUID uuid = UUID.randomUUID();
        HashCode hashCode = UUIDUtil.hash(uuid);
        long volumeId = hashCode.asLong();
        byte[] idValue = ByteBuffer.allocate(Long.BYTES).putLong(volumeId).array();
        tr.set(idKey, idValue);

        // Volume Status
        byte[] statusKey = subspace.packVolumeStatusKey();
        byte[] statusValue = VolumeStatus.READWRITE.toString().getBytes(StandardCharsets.US_ASCII);
        tr.set(statusKey, statusValue);

        // The first segment
        byte[] segmentIdKey = subspace.packVolumeSegmentIdKey(0L);
        tr.set(segmentIdKey, NULL_BYTES);

        return new VolumeMetadata(volumeId, VolumeStatus.READWRITE, List.of(0L));
    }

    /**
     * Reads the volume's unique identifier from FoundationDB.
     *
     * @param tr       the read transaction to use
     * @param subspace the volume subspace containing the ID key
     * @return the volume ID
     */
    public static long readVolumeId(ReadTransaction tr, VolumeSubspace subspace) {
        byte[] idKey = subspace.packVolumeIdKey();
        byte[] idBytes = tr.get(idKey).join();
        return ByteBuffer.wrap(idBytes).getLong();
    }

    /**
     * Loads all segment IDs registered for a volume, returned in ascending order.
     *
     * @param tr       the read transaction to use
     * @param subspace the volume subspace containing the segment keys
     * @return an unmodifiable sorted list of segment IDs
     */
    public static List<Long> loadSegmentIds(ReadTransaction tr, VolumeSubspace subspace) {
        List<Long> segmentIds = new ArrayList<>();
        Range range = subspace.packVolumeSegmentsRange();
        for (KeyValue kv : tr.getRange(range)) {
            Tuple tuple = subspace.getDirectorySubspace().unpack(kv.getKey());
            long segmentId = tuple.getLong(2);
            segmentIds.add(segmentId);
        }
        // Safety valve.
        segmentIds.sort(Comparator.naturalOrder());
        return Collections.unmodifiableList(segmentIds);
    }

    /**
     * Reads the current status of a volume from FoundationDB.
     *
     * @param tr       the read transaction to use
     * @param subspace the volume subspace containing the status key
     * @return the current volume status
     * @throws IllegalStateException if no status entry exists for the volume
     */
    public static VolumeStatus readVolumeStatus(ReadTransaction tr, VolumeSubspace subspace) {
        byte[] bytes = tr.get(subspace.packVolumeStatusKey()).join();
        if (bytes == null) {
            throw new IllegalStateException("No volume status found for " + subspace.getDirectorySubspace().getPath());
        }
        return VolumeStatus.valueOf(new String(bytes));
    }
}
