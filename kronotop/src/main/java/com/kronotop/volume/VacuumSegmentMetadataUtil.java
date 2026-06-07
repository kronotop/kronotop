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

package com.kronotop.volume;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Provides static helpers for reading and writing vacuum metadata stored as individual FoundationDB key-value pairs.
 */
public class VacuumSegmentMetadataUtil {

    public static void save(Transaction tr, VolumeSubspace subspace, long segmentId, VacuumMetadataStatus status, long startedAt) {
        setStatus(tr, subspace, segmentId, status);

        byte[] startedAtKey = subspace.packVacuumSegmentMetadataFieldKey(segmentId, VacuumMetadataField.STARTED_AT.getValue());
        byte[] startedAtValue = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(startedAt).array();
        tr.set(startedAtKey, startedAtValue);
    }

    public static VacuumSegmentMetadata load(ReadTransaction tr, VolumeSubspace subspace, long segmentId) {
        byte[] statusKey = subspace.packVacuumSegmentMetadataFieldKey(segmentId, VacuumMetadataField.STATUS.getValue());
        byte[] statusBytes = tr.get(statusKey).join();
        if (statusBytes == null) {
            return null;
        }

        byte[] startedAtKey = subspace.packVacuumSegmentMetadataFieldKey(segmentId, VacuumMetadataField.STARTED_AT.getValue());
        byte[] startedAtBytes = tr.get(startedAtKey).join();
        long startedAt = ByteBuffer.wrap(startedAtBytes).order(ByteOrder.LITTLE_ENDIAN).getLong();

        return new VacuumSegmentMetadata(segmentId, VacuumMetadataStatus.fromValue(statusBytes[0]), startedAt);
    }

    public static VacuumMetadataStatus readStatus(ReadTransaction tr, VolumeSubspace subspace, long segmentId) {
        byte[] statusKey = subspace.packVacuumSegmentMetadataFieldKey(segmentId, VacuumMetadataField.STATUS.getValue());
        byte[] statusBytes = tr.get(statusKey).join();
        if (statusBytes == null) {
            throw new IllegalStateException("No vacuum metadata found for segment " + segmentId);
        }
        return VacuumMetadataStatus.fromValue(statusBytes[0]);
    }

    public static void setStatus(Transaction tr, VolumeSubspace subspace, long segmentId, VacuumMetadataStatus status) {
        byte[] statusKey = subspace.packVacuumSegmentMetadataFieldKey(segmentId, VacuumMetadataField.STATUS.getValue());
        tr.set(statusKey, new byte[]{status.getValue()});
    }

    public static void savePrefixCardinality(Transaction tr, VolumeSubspace subspace, long segmentId, Prefix prefix, int cardinality) {
        byte[] key = subspace.packVacuumSegmentMetadataPrefixCardinalityKey(segmentId, prefix);
        byte[] value = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(cardinality).array();
        tr.set(key, value);
    }

    public static Range getVacuumPrefixRange(VolumeSubspace subspace, long segmentId) {
        return subspace.packVacuumSegmentMetadataPrefixRange(segmentId);
    }

    public static void delete(Transaction tr, VolumeSubspace subspace, long segmentId) {
        Range range = subspace.packVacuumSegmentMetadataSegmentRange(segmentId);
        tr.clear(range);
    }

    public static List<VacuumSegmentMetadata> loadAll(ReadTransaction tr, VolumeSubspace subspace) {
        Range range = subspace.packVacuumSegmentMetadataSegmentsRange();
        Set<Long> segmentIds = new LinkedHashSet<>();
        for (KeyValue kv : tr.getRange(range)) {
            Tuple tuple = subspace.getDirectorySubspace().unpack(kv.getKey());
            long segmentId = tuple.getLong(2);
            segmentIds.add(segmentId);
        }
        List<VacuumSegmentMetadata> result = new ArrayList<>();
        for (long segmentId : segmentIds) {
            VacuumSegmentMetadata metadata = load(tr, subspace, segmentId);
            if (metadata != null) {
                result.add(metadata);
            }
        }
        return result;
    }
}
