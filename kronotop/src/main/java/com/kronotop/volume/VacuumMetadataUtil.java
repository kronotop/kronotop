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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class VacuumMetadataUtil {

    public static void deleteAll(Transaction tr, VolumeSubspace subspace) {
        Range range = subspace.packVacuumMetadataRange();
        tr.clear(range);
    }

    public static boolean exists(ReadTransaction tr, VolumeSubspace subspace) {
        Range range = subspace.packVacuumMetadataRange();
        return !tr.getRange(range, 1).asList().join().isEmpty();
    }

    public static void save(Transaction tr, VolumeSubspace subspace, long startedAt, long completedAt, VacuumResult result, int segmentsProcessed) {
        byte[] startedAtKey = subspace.packVacuumStatsFieldKey(VacuumStatsField.STARTED_AT.getValue());
        tr.set(startedAtKey, ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(startedAt).array());

        byte[] completedAtKey = subspace.packVacuumStatsFieldKey(VacuumStatsField.COMPLETED_AT.getValue());
        tr.set(completedAtKey, ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(completedAt).array());

        byte[] resultKey = subspace.packVacuumStatsFieldKey(VacuumStatsField.RESULT.getValue());
        tr.set(resultKey, new byte[]{result.getValue()});

        byte[] segmentsKey = subspace.packVacuumStatsFieldKey(VacuumStatsField.SEGMENTS_PROCESSED.getValue());
        tr.set(segmentsKey, ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(segmentsProcessed).array());
    }

    public static VacuumMetadata load(ReadTransaction tr, VolumeSubspace subspace) {
        byte[] resultKey = subspace.packVacuumStatsFieldKey(VacuumStatsField.RESULT.getValue());
        byte[] resultBytes = tr.get(resultKey).join();
        if (resultBytes == null) {
            return null;
        }

        byte[] startedAtKey = subspace.packVacuumStatsFieldKey(VacuumStatsField.STARTED_AT.getValue());
        long startedAt = ByteBuffer.wrap(tr.get(startedAtKey).join()).order(ByteOrder.LITTLE_ENDIAN).getLong();

        byte[] completedAtKey = subspace.packVacuumStatsFieldKey(VacuumStatsField.COMPLETED_AT.getValue());
        long completedAt = ByteBuffer.wrap(tr.get(completedAtKey).join()).order(ByteOrder.LITTLE_ENDIAN).getLong();

        byte[] segmentsKey = subspace.packVacuumStatsFieldKey(VacuumStatsField.SEGMENTS_PROCESSED.getValue());
        int segmentsProcessed = ByteBuffer.wrap(tr.get(segmentsKey).join()).order(ByteOrder.LITTLE_ENDIAN).getInt();

        return new VacuumMetadata(startedAt, completedAt, VacuumResult.fromValue(resultBytes[0]), segmentsProcessed);
    }
}
