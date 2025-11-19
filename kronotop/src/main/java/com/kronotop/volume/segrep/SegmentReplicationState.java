/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.volume.segrep;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Manages segment replication state in FoundationDB for volume replication.
 * Tracks replication progress via cursor positions and error messages per segment.
 */
public class SegmentReplicationState {
    private static final byte SEGMENTS = 0x00;
    private static final byte ERROR_MESSAGE = 0x01;

    /**
     * Records the replication position for a segment.
     *
     * @param tr the FoundationDB transaction
     * @param subspace the directory subspace for replication state
     * @param segmentId the segment identifier
     * @param position the current replication position within the segment
     */
    public static void setPosition(Transaction tr, DirectorySubspace subspace, long segmentId, long position) {
        Tuple tuple = Tuple.from(SEGMENTS, segmentId);
        byte[] key = subspace.pack(tuple);
        byte[] value = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(position).array();
        tr.set(key, value);
    }

    /**
     * Retrieves the latest replication cursor from the highest segment.
     * Returns a cursor with the most recent segment ID and its replication position.
     *
     * @param tr the FoundationDB transaction
     * @param subspace the directory subspace for replication state
     * @return the replication cursor with segment ID and position, or (0,0) if no segments exist
     */
    public static ReplicationCursor readCursor(Transaction tr, DirectorySubspace subspace) {
        Tuple tuple = Tuple.from(SEGMENTS);
        byte[] prefix = subspace.pack(tuple);
        Range range = Range.startsWith(prefix);
        for (KeyValue keyValue : tr.getRange(range, 1, true)) {
            Tuple key = subspace.unpack(keyValue.getKey());
            long segmentId = key.getLong(1);
            long position = ByteBuffer.wrap(keyValue.getValue()).order(ByteOrder.LITTLE_ENDIAN).getLong();
            return new ReplicationCursor(segmentId, position);
        }
        return new ReplicationCursor(0, 0);
    }

    /**
     * Stores an error message for a specific segment.
     *
     * @param tr the FoundationDB transaction
     * @param subspace the directory subspace for replication state
     * @param segmentId the segment identifier
     * @param message the error message string
     */
    public static void setErrorMessage(Transaction tr, DirectorySubspace subspace, long segmentId, String message) {
        Tuple tuple = Tuple.from(ERROR_MESSAGE, segmentId);
        byte[] key = subspace.pack(tuple);
        tr.set(key, message.getBytes());
    }

    /**
     * Retrieves the error message for a specific segment.
     *
     * @param tr the FoundationDB transaction
     * @param subspace the directory subspace for replication state
     * @param segmentId the segment identifier
     * @return the error message string, or null if no error exists
     */
    public static String readErrorMessage(Transaction tr, DirectorySubspace subspace, long segmentId) {
        Tuple tuple = Tuple.from(ERROR_MESSAGE, segmentId);
        byte[] key = subspace.pack(tuple);
        byte[] value = tr.get(key).join();
        if (value == null) {
            return null;
        }
        return new String(value);
    }
}
