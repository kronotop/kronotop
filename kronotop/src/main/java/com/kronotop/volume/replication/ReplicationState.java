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

package com.kronotop.volume.replication;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

/**
 * Manages segment replication state in FoundationDB for volume replication.
 * Tracks replication progress per segment including position, status, stage, sequence numbers,
 * tail pointers, and error messages.
 */
public class ReplicationState {
    private static final byte REPLICATION_STATE = 0x00;
    private static final byte POSITION = 0x01;
    private static final byte ERROR_MESSAGE = 0x02;
    private static final byte STATUS = 0x03;
    private static final byte TAIL_POINTER = 0x04;
    private static final byte STAGE = 0x05;
    private static final byte SEQUENCE_NUMBER = 0x06;

    /**
     * Records the replication position for a segment.
     *
     * @param tr        the FoundationDB transaction
     * @param subspace  the directory subspace for replication state
     * @param segmentId the segment identifier
     * @param position  the current replication position within the segment
     */
    public static void setPosition(Transaction tr, DirectorySubspace subspace, long segmentId, long position) {
        Tuple tuple = Tuple.from(REPLICATION_STATE, POSITION, segmentId);
        byte[] key = subspace.pack(tuple);
        byte[] value = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(position).array();
        tr.set(key, value);
    }

    /**
     * Retrieves the latest replication cursor from the highest segment.
     * Returns a cursor with the most recent segment ID and its replication position.
     *
     * @param tr       the FoundationDB transaction
     * @param subspace the directory subspace for replication state
     * @return the replication cursor with segment ID and position, or (0,0) if no segments exist
     */
    public static ReplicationCursor readCursor(Transaction tr, DirectorySubspace subspace) {
        Tuple tuple = Tuple.from(REPLICATION_STATE, POSITION);
        byte[] prefix = subspace.pack(tuple);
        Range range = Range.startsWith(prefix);
        for (KeyValue keyValue : tr.getRange(range, 1, true)) {
            Tuple key = subspace.unpack(keyValue.getKey());
            long segmentId = key.getLong(2);
            long position = ByteBuffer.wrap(keyValue.getValue()).order(ByteOrder.LITTLE_ENDIAN).getLong();
            return new ReplicationCursor(segmentId, position);
        }
        return new ReplicationCursor(0, 0);
    }

    public static long readPosition(Transaction tr, DirectorySubspace subspace, long segmentId) {
        Tuple tuple = Tuple.from(REPLICATION_STATE, POSITION, segmentId);
        byte[] key = subspace.pack(tuple);
        byte[] value = tr.get(key).join();
        if (value == null) {
            return 0;
        }
        return ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    /**
     * Stores an error message for a specific segment.
     *
     * @param tr        the FoundationDB transaction
     * @param subspace  the directory subspace for replication state
     * @param segmentId the segment identifier
     * @param message   the error message string
     */
    public static void setErrorMessage(Transaction tr, DirectorySubspace subspace, long segmentId, String message) {
        Tuple tuple = Tuple.from(REPLICATION_STATE, ERROR_MESSAGE, segmentId);
        byte[] key = subspace.pack(tuple);
        tr.set(key, message.getBytes());
    }

    /**
     * Clears the error message for a specific segment.
     *
     * @param tr        the FoundationDB transaction
     * @param subspace  the directory subspace for replication state
     * @param segmentId the segment identifier
     */
    public static void clearErrorMessage(Transaction tr, DirectorySubspace subspace, long segmentId) {
        Tuple tuple = Tuple.from(REPLICATION_STATE, ERROR_MESSAGE, segmentId);
        byte[] key = subspace.pack(tuple);
        tr.clear(key);
    }

    /**
     * Retrieves the error message for a specific segment.
     *
     * @param tr        the FoundationDB transaction
     * @param subspace  the directory subspace for replication state
     * @param segmentId the segment identifier
     * @return the error message string, or null if no error exists
     */
    public static String readErrorMessage(Transaction tr, DirectorySubspace subspace, long segmentId) {
        Tuple tuple = Tuple.from(REPLICATION_STATE, ERROR_MESSAGE, segmentId);
        byte[] key = subspace.pack(tuple);
        byte[] value = tr.get(key).join();
        if (value == null) {
            return null;
        }
        return new String(value);
    }

    /**
     * Sets the replication status for a specific segment.
     *
     * @param tr        the FoundationDB transaction
     * @param subspace  the directory subspace for replication state
     * @param segmentId the segment identifier
     * @param status    the replication status to set
     */
    public static void setStatus(Transaction tr, DirectorySubspace subspace, long segmentId, ReplicationStatus status) {
        Tuple tuple = Tuple.from(REPLICATION_STATE, STATUS, segmentId);
        byte[] key = subspace.pack(tuple);
        tr.set(key, status.name().getBytes());
    }

    /**
     * Retrieves the replication status for a specific segment.
     *
     * @param tr        the FoundationDB transaction
     * @param subspace  the directory subspace for replication state
     * @param segmentId the segment identifier
     * @return the replication status, or WAITING if no status exists
     */
    public static ReplicationStatus readStatus(Transaction tr, DirectorySubspace subspace, long segmentId) {
        Tuple tuple = Tuple.from(REPLICATION_STATE, STATUS, segmentId);
        byte[] key = subspace.pack(tuple);
        byte[] value = tr.get(key).join();
        if (value == null) {
            return ReplicationStatus.WAITING;
        }
        return ReplicationStatus.valueOf(new String(value));
    }

    /**
     * Sets the tail pointer for a specific segment, storing the sequence number and next position.
     *
     * @param tr             the FoundationDB transaction
     * @param subspace       the directory subspace for replication state
     * @param segmentId      the segment identifier
     * @param sequenceNumber the changelog sequence number at the tail
     * @param nextPosition   the next available byte position in the segment
     */
    public static void setTailPointer(Transaction tr, DirectorySubspace subspace, long segmentId, long sequenceNumber, long nextPosition) {
        Tuple tuple = Tuple.from(REPLICATION_STATE, TAIL_POINTER, segmentId);
        byte[] key = subspace.pack(tuple);
        byte[] value = Tuple.from(sequenceNumber, nextPosition).pack();
        tr.set(key, value);
    }

    /**
     * Retrieves the tail pointer for a specific segment.
     *
     * @param tr        the FoundationDB transaction
     * @param subspace  the directory subspace for replication state
     * @param segmentId the segment identifier
     * @return a list containing [sequenceNumber, nextPosition], or an empty list if no tail pointer exists
     */
    public static List<Long> readTailPointer(Transaction tr, DirectorySubspace subspace, long segmentId) {
        Tuple tuple = Tuple.from(REPLICATION_STATE, TAIL_POINTER, segmentId);
        byte[] key = subspace.pack(tuple);
        byte[] value = tr.get(key).join();
        if (value == null) {
            return List.of();
        }
        Tuple unpacked = Tuple.fromBytes(value);
        long sequenceNumber = unpacked.getLong(0);
        long nextPosition = unpacked.getLong(1);
        return List.of(sequenceNumber, nextPosition);
    }

    /**
     * Sets the replication stage for a specific segment.
     *
     * @param tr        the FoundationDB transaction
     * @param subspace  the directory subspace for replication state
     * @param segmentId the segment identifier
     * @param stage     the replication stage to set
     */
    public static void setStage(Transaction tr, DirectorySubspace subspace, long segmentId, Stage stage) {
        Tuple tuple = Tuple.from(REPLICATION_STATE, STAGE, segmentId);
        byte[] key = subspace.pack(tuple);
        tr.set(key, stage.name().getBytes());
    }

    /**
     * Retrieves the replication stage for a specific segment.
     *
     * @param tr        the FoundationDB transaction
     * @param subspace  the directory subspace for replication state
     * @param segmentId the segment identifier
     * @return the stage, or null if no stage exists
     */
    public static Stage readStage(Transaction tr, DirectorySubspace subspace, long segmentId) {
        Tuple tuple = Tuple.from(REPLICATION_STATE, STAGE, segmentId);
        byte[] key = subspace.pack(tuple);
        byte[] value = tr.get(key).join();
        if (value == null) {
            return null;
        }
        return Stage.valueOf(new String(value));
    }

    /**
     * Sets the changelog sequence number for a specific segment.
     *
     * @param tr             the FoundationDB transaction
     * @param subspace       the directory subspace for replication state
     * @param segmentId      the segment identifier
     * @param sequenceNumber the changelog sequence number
     */
    public static void setSequenceNumber(Transaction tr, DirectorySubspace subspace, long segmentId, long sequenceNumber) {
        Tuple tuple = Tuple.from(REPLICATION_STATE, SEQUENCE_NUMBER, segmentId);
        byte[] key = subspace.pack(tuple);
        byte[] value = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(sequenceNumber).array();
        tr.set(key, value);
    }

    /**
     * Retrieves the changelog sequence number for a specific segment.
     *
     * @param tr        the FoundationDB transaction
     * @param subspace  the directory subspace for replication state
     * @param segmentId the segment identifier
     * @return the sequence number, or 0 if no sequence number exists
     */
    public static long readSequenceNumber(Transaction tr, DirectorySubspace subspace, long segmentId) {
        Tuple tuple = Tuple.from(REPLICATION_STATE, SEQUENCE_NUMBER, segmentId);
        byte[] key = subspace.pack(tuple);
        byte[] value = tr.get(key).join();
        if (value == null) {
            return 0;
        }
        return ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }
}
