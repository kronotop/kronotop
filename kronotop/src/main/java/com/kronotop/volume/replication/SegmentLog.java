/*
 * Copyright (c) 2023-2025 Kronotop
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

package com.kronotop.volume.replication;

// SegmentLog
// <segment-name><versionstamped-key><epoch> = <operation><position><length>

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;

import static com.kronotop.volume.Subspaces.SEGMENT_LOG_CARDINALITY_SUBSPACE;
import static com.kronotop.volume.Subspaces.SEGMENT_LOG_SUBSPACE;

/**
 * The SegmentLog class represents a log specific to a segment, allowing for the
 * recording of log entries and the tracking of their cardinality within a specified
 * subspace using FoundationDB transactions.
 */
public class SegmentLog {
    protected static final byte[] NULL_BYTES = new byte[]{};
    private static final byte[] CARDINALITY_INCREASE_DELTA = new byte[]{1, 0, 0, 0}; // 1, byte order: little-endian
    private final String segmentName;
    private final DirectorySubspace subspace;
    private final byte[] cardinalityKey;

    /**
     * Constructs a SegmentLog instance with the given segment name and subspace.
     *
     * @param segmentName the name of the segment to be logged
     * @param subspace    the subspace where this segment log is stored
     */
    public SegmentLog(String segmentName, DirectorySubspace subspace) {
        this.segmentName = segmentName;
        this.subspace = subspace;

        Tuple key = Tuple.from(SEGMENT_LOG_CARDINALITY_SUBSPACE, segmentName);
        this.cardinalityKey = subspace.pack(key);
    }

    /**
     * Appends a new log entry to the segment log.
     *
     * @param tr          The transaction to use for this operation.
     * @param userVersion The user version to associate with this log entry.
     * @param value       The log entry value to be encoded and appended.
     */
    public void append(Transaction tr, int userVersion, SegmentLogValue value) {
        append_internal(tr, null, userVersion, value);
    }

    /**
     * Appends a new log entry to the segment log with the given versionstamp.
     *
     * @param tr           The transaction to use for this operation.
     * @param versionstamp The versionstamp to associate with this log entry.
     * @param userVersion  The user version to associate with this log entry.
     * @param value        The log entry value to be encoded and appended.
     */
    public void append(Transaction tr, Versionstamp versionstamp, int userVersion, SegmentLogValue value) {
        append_internal(tr, versionstamp, userVersion, value);
    }

    /**
     * Appends a log entry to the segment log with the provided transaction, versionstamp,
     * user version, and log value. This method internally updates the log entry,
     * manages the cardinality, and updates the secondary index.
     *
     * @param tr           The transaction used for this operation.
     * @param versionstamp The versionstamp associated with the log entry.
     * @param userVersion  The user-defined version to associate with the log entry.
     * @param value        The log entry value that will be encoded and appended to the segment log.
     */
    private void append_internal(Transaction tr, Versionstamp versionstamp, int userVersion, SegmentLogValue value) {
        byte[] key;
        if (versionstamp == null) {
            Tuple preKey = Tuple.from(
                    SEGMENT_LOG_SUBSPACE,
                    segmentName,
                    Versionstamp.incomplete(userVersion),
                    Instant.now().toEpochMilli()
            );
            key = subspace.packWithVersionstamp(preKey);
        } else {
            Tuple preKey = Tuple.from(
                    SEGMENT_LOG_SUBSPACE,
                    segmentName,
                    Versionstamp.incomplete(userVersion),
                    versionstamp,
                    Instant.now().toEpochMilli()
            );
            key = subspace.packWithVersionstamp(preKey);
        }
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, key, value.encode().array());
        tr.mutate(MutationType.ADD, cardinalityKey, CARDINALITY_INCREASE_DELTA);
    }

    /**
     * Retrieves the cardinality value associated with the segment log from the provided transaction.
     *
     * @param tr The transaction to use for fetching the cardinality value.
     * @return The cardinality value as an integer.
     */
    public int getCardinality(Transaction tr) {
        byte[] data = tr.get(cardinalityKey).join();
        return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }
}
