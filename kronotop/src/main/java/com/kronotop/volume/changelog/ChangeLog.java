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

package com.kronotop.volume.changelog;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.volume.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.kronotop.volume.Subspaces.CHANGELOG_BACK_POINTER_SUBSPACE;
import static com.kronotop.volume.Subspaces.CHANGELOG_SUBSPACE;

/**
 * Tracks volume operations in FoundationDB with temporal ordering using Hybrid Logical Clock.
 * Supports append, update, and delete operation tracking for replication.
 */
public class ChangeLog {
    private static final byte[] NULL_BYTES = new byte[]{};
    private final Context context;
    private final DirectorySubspace subspace;
    private final HybridLogicalClock hlc = new HybridLogicalClock();

    /**
     * Creates a new ChangeLog instance.
     *
     * @param context  the context providing FoundationDB access and system time
     * @param subspace the directory subspace for storing changelog entries
     */
    public ChangeLog(Context context, DirectorySubspace subspace) {
        this.context = context;
        this.subspace = subspace;
    }

    /**
     * Returns the latest sequence number from the changelog, or 0 if the changelog is empty.
     *
     * @param tr       the transaction to use for the read operation
     * @param subspace the directory subspace containing the changelog
     * @return the latest sequence number, or 0 if no entries exist
     */
    public static long getLatestSequenceNumber(Transaction tr, DirectorySubspace subspace) {
        byte[] prefix = subspace.pack(Tuple.from(CHANGELOG_SUBSPACE));
        Range range = Range.startsWith(prefix);
        for (KeyValue keyValue : tr.getRange(range, 1, true)) {
            Tuple unpacked = subspace.unpack(keyValue.getKey());
            return unpacked.getLong(1);
        }
        return 0L;
    }

    /**
     * Resolves the ChangeLog sequence number corresponding to a segment's tail pointer for replication or backup.
     *
     * <p>Maps a {@link SegmentTailPointer} (position, length, versionstamp) back to its originating sequence number
     * using the back pointer index for efficient lookup.
     *
     * <p><strong>Implementation:</strong>
     * <ul>
     *   <li><strong>Back Pointer Lookup:</strong> Uses {@link #reverseLookup} to find candidate sequence numbers
     *       for the given segment position, then validates each against the changelog entry.</li>
     *
     *   <li><strong>Exact Match:</strong> The condition {@code pos + len == nextPosition} ensures the entry
     *       aligns precisely with the expected byte boundary.</li>
     *
     *   <li><strong>Versionstamp Validation:</strong> Validates that the changelog entry's versionstamp matches
     *       the pointer's versionstamp, ensuring metadata consistency.</li>
     *
     *   <li><strong>Retention Fallback:</strong> Returns -1 if no matching entry is found, indicating
     *       the entry was removed due to retention policy.</li>
     * </ul>
     *
     * @param tr        the transaction to use for the read operation
     * @param subspace  the directory subspace containing the changelog
     * @param segmentId the segment identifier to match
     * @param pointer   the tail pointer containing position, length, and versionstamp
     * @return the sequence number aligned with the pointer, or -1 if no matching entry is found
     * @throws InconsistentVolumeMetadataException if position matches but versionstamp does not
     */
    public static long resolveTailSequenceNumber(Transaction tr,
                                                 DirectorySubspace subspace,
                                                 long segmentId,
                                                 SegmentTailPointer pointer
    ) {
        // We trust the math here. The data structures must be aligned on the byte level.
        // See "Active Segment Write Invariant"
        List<Long> sequenceNumbers = ChangeLog.reverseLookup(tr, subspace, segmentId, pointer.position());
        for (long sequenceNumber : sequenceNumbers) {
            ChangeLogIterableOptions options = new ChangeLogIterableOptions.
                    Builder().
                    begin(SequenceNumberSelector.firstGreaterOrEqual(sequenceNumber)).
                    end(SequenceNumberSelector.firstGreaterThan(sequenceNumber)).
                    build();
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace, options);
            for (ChangeLogEntry entry : iterable) {
                ChangeLogCoordinate coordinate = entry.getAfter().orElseThrow();
                if (coordinate.segmentId() == segmentId) {
                    if (coordinate.position() + coordinate.length() == pointer.nextPosition()) {
                        if (!entry.getVersionstamp().equals(pointer.versionstamp())) {
                            throw new InconsistentVolumeMetadataException(
                                    "Tail pointer versionstamp does not match the last LIFECYCLE entry."
                            );
                        }
                        return coordinate.sequenceNumber();
                    } else {
                        throw new InconsistentVolumeMetadataException(
                                "Tail pointer position does not align with the last LIFECYCLE entry."
                        );
                    }
                }
            }
        }
        // Retention fallback: no matching found, it should be removed due to the retention period.
        return -1;
    }

    /**
     * Looks up all changelog sequence numbers associated with a segment position via the back pointer index.
     *
     * @param tr        the transaction to use for the read operation
     * @param subspace  the directory subspace containing the changelog
     * @param segmentId the segment identifier
     * @param position  the byte position within the segment
     * @return list of sequence numbers in ascending order, or empty list if no entries exist
     */
    public static List<Long> reverseLookup(Transaction tr, DirectorySubspace subspace, long segmentId, long position) {
        Tuple prefix = Tuple.from(
                CHANGELOG_BACK_POINTER_SUBSPACE,
                segmentId,
                position
        );
        List<Long> result = new ArrayList<>();
        Range range = Range.startsWith(subspace.pack(prefix));
        for (KeyValue keyValue : tr.getRange(range)) {
            Tuple unpackedBackPointer = subspace.unpack(keyValue.getKey());
            long sequenceNumber = unpackedBackPointer.getLong(3);
            result.add(sequenceNumber);
        }
        return result;
    }

    /**
     * Calculates the HLC cutoff timestamp for changelog pruning based on the retention period.
     *
     * @param context         the context providing the current time
     * @param retentionPeriod the retention period in hours
     * @return HLC timestamp representing the cutoff point
     * @throws IllegalArgumentException if retention period is zero or negative
     */
    public static long calculateCutoffEnd(Context context, long retentionPeriod) {
        if (retentionPeriod <= 0) {
            throw new IllegalArgumentException("retention period must be greater than zero");
        }
        long cutoffTimeMs = context.now() - TimeUnit.HOURS.toMillis(retentionPeriod);
        return HybridLogicalClock.fromPhysicalTime(cutoffTimeMs);
    }

    /**
     * Packs entry metadata into a tuple value.
     *
     * @param metadata the entry metadata to pack
     * @param prefix   the prefix associated with the entry
     * @return packed tuple bytes
     */
    private byte[] packValue(EntryMetadata metadata, Prefix prefix) {
        Tuple valueTuple = Tuple.from(metadata.segmentId(), metadata.position(), metadata.length(), prefix.asLong());
        return valueTuple.pack();
    }

    /**
     * Packs previous and current entry metadata into a tuple value for update operations.
     */
    private byte[] packValueForUpdate(EntryMetadata metadata, EntryMetadata prevMetadata, Prefix prefix) {
        Tuple valueTuple = Tuple.from(
                metadata.segmentId(),
                metadata.position(),
                metadata.length(),
                prevMetadata.segmentId(),
                prevMetadata.position(),
                prevMetadata.length(),
                prefix.asLong()
        );
        return valueTuple.pack();
    }

    /**
     * Emits a changelog entry using a versionstamped key mutation.
     *
     * @param tr       the transaction to use
     * @param metadata the entry metadata
     * @param prefix   the prefix associated with the entry
     * @param keyTuple the key tuple containing an incomplete versionstamp
     */
    private void emitChangeWithMutation(Transaction tr, EntryMetadata metadata, Prefix prefix, Tuple keyTuple) {
        byte[] key = subspace.packWithVersionstamp(keyTuple);
        byte[] value = packValue(metadata, prefix);
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, key, value);
    }

    /**
     * Emits a changelog entry using a regular set operation.
     *
     * @param tr       the transaction to use
     * @param metadata the entry metadata
     * @param prefix   the prefix associated with the entry
     * @param keyTuple the key tuple with a complete versionstamp
     */
    private void emitChange(Transaction tr, EntryMetadata metadata, Prefix prefix, Tuple keyTuple) {
        byte[] key = subspace.pack(keyTuple);
        byte[] value = packValue(metadata, prefix);
        tr.set(key, value);
    }

    /**
     * Constructs a key tuple for changelog entry.
     *
     * @param child   the operation kind
     * @param entryId the versionstamp identifying the entry
     * @return key tuple containing all components
     */
    private Tuple getPrimaryKeyTuple(long sequenceNumber, ParentOperationKind parent, OperationKind child, Versionstamp entryId) {
        return Tuple.from(
                CHANGELOG_SUBSPACE,
                sequenceNumber,
                parent.getValue(),
                child.getValue(),
                entryId
        );
    }

    /**
     * Constructs a key tuple for the back pointer index that maps segment positions to changelog entries.
     */
    private Tuple getBackPointerKeyTuple(long segmentId, long position, long sequenceNumber) {
        return Tuple.from(
                CHANGELOG_BACK_POINTER_SUBSPACE,
                segmentId,
                position,
                sequenceNumber
        );
    }

    /**
     * Creates a back pointer entry that enables reverse lookup from segment position to changelog sequence number.
     */
    private void setBackPointer(Transaction tr, long segmentId, long position, long sequenceNumber) {
        Tuple backPointerKeyTuple = getBackPointerKeyTuple(segmentId, position, sequenceNumber);
        byte[] key = subspace.pack(backPointerKeyTuple);
        tr.set(key, NULL_BYTES);
    }

    /**
     * Records an append operation for a new entry with an incomplete versionstamp.
     *
     * @param tr          the transaction to use
     * @param metadata    the entry metadata
     * @param prefix      the prefix associated with the entry
     * @param userVersion the user version for the versionstamp
     */
    public void appendOperation(Transaction tr, EntryMetadata metadata, Prefix prefix, int userVersion) {
        long sequenceNumber = hlc.next(context.now());
        Tuple primaryKeyTuple = getPrimaryKeyTuple(sequenceNumber, ParentOperationKind.LIFECYCLE, OperationKind.APPEND, Versionstamp.incomplete(userVersion));
        emitChangeWithMutation(tr, metadata, prefix, primaryKeyTuple);
        setBackPointer(tr, metadata.segmentId(), metadata.position(), sequenceNumber);
    }

    /**
     * Records an append operation for an existing entry with a complete versionstamp.
     *
     * @param tr           the transaction to use
     * @param metadata     the entry metadata
     * @param prefix       the prefix associated with the entry
     * @param versionstamp the complete versionstamp
     */
    public void appendOperation(Transaction tr, EntryMetadata metadata, Prefix prefix, Versionstamp versionstamp) {
        long sequenceNumber = hlc.next(context.now());
        Tuple primaryKeyTuple = getPrimaryKeyTuple(sequenceNumber, ParentOperationKind.LIFECYCLE, OperationKind.APPEND, versionstamp);
        emitChange(tr, metadata, prefix, primaryKeyTuple);
        setBackPointer(tr, metadata.segmentId(), metadata.position(), sequenceNumber);
    }

    /**
     * Records a delete operation for an entry.
     *
     * @param tr           the transaction to use
     * @param metadata     the entry metadata
     * @param prefix       the prefix associated with the entry
     * @param versionstamp the complete versionstamp
     */
    public void deleteOperation(Transaction tr, EntryMetadata metadata, Prefix prefix, Versionstamp versionstamp) {
        long sequenceNumber = hlc.next(context.now());
        Tuple primaryKeyTuple = getPrimaryKeyTuple(sequenceNumber, ParentOperationKind.FINALIZATION, OperationKind.DELETE, versionstamp);
        emitChange(tr, metadata, prefix, primaryKeyTuple);
        setBackPointer(tr, metadata.segmentId(), metadata.position(), sequenceNumber);
    }

    /**
     * Records an update operation for an entry, storing both previous and current metadata.
     *
     * @param tr           the transaction to use
     * @param prevMetadata the previous entry metadata before the update
     * @param metadata     the new entry metadata after the update
     * @param prefix       the prefix associated with the entry
     * @param versionstamp the complete versionstamp identifying the entry
     */
    public void updateOperation(Transaction tr,
                                EntryMetadata prevMetadata,
                                EntryMetadata metadata,
                                Prefix prefix,
                                Versionstamp versionstamp) {
        long sequenceNumber = hlc.next(context.now());
        Tuple primaryKeyTuple = getPrimaryKeyTuple(sequenceNumber, ParentOperationKind.LIFECYCLE, OperationKind.UPDATE, versionstamp);
        byte[] key = subspace.pack(primaryKeyTuple);
        byte[] value = packValueForUpdate(metadata, prevMetadata, prefix);
        tr.set(key, value);
        setBackPointer(tr, metadata.segmentId(), metadata.position(), sequenceNumber);
        setBackPointer(tr, prevMetadata.segmentId(), prevMetadata.position(), sequenceNumber);
    }

    /**
     * Removes changelog entries and their back pointers within the specified sequence number range.
     *
     * @param tr                  the transaction to use
     * @param cutoffStart         the starting sequence number (inclusive)
     * @param cutoffEnd           the ending sequence number (inclusive)
     * @param segmentMaxPositions map of segment ID to max position for back pointer cleanup
     */
    public void prune(Transaction tr, long cutoffStart, long cutoffEnd, Map<Long, Long> segmentMaxPositions) {
        // Forward changelog cleanup
        tr.clear(
                subspace.pack(Tuple.from(CHANGELOG_SUBSPACE, cutoffStart)),
                ByteArrayUtil.strinc(subspace.pack(Tuple.from(CHANGELOG_SUBSPACE, cutoffEnd)))
        );

        // Back-pointer cleanup per segment
        for (Map.Entry<Long, Long> seg : segmentMaxPositions.entrySet()) {
            long segmentId = seg.getKey();
            long maxPosition = seg.getValue();

            tr.clear(
                    subspace.pack(Tuple.from(CHANGELOG_BACK_POINTER_SUBSPACE, segmentId, 0, cutoffStart)),
                    subspace.pack(Tuple.from(CHANGELOG_BACK_POINTER_SUBSPACE, segmentId, maxPosition, cutoffEnd))
            );
        }
    }
}
