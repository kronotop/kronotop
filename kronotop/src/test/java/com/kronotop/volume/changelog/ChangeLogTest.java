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

package com.kronotop.volume.changelog;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.TestUtil;
import com.kronotop.volume.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static com.kronotop.volume.Subspaces.CHANGELOG_SUBSPACE;
import static org.junit.jupiter.api.Assertions.*;

class ChangeLogTest extends BaseStandaloneInstanceTest {

    private void awaitWatermark(ChangeLog changeLog, long expected) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (changeLog.getSafeWatermark() != expected && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
        assertEquals(expected, changeLog.getSafeWatermark());
    }

    private void awaitWatermarkGreaterThan(ChangeLog changeLog, long threshold) {
        awaitWatermarkGreaterThan(changeLog, threshold, 5);
    }

    private void awaitWatermarkGreaterThan(ChangeLog changeLog, long threshold, long deadlineSeconds) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(deadlineSeconds);
        while (changeLog.getSafeWatermark() <= threshold && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
        System.out.println("eski " + threshold);
        System.out.println("yeni " + changeLog.getSafeWatermark());
        assertTrue(changeLog.getSafeWatermark() > threshold);
    }

    private EntryMetadata createTestMetadata() {
        Prefix prefix = new Prefix("test-prefix");
        return new EntryMetadata(1L, prefix.asBytes(), 100L, 50L, 1);
    }

    @Test
    void shouldRecordAppendOperation_withUserVersion() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-changelog");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        EntryMetadata metadata = createTestMetadata();
        Prefix prefix = new Prefix("test-prefix");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.appendOperation(tr, metadata, prefix, 0);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(CHANGELOG_SUBSPACE));
            List<KeyValue> results = tr.getRange(range).asList().join();

            assertEquals(1, results.size());

            byte[] value = results.getFirst().getValue();
            Tuple valueTuple = Tuple.fromBytes(value);
            assertEquals(metadata.segmentId(), valueTuple.getLong(0));
            assertEquals(metadata.position(), valueTuple.getLong(1));
            assertEquals(metadata.length(), valueTuple.getLong(2));
            assertEquals(prefix.asLong(), valueTuple.getLong(3));
        }
    }

    @Test
    void shouldRecordInsertOperation() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-changelog");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        EntryMetadata metadata = createTestMetadata();
        Prefix prefix = new Prefix("test-prefix");
        Versionstamp versionstamp = TestUtil.generateVersionstamp(0);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.insertOperation(tr, metadata, prefix, versionstamp);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(CHANGELOG_SUBSPACE));
            List<KeyValue> results = tr.getRange(range).asList().join();

            assertEquals(1, results.size());

            byte[] value = results.getFirst().getValue();
            Tuple valueTuple = Tuple.fromBytes(value);
            assertEquals(metadata.segmentId(), valueTuple.getLong(0));
            assertEquals(metadata.position(), valueTuple.getLong(1));
            assertEquals(metadata.length(), valueTuple.getLong(2));
            assertEquals(prefix.asLong(), valueTuple.getLong(3));
        }
    }

    @Test
    void shouldRecordDeleteOperation() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-changelog");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        EntryMetadata metadata = createTestMetadata();
        Prefix prefix = new Prefix("test-prefix");
        Versionstamp versionstamp = TestUtil.generateVersionstamp(0);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.deleteOperation(tr, metadata, prefix, versionstamp);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(CHANGELOG_SUBSPACE));
            List<KeyValue> results = tr.getRange(range).asList().join();

            assertEquals(1, results.size());

            byte[] key = results.getFirst().getKey();
            Tuple keyTuple = subspace.unpack(key);

            long rawParentOpKind = keyTuple.getLong(2);
            ParentOperationKind parentOpKind = ParentOperationKind.valueOf((byte) rawParentOpKind);
            assertEquals(ParentOperationKind.FINALIZATION, parentOpKind);

            long rawOpKind = keyTuple.getLong(3);
            OperationKind opKind = OperationKind.valueOf((byte) rawOpKind);
            assertEquals(OperationKind.DELETE, opKind);
        }
    }

    @Test
    void shouldMaintainChronologicalOrder_forMultipleOperations() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-changelog");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        long length = 50;
        List<EntryMetadata> metadataList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            EntryMetadata metadata = new EntryMetadata(0, prefix.asBytes(), length * i, length, 0);
            metadataList.add(metadata);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < metadataList.size(); i++) {
                changeLog.appendOperation(tr, metadataList.get(i), prefix, i);
            }
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(CHANGELOG_SUBSPACE));
            List<KeyValue> results = tr.getRange(range).asList().join();

            assertEquals(5, results.size());

            long previousLogSeq = -1;
            for (KeyValue kv : results) {
                Tuple keyTuple = subspace.unpack(kv.getKey());
                long logSeq = keyTuple.getLong(1);
                assertTrue(logSeq > previousLogSeq);
                previousLogSeq = logSeq;
            }
        }
    }

    @Test
    void shouldReturnZeroWhenChangelogIsEmpty() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-changelog-empty");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            long sequenceNumber = ChangeLog.getLatestSequenceNumber(tr, subspace);
            assertEquals(0L, sequenceNumber);
        }
    }

    @Test
    void shouldReturnLatestSequenceNumber() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-changelog-latest");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        EntryMetadata metadata = createTestMetadata();
        Prefix prefix = new Prefix("test-prefix");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.appendOperation(tr, metadata, prefix, 0);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            long sequenceNumber = ChangeLog.getLatestSequenceNumber(tr, subspace);
            assertTrue(sequenceNumber > 0);
        }
    }

    @Test
    void shouldReturnHighestSequenceNumberWithMultipleEntries() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-changelog-multiple");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        long length = 50;
        for (int i = 0; i < 5; i++) {
            EntryMetadata metadata = new EntryMetadata(0, prefix.asBytes(), length * i, length, 0);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                changeLog.appendOperation(tr, metadata, prefix, i);
                tr.commit().join();
            }
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            long latestSeq = ChangeLog.getLatestSequenceNumber(tr, subspace);

            Range range = subspace.range(Tuple.from(CHANGELOG_SUBSPACE));
            List<KeyValue> results = tr.getRange(range).asList().join();

            long maxSeq = 0;
            for (KeyValue kv : results) {
                Tuple keyTuple = subspace.unpack(kv.getKey());
                long seq = keyTuple.getLong(1);
                if (seq > maxSeq) {
                    maxSeq = seq;
                }
            }

            assertEquals(maxSeq, latestSeq);
        }
    }

    @Test
    void shouldRecordUpdateOperation() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-changelog-update");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        EntryMetadata prevMetadata = new EntryMetadata(1L, prefix.asBytes(), 100L, 50L, 1);
        EntryMetadata metadata = new EntryMetadata(2L, prefix.asBytes(), 200L, 75L, 2);
        Versionstamp versionstamp = TestUtil.generateVersionstamp(0);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.updateOperation(tr, prevMetadata, metadata, prefix, versionstamp);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(CHANGELOG_SUBSPACE));
            List<KeyValue> results = tr.getRange(range).asList().join();

            assertEquals(1, results.size());

            byte[] key = results.getFirst().getKey();
            Tuple keyTuple = subspace.unpack(key);

            long rawParentOpKind = keyTuple.getLong(2);
            ParentOperationKind parentOpKind = ParentOperationKind.valueOf((byte) rawParentOpKind);
            assertEquals(ParentOperationKind.LIFECYCLE, parentOpKind);

            long rawOpKind = keyTuple.getLong(3);
            OperationKind opKind = OperationKind.valueOf((byte) rawOpKind);
            assertEquals(OperationKind.UPDATE, opKind);

            byte[] value = results.getFirst().getValue();
            Tuple valueTuple = Tuple.fromBytes(value);

            // Verify previous metadata
            assertEquals(metadata.segmentId(), valueTuple.getLong(0));
            assertEquals(metadata.position(), valueTuple.getLong(1));
            assertEquals(metadata.length(), valueTuple.getLong(2));

            // Verify current metadata
            assertEquals(prevMetadata.segmentId(), valueTuple.getLong(3));
            assertEquals(prevMetadata.position(), valueTuple.getLong(4));
            assertEquals(prevMetadata.length(), valueTuple.getLong(5));

            // Verify prefix
            assertEquals(prefix.asLong(), valueTuple.getLong(6));
        }
    }

    @Test
    void shouldReturnSequenceNumber_whenExactPositionMatch() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-aligned-seq");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        long segmentId = 1L;
        long position = 100L;
        long length = 50L;

        EntryMetadata metadata = new EntryMetadata(segmentId, prefix.asBytes(), position, length, 1);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.appendOperation(tr, metadata, prefix, 0);
            tr.commit().join();
        }

        // Retrieve the versionstamp from the changelog entry
        Versionstamp versionstamp;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(CHANGELOG_SUBSPACE));
            List<KeyValue> results = tr.getRange(range).asList().join();
            Tuple keyTuple = subspace.unpack(results.getFirst().getKey());
            versionstamp = keyTuple.getVersionstamp(4);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SegmentTailPointer pointer = new SegmentTailPointer(versionstamp, position, length);
            long result = ChangeLog.resolveTailSequenceNumber(tr, subspace, segmentId, pointer);
            long expected = ChangeLog.getLatestSequenceNumber(tr, subspace);
            // Should return the actual sequence number
            assertEquals(expected, result);
        }
    }

    @Test
    void shouldReturnNegativeOne_whenChangelogIsEmpty() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-aligned-seq-empty");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            long segmentId = 1L;
            Versionstamp versionstamp = TestUtil.generateVersionstamp(0);
            SegmentTailPointer pointer = new SegmentTailPointer(versionstamp, 100L, 50L);

            long result = ChangeLog.resolveTailSequenceNumber(tr, subspace, segmentId, pointer);

            assertEquals(-1, result);
        }
    }

    @Test
    void shouldReturnNegativeOne_whenNoMatchingSegmentId() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-aligned-seq-no-segment");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        long segmentId = 1L;
        long position = 100L;
        long length = 50L;

        EntryMetadata metadata = new EntryMetadata(segmentId, prefix.asBytes(), position, length, 1);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.appendOperation(tr, metadata, prefix, 0);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            long differentSegmentId = 999L;
            Versionstamp versionstamp = TestUtil.generateVersionstamp(0);
            SegmentTailPointer pointer = new SegmentTailPointer(versionstamp, position, length);

            long result = ChangeLog.resolveTailSequenceNumber(tr, subspace, differentSegmentId, pointer);

            assertEquals(-1, result);
        }
    }

    @Test
    void shouldIncludeEntryAtLastSequenceNumber() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-aligned-seq-at-last");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        long segmentId = 1L;
        long position = 100L;
        long length = 50L;

        EntryMetadata metadata = new EntryMetadata(segmentId, prefix.asBytes(), position, length, 1);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.appendOperation(tr, metadata, prefix, 0);
            tr.commit().join();
        }

        // Retrieve the actual sequence number and versionstamp of the entry
        long entrySequenceNumber;
        Versionstamp versionstamp;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(CHANGELOG_SUBSPACE));
            List<KeyValue> results = tr.getRange(range).asList().join();
            Tuple keyTuple = subspace.unpack(results.getFirst().getKey());
            entrySequenceNumber = keyTuple.getLong(1);
            versionstamp = keyTuple.getVersionstamp(4);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SegmentTailPointer pointer = new SegmentTailPointer(versionstamp, position, length);
            long result = ChangeLog.resolveTailSequenceNumber(tr, subspace, segmentId, pointer);

            assertEquals(entrySequenceNumber, result);
        }
    }

    @Test
    void shouldMatchWithUpdateOperation() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-aligned-seq-update");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");
        Versionstamp entryVersionstamp = TestUtil.generateVersionstamp(0);

        // Previous metadata (old location)
        long prevSegmentId = 1L;
        long prevPosition = 50L;
        long prevLength = 30L;
        EntryMetadata prevMetadata = new EntryMetadata(prevSegmentId, prefix.asBytes(), prevPosition, prevLength, 1);

        // Current metadata (new location in active segment)
        long segmentId = 2L;
        long position = 100L;
        long length = 50L;
        EntryMetadata metadata = new EntryMetadata(segmentId, prefix.asBytes(), position, length, 2);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.updateOperation(tr, prevMetadata, metadata, prefix, entryVersionstamp);
            tr.commit().join();
        }

        // Retrieve the versionstamp from the changelog entry
        Versionstamp versionstamp;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(CHANGELOG_SUBSPACE));
            List<KeyValue> results = tr.getRange(range).asList().join();
            Tuple keyTuple = subspace.unpack(results.getFirst().getKey());
            versionstamp = keyTuple.getVersionstamp(4);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SegmentTailPointer pointer = new SegmentTailPointer(versionstamp, position, length);
            long result = ChangeLog.resolveTailSequenceNumber(tr, subspace, segmentId, pointer);

            long expected = ChangeLog.getLatestSequenceNumber(tr, subspace);
            // Should return the actual sequence number
            assertEquals(expected, result);
        }
    }

    @Test
    void shouldReturnNegativeOne_whenEntriesRemovedByRetention() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-aligned-seq-retention");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        long segmentId = 1L;
        long position = 100L;
        long length = 50L;

        EntryMetadata metadata = new EntryMetadata(segmentId, prefix.asBytes(), position, length, 1);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.appendOperation(tr, metadata, prefix, 0);
            tr.commit().join();
        }

        // Simulate retention cleanup by deleting the entry
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(CHANGELOG_SUBSPACE));
            tr.clear(range);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Versionstamp versionstamp = TestUtil.generateVersionstamp(0);
            SegmentTailPointer pointer = new SegmentTailPointer(versionstamp, position, length);
            long result = ChangeLog.resolveTailSequenceNumber(tr, subspace, segmentId, pointer);
            assertEquals(-1, result);
        }
    }

    @Test
    void shouldThrowInconsistentVolumeMetadataException_whenVersionstampMismatch() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-aligned-seq-mismatch");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        long segmentId = 1L;
        long position = 100L;
        long length = 50L;

        EntryMetadata metadata = new EntryMetadata(segmentId, prefix.asBytes(), position, length, 1);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.appendOperation(tr, metadata, prefix, 0);
            tr.commit().join();
        }

        // Use a different versionstamp than what's in the changelog
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Versionstamp wrongVersionstamp = TestUtil.generateVersionstamp(999);
            SegmentTailPointer pointer = new SegmentTailPointer(wrongVersionstamp, position, length);

            assertThrows(InconsistentVolumeMetadataException.class, () ->
                    ChangeLog.resolveTailSequenceNumber(tr, subspace, segmentId, pointer));
        }
    }

    @Test
    void shouldThrowInconsistentVolumeMetadataException_whenPositionMismatch() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-aligned-seq-pos-mismatch");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        long segmentId = 1L;
        long position = 100L;
        long length = 50L;

        EntryMetadata metadata = new EntryMetadata(segmentId, prefix.asBytes(), position, length, 1);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.appendOperation(tr, metadata, prefix, 0);
            tr.commit().join();
        }

        // Retrieve the versionstamp from the changelog entry
        Versionstamp versionstamp;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(CHANGELOG_SUBSPACE));
            List<KeyValue> results = tr.getRange(range).asList().join();
            Tuple keyTuple = subspace.unpack(results.getFirst().getKey());
            versionstamp = keyTuple.getVersionstamp(4);
        }

        // Use the correct versionstamp but wrong position (misaligned by 10)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SegmentTailPointer pointer = new SegmentTailPointer(versionstamp, position, length + 10);

            assertThrows(InconsistentVolumeMetadataException.class, () ->
                    ChangeLog.resolveTailSequenceNumber(tr, subspace, segmentId, pointer));
        }
    }

    @Test
    void shouldCreateBackPointerForAppendOperation() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-backpointer-append");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        long segmentId = 1L;
        long position = 100L;
        long length = 50L;

        EntryMetadata metadata = new EntryMetadata(segmentId, prefix.asBytes(), position, length, 1);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.appendOperation(tr, metadata, prefix, 0);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<Long> sequenceNumbers = ChangeLog.reverseLookup(tr, subspace, segmentId, position);
            assertEquals(1, sequenceNumbers.size());

            long expectedSeqNum = ChangeLog.getLatestSequenceNumber(tr, subspace);
            assertEquals(expectedSeqNum, sequenceNumbers.getFirst());
        }
    }

    @Test
    void shouldCreateBackPointerForDeleteOperation() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-backpointer-delete");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");
        Versionstamp versionstamp = TestUtil.generateVersionstamp(0);

        long segmentId = 1L;
        long position = 100L;
        long length = 50L;

        EntryMetadata metadata = new EntryMetadata(segmentId, prefix.asBytes(), position, length, 1);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.deleteOperation(tr, metadata, prefix, versionstamp);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<Long> sequenceNumbers = ChangeLog.reverseLookup(tr, subspace, segmentId, position);
            assertEquals(1, sequenceNumbers.size());

            long expectedSeqNum = ChangeLog.getLatestSequenceNumber(tr, subspace);
            assertEquals(expectedSeqNum, sequenceNumbers.getFirst());
        }
    }

    @Test
    void shouldCreateBackPointersForUpdateOperation() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-backpointer-update");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");
        Versionstamp versionstamp = TestUtil.generateVersionstamp(0);

        // Previous metadata (old location)
        long prevSegmentId = 1L;
        long prevPosition = 50L;
        long prevLength = 30L;
        EntryMetadata prevMetadata = new EntryMetadata(prevSegmentId, prefix.asBytes(), prevPosition, prevLength, 1);

        // Current metadata (new location)
        long segmentId = 2L;
        long position = 100L;
        long length = 50L;
        EntryMetadata metadata = new EntryMetadata(segmentId, prefix.asBytes(), position, length, 2);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.updateOperation(tr, prevMetadata, metadata, prefix, versionstamp);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            long expectedSeqNum = ChangeLog.getLatestSequenceNumber(tr, subspace);

            // Check back pointer for the new position
            List<Long> newPositionSeqNums = ChangeLog.reverseLookup(tr, subspace, segmentId, position);
            assertEquals(1, newPositionSeqNums.size());
            assertEquals(expectedSeqNum, newPositionSeqNums.getFirst());

            // Check back pointer for the previous position
            List<Long> prevPositionSeqNums = ChangeLog.reverseLookup(tr, subspace, prevSegmentId, prevPosition);
            assertEquals(1, prevPositionSeqNums.size());
            assertEquals(expectedSeqNum, prevPositionSeqNums.getFirst());
        }
    }

    @Test
    void shouldReturnEmptyListForNonExistentBackPointer() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-backpointer-nonexistent");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<Long> sequenceNumbers = ChangeLog.reverseLookup(tr, subspace, 999L, 999L);
            assertTrue(sequenceNumbers.isEmpty());
        }
    }

    @Test
    void shouldReturnMultipleSequenceNumbersForSamePosition() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-backpointer-multiple");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        long segmentId = 1L;
        long position = 100L;
        long length = 50L;

        // First append at this position
        EntryMetadata metadata1 = new EntryMetadata(segmentId, prefix.asBytes(), position, length, 1);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.appendOperation(tr, metadata1, prefix, 0);
            tr.commit().join();
        }

        // Delete at the same position (reusing the same segment position)
        Versionstamp versionstamp = TestUtil.generateVersionstamp(0);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.deleteOperation(tr, metadata1, prefix, versionstamp);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<Long> sequenceNumbers = ChangeLog.reverseLookup(tr, subspace, segmentId, position);
            assertEquals(2, sequenceNumbers.size());
            // Sequence numbers should be in ascending order (back pointer keys are sorted)
            assertTrue(sequenceNumbers.get(0) < sequenceNumbers.get(1));
        }
    }

    @Test
    void shouldPruneEntriesWithinSequenceNumberRange() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-cleanup");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        long segmentId = 1L;
        long length = 50L;

        // Insert multiple entries across separate transactions to get different sequence numbers
        List<Long> sequenceNumbers = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            EntryMetadata metadata = new EntryMetadata(segmentId, prefix.asBytes(), length * i, length, i);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                changeLog.appendOperation(tr, metadata, prefix, i);
                tr.commit().join();
            }
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                sequenceNumbers.add(ChangeLog.getLatestSequenceNumber(tr, subspace));
            }
        }

        // Verify we have 5 entries before cleanup
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(CHANGELOG_SUBSPACE));
            List<KeyValue> results = tr.getRange(range).asList().join();
            assertEquals(5, results.size());
        }

        // Cleanup entries from the first to the third sequence number (inclusive)
        long start = sequenceNumbers.get(0);
        long end = sequenceNumbers.get(2);
        long maxPosition = length * 2; // Position of the third entry
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.prune(tr, start, end, Map.of(segmentId, maxPosition));
            tr.commit().join();
        }

        // Verify only the last 2 entries remain
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(CHANGELOG_SUBSPACE));
            List<KeyValue> results = tr.getRange(range).asList().join();
            assertEquals(2, results.size());

            // Verify the remaining entries have sequence numbers greater than the cleaned range
            for (KeyValue kv : results) {
                Tuple keyTuple = subspace.unpack(kv.getKey());
                long seqNum = keyTuple.getLong(1);
                assertTrue(seqNum > end);
            }
        }
    }

    @Test
    void shouldPruneBackPointersWithinRange() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-cleanup-backpointer");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        long segmentId = 1L;
        long length = 50L;

        // Insert multiple entries across separate transactions to get different sequence numbers
        List<Long> sequenceNumbers = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            long position = length * i;
            EntryMetadata metadata = new EntryMetadata(segmentId, prefix.asBytes(), position, length, i);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                changeLog.appendOperation(tr, metadata, prefix, i);
                tr.commit().join();
            }
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                sequenceNumbers.add(ChangeLog.getLatestSequenceNumber(tr, subspace));
            }
        }

        // Verify back pointers exist for all 5 entries
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < 5; i++) {
                long position = length * i;
                List<Long> backPointers = ChangeLog.reverseLookup(tr, subspace, segmentId, position);
                assertEquals(1, backPointers.size());
            }
        }

        // Cleanup entries from the first to the third sequence number (inclusive)
        long start = sequenceNumbers.get(0);
        long end = sequenceNumbers.get(2);
        long maxPosition = length * 3; // One past the third entry's position to include it in the range
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.prune(tr, start, end, Map.of(segmentId, maxPosition));
            tr.commit().join();
        }

        // Verify back pointers for the first 3 entries are cleaned up
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < 3; i++) {
                long position = length * i;
                List<Long> backPointers = ChangeLog.reverseLookup(tr, subspace, segmentId, position);
                assertTrue(backPointers.isEmpty(), "Back pointer at position " + position + " should be cleaned up");
            }

            // Verify back pointers for the last 2 entries still exist
            for (int i = 3; i < 5; i++) {
                long position = length * i;
                List<Long> backPointers = ChangeLog.reverseLookup(tr, subspace, segmentId, position);
                assertEquals(1, backPointers.size(), "Back pointer at position " + position + " should still exist");
            }
        }
    }

    @Test
    void shouldCalculateCutoffEnd() {
        long retentionPeriod = 24; // 24 hours
        long beforeCall = context.now();
        long cutoffEnd = ChangeLog.calculateCutoffEnd(context, retentionPeriod);
        long afterCall = context.now();

        long minExpected = HybridLogicalClock.fromPhysicalTime(beforeCall - (24 * 60 * 60 * 1000));
        long maxExpected = HybridLogicalClock.fromPhysicalTime(afterCall - (24 * 60 * 60 * 1000));

        assertTrue(cutoffEnd >= minExpected && cutoffEnd <= maxExpected);
    }

    @Test
    void shouldThrowWhenRetentionPeriodIsZero() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> ChangeLog.calculateCutoffEnd(context, 0)
        );
        assertEquals("retention period must be greater than zero", exception.getMessage());
    }

    @Test
    void shouldThrowWhenRetentionPeriodIsNegative() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> ChangeLog.calculateCutoffEnd(context, -1)
        );
        assertEquals("retention period must be greater than zero", exception.getMessage());
    }

    @Test
    void shouldReturnZeroBeforeAnyOperations() {
        // Behavior: A fresh ChangeLog instance returns 0 as a safe watermark since no operations have been generated.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-watermark-zero");
        ChangeLog changeLog = new ChangeLog(context, subspace);

        assertEquals(0, changeLog.getSafeWatermark());
    }

    @Test
    void shouldTrackInFlightSequenceNumbers() {
        // Behavior: An uncommitted transaction's HLC is tracked as in-flight, so the safe watermark
        // is less than the generated sequence number.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-watermark-inflight");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        EntryMetadata metadata = createTestMetadata();
        Prefix prefix = new Prefix("test-prefix");

        Transaction tr = context.getFoundationDB().createTransaction();
        changeLog.appendOperation(tr, metadata, prefix, 0);

        // The HLC is in-flight (transaction not committed), so the watermark must be less than what was generated
        long watermark = changeLog.getSafeWatermark();

        // Watermark should be sequenceNumber - 1 (the only in-flight entry minus 1)
        // We can verify it's strictly less than what would be the sequence number by committing and checking
        tr.commit().join();

        // After commit, the getVersionstamp future completes and removes the in-flight entry
        // Give the callback a moment to fire
        try (Transaction readTr = context.getFoundationDB().createTransaction()) {
            long committedSeq = ChangeLog.getLatestSequenceNumber(readTr, subspace);
            assertTrue(watermark < committedSeq);
        }

        tr.close();
    }

    @Test
    void shouldRemoveFromInFlightAfterCommit() {
        // Behavior: After a transaction commits, its HLC is removed from the in-flight set and
        // the safe watermark equals the committed sequence number.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-watermark-commit");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        EntryMetadata metadata = createTestMetadata();
        Prefix prefix = new Prefix("test-prefix");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.appendOperation(tr, metadata, prefix, 0);
            tr.commit().join();
        }

        // After commit, the getVersionstamp future completes and the callback removes the in-flight entry
        try (Transaction readTr = context.getFoundationDB().createTransaction()) {
            long committedSeq = ChangeLog.getLatestSequenceNumber(readTr, subspace);
            awaitWatermark(changeLog, committedSeq);
        }
    }

    @Test
    void shouldRemoveFromInFlightAfterTransactionClose() {
        // Behavior: Closing a transaction without committing removes the HLC from the in-flight set,
        // since the getVersionstamp() future completes exceptionally.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-watermark-close");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        EntryMetadata metadata = createTestMetadata();
        Prefix prefix = new Prefix("test-prefix");

        CompletableFuture<byte[]> vsFuture;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.appendOperation(tr, metadata, prefix, 0);
            vsFuture = tr.getVersionstamp();
            // Not committing — just closing
        }

        // Wait for the future to complete (exceptionally, since transaction was closed without commit)
        vsFuture.handle((result, error) -> null).join();

        // The in-flight entry should have been removed. Since no successful commit happened,
        // watermark equals lastGeneratedSequenceNumber (set is now empty).
        long watermark = changeLog.getSafeWatermark();
        assertTrue(watermark > 0, "Watermark should reflect the generated sequence number");
    }

    @Test
    void shouldReturnMinMinusOneWhenMultipleInFlight() {
        // Behavior: With multiple in-flight transactions, the safe watermark is min(in_flight) - 1.
        // After one commits, the watermark advances to the new minimum.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-watermark-multiple");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        EntryMetadata metadata1 = new EntryMetadata(1L, prefix.asBytes(), 100L, 50L, 1);
        EntryMetadata metadata2 = new EntryMetadata(1L, prefix.asBytes(), 150L, 50L, 2);

        Transaction tr1 = context.getFoundationDB().createTransaction();
        changeLog.insertOperation(tr1, metadata1, prefix, TestUtil.generateVersionstamp(0));

        long watermarkAfterFirst = changeLog.getSafeWatermark();

        Transaction tr2 = context.getFoundationDB().createTransaction();
        changeLog.insertOperation(tr2, metadata2, prefix, TestUtil.generateVersionstamp(1));

        // Watermark should not change since the first (lower) HLC is still in-flight
        assertEquals(watermarkAfterFirst, changeLog.getSafeWatermark());

        // Commit the second transaction (higher HLC)
        tr2.commit().join();
        tr2.close();

        // Watermark should still be min(in_flight) - 1, which hasn't changed
        // (tr1 with the lower HLC is still in-flight)
        assertEquals(watermarkAfterFirst, changeLog.getSafeWatermark());

        // Now commit the first transaction (lower HLC)
        tr1.commit().join();
        tr1.close();

        // Now both are committed, watermark should advance to the lastGeneratedSequenceNumber
        awaitWatermarkGreaterThan(changeLog, watermarkAfterFirst);
    }

    @Test
    void shouldAdvanceWatermarkAsTransactionsComplete() {
        // Behavior: With 3 in-flight transactions committed in reverse order, the watermark stays at
        // min(remaining) - 1 until all complete, then equals the last generated sequence number.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-watermark-reverse");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        EntryMetadata metadata1 = new EntryMetadata(1L, prefix.asBytes(), 0L, 50L, 1);
        EntryMetadata metadata2 = new EntryMetadata(1L, prefix.asBytes(), 50L, 50L, 2);
        EntryMetadata metadata3 = new EntryMetadata(1L, prefix.asBytes(), 100L, 50L, 3);

        Transaction tr1 = context.getFoundationDB().createTransaction();
        changeLog.insertOperation(tr1, metadata1, prefix, TestUtil.generateVersionstamp(0));
        long watermarkAfterFirst = changeLog.getSafeWatermark();

        Transaction tr2 = context.getFoundationDB().createTransaction();
        changeLog.insertOperation(tr2, metadata2, prefix, TestUtil.generateVersionstamp(1));

        Transaction tr3 = context.getFoundationDB().createTransaction();
        changeLog.insertOperation(tr3, metadata3, prefix, TestUtil.generateVersionstamp(2));

        // All three are in-flight, watermark = min(all three) - 1
        assertEquals(watermarkAfterFirst, changeLog.getSafeWatermark());

        // Commit in reverse order: tr3 first
        tr3.commit().join();
        tr3.close();
        // Watermark unchanged — tr1 (lowest) still in-flight
        assertEquals(watermarkAfterFirst, changeLog.getSafeWatermark());

        // Commit tr2
        tr2.commit().join();
        tr2.close();
        // Watermark unchanged — tr1 (lowest) still in-flight
        assertEquals(watermarkAfterFirst, changeLog.getSafeWatermark());

        // Commit tr1 (the lowest)
        tr1.commit().join();
        tr1.close();
        // All committed — watermark should advance past all of them
        awaitWatermarkGreaterThan(changeLog, watermarkAfterFirst);
    }

    @Test
    void shouldMaintainChronologicalOrder_withMixedOperationsAndOutOfOrderVersionstamps() {
        // Behavior: Mixed APPEND, INSERT, UPDATE, and DELETE operations maintain HLC ordering in the changelog
        // even when INSERTs carry versionstamps older than preceding APPENDs.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-mixed-ops-out-of-order");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        long segmentId = 1L;
        long length = 50L;

        // Tx1: Two APPEND operations with userVersion 5 and 6
        EntryMetadata appendMeta1 = new EntryMetadata(segmentId, prefix.asBytes(), 0L, length, 1);
        EntryMetadata appendMeta2 = new EntryMetadata(segmentId, prefix.asBytes(), 50L, length, 2);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.appendOperation(tr, appendMeta1, prefix, 5);
            changeLog.appendOperation(tr, appendMeta2, prefix, 6);
            tr.commit().join();
        }

        // Read back committed APPEND entries to extract the trVersion
        byte[] trVersion;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(CHANGELOG_SUBSPACE));
            List<KeyValue> results = tr.getRange(range).asList().join();
            assertEquals(2, results.size());
            Tuple keyTuple = subspace.unpack(results.getFirst().getKey());
            Versionstamp committedVs = keyTuple.getVersionstamp(4);
            trVersion = committedVs.getTransactionVersion();
        }

        // Fabricate complete versionstamps with the same trVersion but lower userVersions
        Versionstamp insertVs1 = Versionstamp.complete(trVersion, 1);
        Versionstamp insertVs2 = Versionstamp.complete(trVersion, 2);
        Versionstamp updateVs = Versionstamp.complete(trVersion, 3);
        Versionstamp deleteVs = Versionstamp.complete(trVersion, 4);

        // Tx2: Two INSERT operations with "older" versionstamps
        EntryMetadata insertMeta1 = new EntryMetadata(segmentId, prefix.asBytes(), 100L, length, 3);
        EntryMetadata insertMeta2 = new EntryMetadata(segmentId, prefix.asBytes(), 150L, length, 4);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.insertOperation(tr, insertMeta1, prefix, insertVs1);
            changeLog.insertOperation(tr, insertMeta2, prefix, insertVs2);
            tr.commit().join();
        }

        // Tx3: UPDATE operation
        EntryMetadata updatePrevMeta = new EntryMetadata(segmentId, prefix.asBytes(), 100L, length, 3);
        EntryMetadata updateNewMeta = new EntryMetadata(segmentId, prefix.asBytes(), 200L, length, 5);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.updateOperation(tr, updatePrevMeta, updateNewMeta, prefix, updateVs);
            tr.commit().join();
        }

        // Tx4: DELETE operation
        EntryMetadata deleteMeta = new EntryMetadata(segmentId, prefix.asBytes(), 50L, length, 2);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.deleteOperation(tr, deleteMeta, prefix, deleteVs);
            tr.commit().join();
        }

        // Verification
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            // 1. Count: Exactly 6 entries
            assertEquals(6, entries.size());

            // 2. HLC ordering: Sequence numbers are strictly increasing
            long previousSeqNum = -1;
            for (ChangeLogEntry entry : entries) {
                long seqNum = entry.getAfter().orElseGet(() -> entry.getBefore().orElseThrow()).sequenceNumber();
                assertTrue(seqNum > previousSeqNum, "Sequence numbers must be strictly increasing");
                previousSeqNum = seqNum;
            }

            // 3. Operation kinds in expected order
            assertEquals(OperationKind.APPEND, entries.get(0).getKind());
            assertEquals(OperationKind.APPEND, entries.get(1).getKind());
            assertEquals(OperationKind.INSERT, entries.get(2).getKind());
            assertEquals(OperationKind.INSERT, entries.get(3).getKind());
            assertEquals(OperationKind.UPDATE, entries.get(4).getKind());
            assertEquals(OperationKind.DELETE, entries.get(5).getKind());

            // 4. Versionstamp age assertion: INSERT versionstamps are older than APPEND versionstamps
            assertTrue(
                    entries.get(2).getVersionstamp().compareTo(entries.get(0).getVersionstamp()) < 0,
                    "INSERT versionstamp should be older than APPEND"
            );
            assertTrue(
                    entries.get(3).getVersionstamp().compareTo(entries.get(0).getVersionstamp()) < 0,
                    "INSERT versionstamp should be older than APPEND"
            );

            // 5. Coordinate metadata
            // APPENDs: after present, before absent
            assertTrue(entries.get(0).hasAfter());
            assertFalse(entries.get(0).hasBefore());
            ChangeLogCoordinate append1After = entries.get(0).getAfter().orElseThrow();
            assertEquals(segmentId, append1After.segmentId());
            assertEquals(0L, append1After.position());
            assertEquals(length, append1After.length());

            assertTrue(entries.get(1).hasAfter());
            assertFalse(entries.get(1).hasBefore());
            ChangeLogCoordinate append2After = entries.get(1).getAfter().orElseThrow();
            assertEquals(segmentId, append2After.segmentId());
            assertEquals(50L, append2After.position());
            assertEquals(length, append2After.length());

            // INSERTs: after present, before absent
            assertTrue(entries.get(2).hasAfter());
            assertFalse(entries.get(2).hasBefore());
            ChangeLogCoordinate insert1After = entries.get(2).getAfter().orElseThrow();
            assertEquals(segmentId, insert1After.segmentId());
            assertEquals(100L, insert1After.position());
            assertEquals(length, insert1After.length());

            assertTrue(entries.get(3).hasAfter());
            assertFalse(entries.get(3).hasBefore());
            ChangeLogCoordinate insert2After = entries.get(3).getAfter().orElseThrow();
            assertEquals(segmentId, insert2After.segmentId());
            assertEquals(150L, insert2After.position());
            assertEquals(length, insert2After.length());

            // UPDATE: both before and after present
            assertTrue(entries.get(4).hasAfter());
            assertTrue(entries.get(4).hasBefore());
            ChangeLogCoordinate updateAfter = entries.get(4).getAfter().orElseThrow();
            assertEquals(segmentId, updateAfter.segmentId());
            assertEquals(200L, updateAfter.position());
            assertEquals(length, updateAfter.length());
            ChangeLogCoordinate updateBefore = entries.get(4).getBefore().orElseThrow();
            assertEquals(segmentId, updateBefore.segmentId());
            assertEquals(100L, updateBefore.position());
            assertEquals(length, updateBefore.length());

            // DELETE: before present, after absent
            assertFalse(entries.get(5).hasAfter());
            assertTrue(entries.get(5).hasBefore());
            ChangeLogCoordinate deleteBefore = entries.get(5).getBefore().orElseThrow();
            assertEquals(segmentId, deleteBefore.segmentId());
            assertEquals(50L, deleteBefore.position());
            assertEquals(length, deleteBefore.length());
        }
    }

    @Test
    void shouldCleanUpInFlightAfterCancelledTransaction() {
        // Behavior: Cancelling a transaction completes the getVersionstamp() future exceptionally,
        // which immediately removes the in-flight sequence number and advances the watermark.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-watermark-cancelled");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        EntryMetadata metadata = createTestMetadata();
        Prefix prefix = new Prefix("test-prefix");

        Transaction tr = context.getFoundationDB().createTransaction();
        changeLog.appendOperation(tr, metadata, prefix, 0);
        long watermarkWhileInFlight = changeLog.getSafeWatermark();

        tr.cancel();
        tr.close();

        // The future completes exceptionally right away — no need for a long deadline
        awaitWatermarkGreaterThan(changeLog, watermarkWhileInFlight);
    }

    @Test
    void shouldCleanUpInFlightAfterAbandonedTransaction() {
        // Behavior: An abandoned transaction (neither committed nor cancelled) triggers the
        // orTimeout safety net, which removes the in-flight sequence number and allows the
        // watermark to advance.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-watermark-cancel");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        EntryMetadata metadata = createTestMetadata();
        Prefix prefix = new Prefix("test-prefix");

        Transaction tr = context.getFoundationDB().createTransaction();

        // While the entry is in-flight, watermark = inFlightSequenceNumbers.first() - 1

        // Do NOT cancel or close — the getVersionstamp() future hangs indefinitely.
        // Only the orTimeout(10s) safety net will fire and remove the in-flight entry.
        // Once removed, the watermark advances to the lastGeneratedSequenceNumber (= watermarkWhileInFlight + 1).
        try (tr) {
            changeLog.appendOperation(tr, metadata, prefix, 0);
            long watermarkWhileInFlight = changeLog.getSafeWatermark();
            awaitWatermarkGreaterThan(changeLog, watermarkWhileInFlight, 15);
        }
    }

    @Test
    void shouldMaintainMonotonicWatermark() {
        // Behavior: The safe watermark never decreases across a sequence of committed transactions.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-watermark-monotonic");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        long previousWatermark = 0;
        for (int i = 0; i < 20; i++) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 50L * i, 50L, i);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                changeLog.appendOperation(tr, metadata, prefix, i);
                tr.commit().join();
            }

            awaitWatermarkGreaterThan(changeLog, previousWatermark);
            long current = changeLog.getSafeWatermark();
            assertTrue(current >= previousWatermark,
                    "Watermark decreased from " + previousWatermark + " to " + current);
            previousWatermark = current;
        }

        // The final watermark should equal the latest sequence number
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            long latest = ChangeLog.getLatestSequenceNumber(tr, subspace);
            assertEquals(latest, changeLog.getSafeWatermark());
        }
    }

    @Test
    void shouldAdvanceWatermarkUnderConcurrentWriters() throws InterruptedException {
        // Behavior: Concurrent writers produce a monotonically non-decreasing watermark sequence,
        // and after all writers finish, the watermark reaches the final generated value.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-watermark-concurrent");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        int writerCount = 50;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(writerCount);

        // Start writers
        for (int i = 0; i < writerCount; i++) {
            final int idx = i;
            Thread.startVirtualThread(() -> {
                try {
                    startLatch.await();
                    EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 50L * idx, 50L, idx);
                    try (Transaction tr = context.getFoundationDB().createTransaction()) {
                        changeLog.appendOperation(tr, metadata, prefix, idx);
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(ThreadLocalRandom.current().nextInt(50)));
                        tr.commit().join();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Reader thread: sample watermark values
        CopyOnWriteArrayList<Long> samples = new CopyOnWriteArrayList<>();
        AtomicBoolean readerRunning = new AtomicBoolean(true);
        Thread reader = Thread.startVirtualThread(() -> {
            while (readerRunning.get()) {
                samples.add(changeLog.getSafeWatermark());
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
        });

        // Release all writers
        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "Writers did not finish in time");

        // Stop reader
        readerRunning.set(false);
        reader.join(5000);

        // Watermark should reach its final value
        awaitWatermarkGreaterThan(changeLog, 0);

        // Verify recorded watermark values are monotonically non-decreasing
        long prev = 0;
        for (long sample : samples) {
            assertTrue(sample >= prev, "Watermark decreased from " + prev + " to " + sample);
            prev = sample;
        }
    }

    @Test
    void shouldAdvanceWatermarkWhenConcurrentWritersAbandon() throws InterruptedException {
        // Behavior: A mix of committed and abandoned transactions does not permanently stall
        // the watermark. Abandoned transactions are cleaned up by the orTimeout(10s) safety net,
        // after which the watermark advances past its in-flight value.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-watermark-mixed-rollback");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        int threadCount = 100;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            Thread.startVirtualThread(() -> {
                try {
                    startLatch.await();
                    EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 50L * idx, 50L, idx);
                    Transaction tr = context.getFoundationDB().createTransaction();
                    changeLog.appendOperation(tr, metadata, prefix, idx % 100);
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(ThreadLocalRandom.current().nextInt(50)));

                    if (idx % 2 == 0) {
                        // Commit — getVersionstamp() future completes normally
                        tr.commit().join();
                        tr.close();
                    }
                    // Abandon — do NOT cancel or close.
                    // getVersionstamp() future hangs until orTimeout(10s) fires.
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "Threads did not finish in time");

        // Record the watermark while rolled-back in-flight entries still exist
        long watermarkBeforeTimeout = changeLog.getSafeWatermark();

        // The orTimeout(10s) safety net needs time to fire for abandoned transactions.
        // Once all in-flight entries are cleaned up, the watermark advances past this point.
        awaitWatermarkGreaterThan(changeLog, watermarkBeforeTimeout, 15);
    }
}