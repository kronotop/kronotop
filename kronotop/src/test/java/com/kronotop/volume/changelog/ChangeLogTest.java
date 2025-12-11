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

import static com.kronotop.volume.Subspaces.CHANGELOG_SUBSPACE;
import static org.junit.jupiter.api.Assertions.*;

class ChangeLogTest extends BaseStandaloneInstanceTest {

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
    void shouldRecordAppendOperation_withVersionstamp() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-changelog");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        EntryMetadata metadata = createTestMetadata();
        Prefix prefix = new Prefix("test-prefix");
        Versionstamp versionstamp = TestUtil.generateVersionstamp(0);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.appendOperation(tr, metadata, prefix, versionstamp);
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
        ChangeLog changeLog = new ChangeLog(context, subspace);

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

        // Delete at the same position (reusing the same segment position after vacuum)
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
}