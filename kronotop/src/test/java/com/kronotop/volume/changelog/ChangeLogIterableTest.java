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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.TestUtil;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.ParentOperationKind;
import com.kronotop.volume.Prefix;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ChangeLogIterableTest extends BaseStandaloneInstanceTest {

    @Test
    void shouldIterateAppendEntries() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-append-entries");
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
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            assertEquals(1, entries.size());

            ChangeLogEntry entry = entries.get(0);
            assertTrue(entry.hasAfter());
            assertFalse(entry.hasBefore());
            assertNotNull(entry.getVersionstamp());
            assertEquals(prefix.asLong(), entry.getPrefix());

            ChangeLogCoordinate after = entry.getAfter().orElseThrow();
            assertEquals(segmentId, after.segmentId());
            assertEquals(position, after.position());
            assertEquals(length, after.length());
        }
    }

    @Test
    void shouldIterateDeleteEntries() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-delete-entries");
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
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            assertEquals(1, entries.size());

            ChangeLogEntry entry = entries.get(0);
            assertFalse(entry.hasAfter());
            assertTrue(entry.hasBefore());
            assertEquals(versionstamp, entry.getVersionstamp());
            assertEquals(prefix.asLong(), entry.getPrefix());

            ChangeLogCoordinate before = entry.getBefore().orElseThrow();
            assertEquals(segmentId, before.segmentId());
            assertEquals(position, before.position());
            assertEquals(length, before.length());
        }
    }

    @Test
    void shouldIterateUpdateEntries() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-update-entries");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");
        Versionstamp versionstamp = TestUtil.generateVersionstamp(0);

        long prevSegmentId = 1L;
        long prevPosition = 50L;
        long prevLength = 30L;
        EntryMetadata prevMetadata = new EntryMetadata(prevSegmentId, prefix.asBytes(), prevPosition, prevLength, 1);

        long segmentId = 2L;
        long position = 100L;
        long length = 50L;
        EntryMetadata metadata = new EntryMetadata(segmentId, prefix.asBytes(), position, length, 2);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.updateOperation(tr, prevMetadata, metadata, prefix, versionstamp);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            assertEquals(1, entries.size());

            ChangeLogEntry entry = entries.get(0);
            assertTrue(entry.hasAfter());
            assertTrue(entry.hasBefore());
            assertEquals(versionstamp, entry.getVersionstamp());
            assertEquals(prefix.asLong(), entry.getPrefix());

            ChangeLogCoordinate after = entry.getAfter().orElseThrow();
            assertEquals(segmentId, after.segmentId());
            assertEquals(position, after.position());
            assertEquals(length, after.length());

            ChangeLogCoordinate before = entry.getBefore().orElseThrow();
            assertEquals(prevSegmentId, before.segmentId());
            assertEquals(prevPosition, before.position());
            assertEquals(prevLength, before.length());
        }
    }

    @Test
    void shouldReturnEmptyIteratorWhenNoEntries() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-empty-entries");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            assertTrue(entries.isEmpty());
        }
    }

    @Test
    void shouldRespectLimitParameter() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-limit-parameter");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        // Create 5 APPEND entries
        for (int i = 0; i < 5; i++) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), i * 50L, 50L, i);
                changeLog.appendOperation(tr, metadata, prefix, i);
                tr.commit().join();
            }
        }

        // Iterate with limit=2
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterableOptions options = new ChangeLogIterableOptions.Builder()
                    .limit(2)
                    .build();
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace, options);
            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            assertEquals(2, entries.size());
        }
    }

    @Test
    void shouldIterateInReverseOrder() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-reverse-order");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        // Create 3 APPEND entries with distinct positions
        for (int i = 0; i < 3; i++) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), i * 50L, 50L, i);
                changeLog.appendOperation(tr, metadata, prefix, i);
                tr.commit().join();
            }
        }

        // Iterate in reverse order
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterableOptions options = new ChangeLogIterableOptions.Builder()
                    .limit(3)
                    .reverse(true)
                    .build();
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace, options);
            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            assertEquals(3, entries.size());

            // Verify descending order by position (last inserted should be first)
            assertEquals(100L, entries.get(0).getAfter().orElseThrow().position());
            assertEquals(50L, entries.get(1).getAfter().orElseThrow().position());
            assertEquals(0L, entries.get(2).getAfter().orElseThrow().position());
        }
    }

    @Test
    void shouldFilterBySequenceNumberRange() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-sequence-range");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        // Create 5 APPEND entries
        for (int i = 0; i < 5; i++) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), i * 50L, 50L, i);
                changeLog.appendOperation(tr, metadata, prefix, i);
                tr.commit().join();
            }
        }

        // Get all entries first to find sequence numbers
        List<Long> sequenceNumbers = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            for (ChangeLogEntry entry : iterable) {
                sequenceNumbers.add(entry.getAfter().orElseThrow().sequenceNumber());
            }
        }
        assertEquals(5, sequenceNumbers.size());

        // Filter to get only entries 2, 3 (index 1 to 3 exclusive)
        long beginSeq = sequenceNumbers.get(1);
        long endSeq = sequenceNumbers.get(3);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterableOptions options = new ChangeLogIterableOptions.Builder()
                    .begin(SequenceNumberSelector.firstGreaterOrEqual(beginSeq))
                    .end(SequenceNumberSelector.firstGreaterOrEqual(endSeq))
                    .build();
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace, options);

            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            assertEquals(2, entries.size());
            assertEquals(50L, entries.get(0).getAfter().orElseThrow().position());
            assertEquals(100L, entries.get(1).getAfter().orElseThrow().position());
        }
    }

    @Test
    void shouldFilterWithFirstGreaterThan() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-first-greater-than");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        // Create 5 APPEND entries
        for (int i = 0; i < 5; i++) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), i * 50L, 50L, i);
                changeLog.appendOperation(tr, metadata, prefix, i);
                tr.commit().join();
            }
        }

        // Get all sequence numbers
        List<Long> sequenceNumbers = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            for (ChangeLogEntry entry : iterable) {
                sequenceNumbers.add(entry.getAfter().orElseThrow().sequenceNumber());
            }
        }

        // firstGreaterThan excludes the begin sequence number
        long beginSeq = sequenceNumbers.get(1);
        long endSeq = sequenceNumbers.get(4);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterableOptions options = new ChangeLogIterableOptions.Builder()
                    .begin(SequenceNumberSelector.firstGreaterThan(beginSeq))
                    .end(SequenceNumberSelector.firstGreaterOrEqual(endSeq))
                    .build();
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace, options);

            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            // Should get entries at index 2, 3 (excludes 1, excludes 4)
            assertEquals(2, entries.size());
            assertEquals(100L, entries.get(0).getAfter().orElseThrow().position());
            assertEquals(150L, entries.get(1).getAfter().orElseThrow().position());
        }
    }

    @Test
    void shouldFilterWithBothFirstGreaterThan() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-both-greater-than");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        // Create 5 APPEND entries
        for (int i = 0; i < 5; i++) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), i * 50L, 50L, i);
                changeLog.appendOperation(tr, metadata, prefix, i);
                tr.commit().join();
            }
        }

        // Get all sequence numbers
        List<Long> sequenceNumbers = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            for (ChangeLogEntry entry : iterable) {
                sequenceNumbers.add(entry.getAfter().orElseThrow().sequenceNumber());
            }
        }

        // Both firstGreaterThan: excludes begin, includes up to but not including end
        long beginSeq = sequenceNumbers.get(0);
        long endSeq = sequenceNumbers.get(3);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterableOptions options = new ChangeLogIterableOptions.Builder()
                    .begin(SequenceNumberSelector.firstGreaterThan(beginSeq))
                    .end(SequenceNumberSelector.firstGreaterThan(endSeq))
                    .build();
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace, options);

            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            // Should get entries at index 1, 2, 3 (excludes 0, includes up to 3)
            assertEquals(3, entries.size());
            assertEquals(50L, entries.get(0).getAfter().orElseThrow().position());
            assertEquals(100L, entries.get(1).getAfter().orElseThrow().position());
            assertEquals(150L, entries.get(2).getAfter().orElseThrow().position());
        }
    }

    @Test
    void shouldPreserveInsertionOrderWithMixedOperations() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-mixed-operations");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        // Operation 1: APPEND
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 0L, 50L, 1);
            changeLog.appendOperation(tr, metadata, prefix, 0);
            tr.commit().join();
        }

        // Operation 2: UPDATE (references operation 1)
        Versionstamp updateVersionstamp = TestUtil.generateVersionstamp(1);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata prevMetadata = new EntryMetadata(1L, prefix.asBytes(), 0L, 50L, 1);
            EntryMetadata newMetadata = new EntryMetadata(1L, prefix.asBytes(), 100L, 60L, 2);
            changeLog.updateOperation(tr, prevMetadata, newMetadata, prefix, updateVersionstamp);
            tr.commit().join();
        }

        // Operation 3: DELETE
        Versionstamp deleteVersionstamp = TestUtil.generateVersionstamp(2);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(2L, prefix.asBytes(), 200L, 40L, 3);
            changeLog.deleteOperation(tr, metadata, prefix, deleteVersionstamp);
            tr.commit().join();
        }

        // Operation 4: APPEND
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(2L, prefix.asBytes(), 300L, 70L, 4);
            changeLog.appendOperation(tr, metadata, prefix, 3);
            tr.commit().join();
        }

        // Verify iteration order and entry types
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            assertEquals(4, entries.size());

            // Entry 0: APPEND (hasAfter=true, hasBefore=false)
            ChangeLogEntry entry0 = entries.get(0);
            assertTrue(entry0.hasAfter());
            assertFalse(entry0.hasBefore());
            assertEquals(0L, entry0.getAfter().orElseThrow().position());

            // Entry 1: UPDATE (hasAfter=true, hasBefore=true)
            ChangeLogEntry entry1 = entries.get(1);
            assertTrue(entry1.hasAfter());
            assertTrue(entry1.hasBefore());
            assertEquals(100L, entry1.getAfter().orElseThrow().position());
            assertEquals(0L, entry1.getBefore().orElseThrow().position());

            // Entry 2: DELETE (hasAfter=false, hasBefore=true)
            ChangeLogEntry entry2 = entries.get(2);
            assertFalse(entry2.hasAfter());
            assertTrue(entry2.hasBefore());
            assertEquals(200L, entry2.getBefore().orElseThrow().position());

            // Entry 3: APPEND (hasAfter=true, hasBefore=false)
            ChangeLogEntry entry3 = entries.get(3);
            assertTrue(entry3.hasAfter());
            assertFalse(entry3.hasBefore());
            assertEquals(300L, entry3.getAfter().orElseThrow().position());
        }
    }

    @Test
    void shouldFilterByLifecycleOperationKind() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-lifecycle-filter");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        // Mixed ordering: DELETE, APPEND, DELETE, UPDATE, APPEND, DELETE

        // Op 1: DELETE (FINALIZATION)
        Versionstamp deleteVersionstamp1 = TestUtil.generateVersionstamp(0);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 0L, 40L, 1);
            changeLog.deleteOperation(tr, metadata, prefix, deleteVersionstamp1);
            tr.commit().join();
        }

        // Op 2: APPEND (LIFECYCLE)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 50L, 50L, 2);
            changeLog.appendOperation(tr, metadata, prefix, 1);
            tr.commit().join();
        }

        // Op 3: DELETE (FINALIZATION)
        Versionstamp deleteVersionstamp2 = TestUtil.generateVersionstamp(2);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 100L, 35L, 3);
            changeLog.deleteOperation(tr, metadata, prefix, deleteVersionstamp2);
            tr.commit().join();
        }

        // Op 4: UPDATE (LIFECYCLE)
        Versionstamp updateVersionstamp = TestUtil.generateVersionstamp(3);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata prevMetadata = new EntryMetadata(1L, prefix.asBytes(), 50L, 50L, 2);
            EntryMetadata newMetadata = new EntryMetadata(1L, prefix.asBytes(), 150L, 60L, 4);
            changeLog.updateOperation(tr, prevMetadata, newMetadata, prefix, updateVersionstamp);
            tr.commit().join();
        }

        // Op 5: APPEND (LIFECYCLE)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 200L, 50L, 5);
            changeLog.appendOperation(tr, metadata, prefix, 4);
            tr.commit().join();
        }

        // Op 6: DELETE (FINALIZATION)
        Versionstamp deleteVersionstamp3 = TestUtil.generateVersionstamp(5);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 250L, 30L, 6);
            changeLog.deleteOperation(tr, metadata, prefix, deleteVersionstamp3);
            tr.commit().join();
        }

        // Get all sequence numbers first
        List<Long> sequenceNumbers = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            for (ChangeLogEntry entry : iterable) {
                long seqNum = entry.hasAfter()
                        ? entry.getAfter().orElseThrow().sequenceNumber()
                        : entry.getBefore().orElseThrow().sequenceNumber();
                sequenceNumbers.add(seqNum);
            }
        }
        assertEquals(6, sequenceNumbers.size());

        // Filter by LIFECYCLE with begin and end selectors
        long beginSeq = sequenceNumbers.get(0);
        long endSeq = sequenceNumbers.get(5);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterableOptions options = new ChangeLogIterableOptions.Builder()
                    .parentOperationKind(ParentOperationKind.LIFECYCLE)
                    .begin(SequenceNumberSelector.firstGreaterOrEqual(beginSeq))
                    .end(SequenceNumberSelector.firstGreaterThan(endSeq))
                    .build();
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace, options);

            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            // Should have 3 LIFECYCLE entries (2 APPEND + 1 UPDATE), excluding DELETEs
            assertEquals(3, entries.size());

            // Entry 0: APPEND at position 50
            assertTrue(entries.get(0).hasAfter());
            assertFalse(entries.get(0).hasBefore());
            assertEquals(50L, entries.get(0).getAfter().orElseThrow().position());

            // Entry 1: UPDATE at position 150
            assertTrue(entries.get(1).hasAfter());
            assertTrue(entries.get(1).hasBefore());
            assertEquals(150L, entries.get(1).getAfter().orElseThrow().position());

            // Entry 2: APPEND at position 200
            assertTrue(entries.get(2).hasAfter());
            assertFalse(entries.get(2).hasBefore());
            assertEquals(200L, entries.get(2).getAfter().orElseThrow().position());
        }
    }

    @Test
    void shouldFilterByFinalizationOperationKind() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-finalization-filter");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        // Mixed ordering: APPEND, DELETE, UPDATE, DELETE, APPEND, DELETE

        // Op 1: APPEND (LIFECYCLE)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 0L, 50L, 1);
            changeLog.appendOperation(tr, metadata, prefix, 0);
            tr.commit().join();
        }

        // Op 2: DELETE (FINALIZATION)
        Versionstamp deleteVersionstamp1 = TestUtil.generateVersionstamp(1);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 100L, 40L, 2);
            changeLog.deleteOperation(tr, metadata, prefix, deleteVersionstamp1);
            tr.commit().join();
        }

        // Op 3: UPDATE (LIFECYCLE)
        Versionstamp updateVersionstamp = TestUtil.generateVersionstamp(2);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata prevMetadata = new EntryMetadata(1L, prefix.asBytes(), 0L, 50L, 1);
            EntryMetadata newMetadata = new EntryMetadata(1L, prefix.asBytes(), 150L, 60L, 3);
            changeLog.updateOperation(tr, prevMetadata, newMetadata, prefix, updateVersionstamp);
            tr.commit().join();
        }

        // Op 4: DELETE (FINALIZATION)
        Versionstamp deleteVersionstamp2 = TestUtil.generateVersionstamp(3);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 200L, 30L, 4);
            changeLog.deleteOperation(tr, metadata, prefix, deleteVersionstamp2);
            tr.commit().join();
        }

        // Op 5: APPEND (LIFECYCLE)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 250L, 50L, 5);
            changeLog.appendOperation(tr, metadata, prefix, 4);
            tr.commit().join();
        }

        // Op 6: DELETE (FINALIZATION)
        Versionstamp deleteVersionstamp3 = TestUtil.generateVersionstamp(5);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 300L, 35L, 6);
            changeLog.deleteOperation(tr, metadata, prefix, deleteVersionstamp3);
            tr.commit().join();
        }

        // Get all sequence numbers first
        List<Long> sequenceNumbers = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            for (ChangeLogEntry entry : iterable) {
                long seqNum = entry.hasAfter()
                        ? entry.getAfter().orElseThrow().sequenceNumber()
                        : entry.getBefore().orElseThrow().sequenceNumber();
                sequenceNumbers.add(seqNum);
            }
        }
        assertEquals(6, sequenceNumbers.size());

        // Filter by FINALIZATION
        long beginSeq = sequenceNumbers.get(0);
        long endSeq = sequenceNumbers.get(5);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterableOptions options = new ChangeLogIterableOptions.Builder()
                    .parentOperationKind(ParentOperationKind.FINALIZATION)
                    .begin(SequenceNumberSelector.firstGreaterOrEqual(beginSeq))
                    .end(SequenceNumberSelector.firstGreaterThan(endSeq))
                    .build();
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace, options);

            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            // Should have 3 FINALIZATION entries (3 DELETEs), excluding APPEND and UPDATE
            assertEquals(3, entries.size());

            // Entry 0: DELETE at position 100
            assertFalse(entries.get(0).hasAfter());
            assertTrue(entries.get(0).hasBefore());
            assertEquals(100L, entries.get(0).getBefore().orElseThrow().position());

            // Entry 1: DELETE at position 200
            assertFalse(entries.get(1).hasAfter());
            assertTrue(entries.get(1).hasBefore());
            assertEquals(200L, entries.get(1).getBefore().orElseThrow().position());

            // Entry 2: DELETE at position 300
            assertFalse(entries.get(2).hasAfter());
            assertTrue(entries.get(2).hasBefore());
            assertEquals(300L, entries.get(2).getBefore().orElseThrow().position());
        }
    }

    @Test
    void shouldCombineParentOperationKindWithSequenceRange() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-parent-op-with-range");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        // Mixed ordering: APPEND, DELETE, APPEND, DELETE, UPDATE, DELETE

        // Op 1: APPEND (LIFECYCLE) - position 0
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 0L, 50L, 1);
            changeLog.appendOperation(tr, metadata, prefix, 0);
            tr.commit().join();
        }

        // Op 2: DELETE (FINALIZATION) - position 50
        Versionstamp deleteVersionstamp1 = TestUtil.generateVersionstamp(1);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 50L, 40L, 2);
            changeLog.deleteOperation(tr, metadata, prefix, deleteVersionstamp1);
            tr.commit().join();
        }

        // Op 3: APPEND (LIFECYCLE) - position 100
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 100L, 50L, 3);
            changeLog.appendOperation(tr, metadata, prefix, 2);
            tr.commit().join();
        }

        // Op 4: DELETE (FINALIZATION) - position 150
        Versionstamp deleteVersionstamp2 = TestUtil.generateVersionstamp(3);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 150L, 35L, 4);
            changeLog.deleteOperation(tr, metadata, prefix, deleteVersionstamp2);
            tr.commit().join();
        }

        // Op 5: UPDATE (LIFECYCLE) - position 200
        Versionstamp updateVersionstamp = TestUtil.generateVersionstamp(4);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata prevMetadata = new EntryMetadata(1L, prefix.asBytes(), 100L, 50L, 3);
            EntryMetadata newMetadata = new EntryMetadata(1L, prefix.asBytes(), 200L, 60L, 5);
            changeLog.updateOperation(tr, prevMetadata, newMetadata, prefix, updateVersionstamp);
            tr.commit().join();
        }

        // Op 6: DELETE (FINALIZATION) - position 250
        Versionstamp deleteVersionstamp3 = TestUtil.generateVersionstamp(5);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 250L, 30L, 6);
            changeLog.deleteOperation(tr, metadata, prefix, deleteVersionstamp3);
            tr.commit().join();
        }

        // Get all sequence numbers first
        List<Long> sequenceNumbers = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            for (ChangeLogEntry entry : iterable) {
                long seqNum = entry.hasAfter()
                        ? entry.getAfter().orElseThrow().sequenceNumber()
                        : entry.getBefore().orElseThrow().sequenceNumber();
                sequenceNumbers.add(seqNum);
            }
        }
        assertEquals(6, sequenceNumbers.size());

        // Filter by LIFECYCLE with range from index 1 to 5 (exclusive)
        // This range contains: DELETE(1), APPEND(2), DELETE(3), UPDATE(4)
        // After LIFECYCLE filter: APPEND(2), UPDATE(4) = 2 entries
        long beginSeq = sequenceNumbers.get(1);
        long endSeq = sequenceNumbers.get(5);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterableOptions options = new ChangeLogIterableOptions.Builder()
                    .parentOperationKind(ParentOperationKind.LIFECYCLE)
                    .begin(SequenceNumberSelector.firstGreaterOrEqual(beginSeq))
                    .end(SequenceNumberSelector.firstGreaterOrEqual(endSeq))
                    .build();
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace, options);

            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            // Should have 2 LIFECYCLE entries within the range (APPEND + UPDATE)
            assertEquals(2, entries.size());

            // Entry 0: APPEND at position 100
            assertTrue(entries.get(0).hasAfter());
            assertFalse(entries.get(0).hasBefore());
            assertEquals(100L, entries.get(0).getAfter().orElseThrow().position());

            // Entry 1: UPDATE at position 200
            assertTrue(entries.get(1).hasAfter());
            assertTrue(entries.get(1).hasBefore());
            assertEquals(200L, entries.get(1).getAfter().orElseThrow().position());
        }

        // Also test FINALIZATION with the same range
        // This range contains: DELETE(1), APPEND(2), DELETE(3), UPDATE(4)
        // After FINALIZATION filter: DELETE(1), DELETE(3) = 2 entries
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterableOptions options = new ChangeLogIterableOptions.Builder()
                    .parentOperationKind(ParentOperationKind.FINALIZATION)
                    .begin(SequenceNumberSelector.firstGreaterOrEqual(beginSeq))
                    .end(SequenceNumberSelector.firstGreaterOrEqual(endSeq))
                    .build();
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace, options);

            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            // Should have 2 FINALIZATION entries within the range (2 DELETEs)
            assertEquals(2, entries.size());

            // Entry 0: DELETE at position 50
            assertFalse(entries.get(0).hasAfter());
            assertTrue(entries.get(0).hasBefore());
            assertEquals(50L, entries.get(0).getBefore().orElseThrow().position());

            // Entry 1: DELETE at position 150
            assertFalse(entries.get(1).hasAfter());
            assertTrue(entries.get(1).hasBefore());
            assertEquals(150L, entries.get(1).getBefore().orElseThrow().position());
        }
    }

    @Test
    void shouldReturnEmptyWhenNoMatchingParentOperationKind() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-no-matching-parent-op");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        // Create only LIFECYCLE operations (APPEND, UPDATE)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 0L, 50L, 1);
            changeLog.appendOperation(tr, metadata, prefix, 0);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 50L, 50L, 2);
            changeLog.appendOperation(tr, metadata, prefix, 1);
            tr.commit().join();
        }

        Versionstamp updateVersionstamp = TestUtil.generateVersionstamp(2);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata prevMetadata = new EntryMetadata(1L, prefix.asBytes(), 0L, 50L, 1);
            EntryMetadata newMetadata = new EntryMetadata(1L, prefix.asBytes(), 100L, 60L, 3);
            changeLog.updateOperation(tr, prevMetadata, newMetadata, prefix, updateVersionstamp);
            tr.commit().join();
        }

        // Get all sequence numbers
        List<Long> sequenceNumbers = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            for (ChangeLogEntry entry : iterable) {
                long seqNum = entry.hasAfter()
                        ? entry.getAfter().orElseThrow().sequenceNumber()
                        : entry.getBefore().orElseThrow().sequenceNumber();
                sequenceNumbers.add(seqNum);
            }
        }
        assertEquals(3, sequenceNumbers.size());

        // Filter by FINALIZATION - should return empty since no DELETE operations
        long beginSeq = sequenceNumbers.get(0);
        long endSeq = sequenceNumbers.get(2);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterableOptions options = new ChangeLogIterableOptions.Builder()
                    .parentOperationKind(ParentOperationKind.FINALIZATION)
                    .begin(SequenceNumberSelector.firstGreaterOrEqual(beginSeq))
                    .end(SequenceNumberSelector.firstGreaterThan(endSeq))
                    .build();
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace, options);

            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            assertTrue(entries.isEmpty());
        }
    }

    @Test
    void shouldCombineParentOperationKindWithReverse() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-parent-op-with-reverse");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        // Mixed ordering: APPEND, DELETE, UPDATE, DELETE, APPEND

        // Op 1: APPEND (LIFECYCLE) - position 0
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 0L, 50L, 1);
            changeLog.appendOperation(tr, metadata, prefix, 0);
            tr.commit().join();
        }

        // Op 2: DELETE (FINALIZATION) - position 50
        Versionstamp deleteVersionstamp1 = TestUtil.generateVersionstamp(1);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 50L, 40L, 2);
            changeLog.deleteOperation(tr, metadata, prefix, deleteVersionstamp1);
            tr.commit().join();
        }

        // Op 3: UPDATE (LIFECYCLE) - position 100
        Versionstamp updateVersionstamp = TestUtil.generateVersionstamp(2);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata prevMetadata = new EntryMetadata(1L, prefix.asBytes(), 0L, 50L, 1);
            EntryMetadata newMetadata = new EntryMetadata(1L, prefix.asBytes(), 100L, 60L, 3);
            changeLog.updateOperation(tr, prevMetadata, newMetadata, prefix, updateVersionstamp);
            tr.commit().join();
        }

        // Op 4: DELETE (FINALIZATION) - position 150
        Versionstamp deleteVersionstamp2 = TestUtil.generateVersionstamp(3);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 150L, 35L, 4);
            changeLog.deleteOperation(tr, metadata, prefix, deleteVersionstamp2);
            tr.commit().join();
        }

        // Op 5: APPEND (LIFECYCLE) - position 200
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 200L, 50L, 5);
            changeLog.appendOperation(tr, metadata, prefix, 4);
            tr.commit().join();
        }

        // Get all sequence numbers
        List<Long> sequenceNumbers = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            for (ChangeLogEntry entry : iterable) {
                long seqNum = entry.hasAfter()
                        ? entry.getAfter().orElseThrow().sequenceNumber()
                        : entry.getBefore().orElseThrow().sequenceNumber();
                sequenceNumbers.add(seqNum);
            }
        }
        assertEquals(5, sequenceNumbers.size());

        long beginSeq = sequenceNumbers.get(0);
        long endSeq = sequenceNumbers.get(4);

        // Filter by LIFECYCLE with reverse - should return APPEND(200), UPDATE(100), APPEND(0)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterableOptions options = new ChangeLogIterableOptions.Builder()
                    .parentOperationKind(ParentOperationKind.LIFECYCLE)
                    .begin(SequenceNumberSelector.firstGreaterOrEqual(beginSeq))
                    .end(SequenceNumberSelector.firstGreaterThan(endSeq))
                    .reverse(true)
                    .build();
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace, options);

            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            assertEquals(3, entries.size());

            // Entry 0: APPEND at position 200 (last LIFECYCLE in reverse)
            assertTrue(entries.get(0).hasAfter());
            assertFalse(entries.get(0).hasBefore());
            assertEquals(200L, entries.get(0).getAfter().orElseThrow().position());

            // Entry 1: UPDATE at position 100
            assertTrue(entries.get(1).hasAfter());
            assertTrue(entries.get(1).hasBefore());
            assertEquals(100L, entries.get(1).getAfter().orElseThrow().position());

            // Entry 2: APPEND at position 0 (first LIFECYCLE in reverse)
            assertTrue(entries.get(2).hasAfter());
            assertFalse(entries.get(2).hasBefore());
            assertEquals(0L, entries.get(2).getAfter().orElseThrow().position());
        }

        // Filter by FINALIZATION with reverse - should return DELETE(150), DELETE(50)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterableOptions options = new ChangeLogIterableOptions.Builder()
                    .parentOperationKind(ParentOperationKind.FINALIZATION)
                    .begin(SequenceNumberSelector.firstGreaterOrEqual(beginSeq))
                    .end(SequenceNumberSelector.firstGreaterThan(endSeq))
                    .reverse(true)
                    .build();
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace, options);

            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            assertEquals(2, entries.size());

            // Entry 0: DELETE at position 150 (last FINALIZATION in reverse)
            assertFalse(entries.get(0).hasAfter());
            assertTrue(entries.get(0).hasBefore());
            assertEquals(150L, entries.get(0).getBefore().orElseThrow().position());

            // Entry 1: DELETE at position 50 (first FINALIZATION in reverse)
            assertFalse(entries.get(1).hasAfter());
            assertTrue(entries.get(1).hasBefore());
            assertEquals(50L, entries.get(1).getBefore().orElseThrow().position());
        }
    }

    @Test
    void shouldCombineParentOperationKindWithReverseAndLimit() {
        // This test verifies the interaction between ParentOperationKind filtering, reverse iteration, and limit.
        // The key insight demonstrated is that the limit is applied at the FoundationDB range query level
        // (before iterator filtering), so the test is designed with data that correctly validates this behavior:
        // - 7 operations: APPEND, DELETE, UPDATE, APPEND, APPEND, DELETE, DELETE
        // - With limit=4 and reverse=true, FDB returns the last 4 entries: Op7(DELETE), Op6(DELETE), Op5(APPEND), Op4(APPEND)
        // - LIFECYCLE filter returns: APPEND(200), APPEND(150)
        // - FINALIZATION filter returns: DELETE(300), DELETE(250)
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-parent-op-reverse-limit");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        // Mixed ordering designed so that limit applies before filter correctly
        // We need consecutive LIFECYCLE or FINALIZATION entries at the end to test limit+reverse+filter

        // Op 1: APPEND (LIFECYCLE) - position 0
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 0L, 50L, 1);
            changeLog.appendOperation(tr, metadata, prefix, 0);
            tr.commit().join();
        }

        // Op 2: DELETE (FINALIZATION) - position 50
        Versionstamp deleteVersionstamp1 = TestUtil.generateVersionstamp(1);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 50L, 40L, 2);
            changeLog.deleteOperation(tr, metadata, prefix, deleteVersionstamp1);
            tr.commit().join();
        }

        // Op 3: UPDATE (LIFECYCLE) - position 100
        Versionstamp updateVersionstamp = TestUtil.generateVersionstamp(2);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata prevMetadata = new EntryMetadata(1L, prefix.asBytes(), 0L, 50L, 1);
            EntryMetadata newMetadata = new EntryMetadata(1L, prefix.asBytes(), 100L, 60L, 3);
            changeLog.updateOperation(tr, prevMetadata, newMetadata, prefix, updateVersionstamp);
            tr.commit().join();
        }

        // Op 4: APPEND (LIFECYCLE) - position 150
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 150L, 50L, 4);
            changeLog.appendOperation(tr, metadata, prefix, 3);
            tr.commit().join();
        }

        // Op 5: APPEND (LIFECYCLE) - position 200
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 200L, 50L, 5);
            changeLog.appendOperation(tr, metadata, prefix, 4);
            tr.commit().join();
        }

        // Op 6: DELETE (FINALIZATION) - position 250
        Versionstamp deleteVersionstamp2 = TestUtil.generateVersionstamp(5);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 250L, 30L, 6);
            changeLog.deleteOperation(tr, metadata, prefix, deleteVersionstamp2);
            tr.commit().join();
        }

        // Op 7: DELETE (FINALIZATION) - position 300
        Versionstamp deleteVersionstamp3 = TestUtil.generateVersionstamp(6);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 300L, 30L, 7);
            changeLog.deleteOperation(tr, metadata, prefix, deleteVersionstamp3);
            tr.commit().join();
        }

        // Get all sequence numbers
        List<Long> sequenceNumbers = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            for (ChangeLogEntry entry : iterable) {
                long seqNum = entry.hasAfter()
                        ? entry.getAfter().orElseThrow().sequenceNumber()
                        : entry.getBefore().orElseThrow().sequenceNumber();
                sequenceNumbers.add(seqNum);
            }
        }
        assertEquals(7, sequenceNumbers.size());

        long beginSeq = sequenceNumbers.get(0);
        long endSeq = sequenceNumbers.get(6);

        // Note: limit is applied at FoundationDB range query level, before iterator filtering
        // With limit=4 reverse, we get raw entries: Op7, Op6, Op5, Op4
        // After LIFECYCLE filter: APPEND(200), APPEND(150) = 2 entries
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterableOptions options = new ChangeLogIterableOptions.Builder()
                    .parentOperationKind(ParentOperationKind.LIFECYCLE)
                    .begin(SequenceNumberSelector.firstGreaterOrEqual(beginSeq))
                    .end(SequenceNumberSelector.firstGreaterThan(endSeq))
                    .reverse(true)
                    .limit(4)
                    .build();
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace, options);

            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            assertEquals(2, entries.size());

            // Entry 0: APPEND at position 200 (Op5 in reverse)
            assertTrue(entries.get(0).hasAfter());
            assertFalse(entries.get(0).hasBefore());
            assertEquals(200L, entries.get(0).getAfter().orElseThrow().position());

            // Entry 1: APPEND at position 150 (Op4 in reverse)
            assertTrue(entries.get(1).hasAfter());
            assertFalse(entries.get(1).hasBefore());
            assertEquals(150L, entries.get(1).getAfter().orElseThrow().position());
        }

        // With limit=4 reverse, we get raw entries: Op7, Op6, Op5, Op4
        // After FINALIZATION filter: DELETE(300), DELETE(250) = 2 entries
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterableOptions options = new ChangeLogIterableOptions.Builder()
                    .parentOperationKind(ParentOperationKind.FINALIZATION)
                    .begin(SequenceNumberSelector.firstGreaterOrEqual(beginSeq))
                    .end(SequenceNumberSelector.firstGreaterThan(endSeq))
                    .reverse(true)
                    .limit(4)
                    .build();
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace, options);

            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            assertEquals(2, entries.size());

            // Entry 0: DELETE at position 300 (Op7 in reverse)
            assertFalse(entries.get(0).hasAfter());
            assertTrue(entries.get(0).hasBefore());
            assertEquals(300L, entries.get(0).getBefore().orElseThrow().position());

            // Entry 1: DELETE at position 250 (Op6 in reverse)
            assertFalse(entries.get(1).hasAfter());
            assertTrue(entries.get(1).hasBefore());
            assertEquals(250L, entries.get(1).getBefore().orElseThrow().position());
        }
    }

    @Test
    void shouldCombineParentOperationKindWithLimit() {
        // This test verifies the interaction between ParentOperationKind filtering and limit (forward iteration).
        // The limit is applied at the FoundationDB range query level (before iterator filtering).
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-parent-op-limit");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        // Mixed ordering: DELETE, DELETE, APPEND, UPDATE, DELETE, APPEND
        // First 4 entries contain 2 DELETEs and 2 LIFECYCLEs (APPEND, UPDATE)

        // Op 1: DELETE (FINALIZATION) - position 0
        Versionstamp deleteVersionstamp1 = TestUtil.generateVersionstamp(0);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 0L, 40L, 1);
            changeLog.deleteOperation(tr, metadata, prefix, deleteVersionstamp1);
            tr.commit().join();
        }

        // Op 2: DELETE (FINALIZATION) - position 50
        Versionstamp deleteVersionstamp2 = TestUtil.generateVersionstamp(1);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 50L, 40L, 2);
            changeLog.deleteOperation(tr, metadata, prefix, deleteVersionstamp2);
            tr.commit().join();
        }

        // Op 3: APPEND (LIFECYCLE) - position 100
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 100L, 50L, 3);
            changeLog.appendOperation(tr, metadata, prefix, 2);
            tr.commit().join();
        }

        // Op 4: UPDATE (LIFECYCLE) - position 150
        Versionstamp updateVersionstamp = TestUtil.generateVersionstamp(3);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata prevMetadata = new EntryMetadata(1L, prefix.asBytes(), 100L, 50L, 3);
            EntryMetadata newMetadata = new EntryMetadata(1L, prefix.asBytes(), 150L, 60L, 4);
            changeLog.updateOperation(tr, prevMetadata, newMetadata, prefix, updateVersionstamp);
            tr.commit().join();
        }

        // Op 5: DELETE (FINALIZATION) - position 200
        Versionstamp deleteVersionstamp3 = TestUtil.generateVersionstamp(4);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 200L, 30L, 5);
            changeLog.deleteOperation(tr, metadata, prefix, deleteVersionstamp3);
            tr.commit().join();
        }

        // Op 6: APPEND (LIFECYCLE) - position 250
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            EntryMetadata metadata = new EntryMetadata(1L, prefix.asBytes(), 250L, 50L, 6);
            changeLog.appendOperation(tr, metadata, prefix, 5);
            tr.commit().join();
        }

        // Get all sequence numbers
        List<Long> sequenceNumbers = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            for (ChangeLogEntry entry : iterable) {
                long seqNum = entry.hasAfter()
                        ? entry.getAfter().orElseThrow().sequenceNumber()
                        : entry.getBefore().orElseThrow().sequenceNumber();
                sequenceNumbers.add(seqNum);
            }
        }
        assertEquals(6, sequenceNumbers.size());

        long beginSeq = sequenceNumbers.get(0);
        long endSeq = sequenceNumbers.get(5);

        // With limit=4 forward, we get raw entries: Op1, Op2, Op3, Op4
        // After LIFECYCLE filter: APPEND(100), UPDATE(150) = 2 entries
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterableOptions options = new ChangeLogIterableOptions.Builder()
                    .parentOperationKind(ParentOperationKind.LIFECYCLE)
                    .begin(SequenceNumberSelector.firstGreaterOrEqual(beginSeq))
                    .end(SequenceNumberSelector.firstGreaterThan(endSeq))
                    .limit(4)
                    .build();
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace, options);

            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            assertEquals(2, entries.size());

            // Entry 0: APPEND at position 100 (Op3)
            assertTrue(entries.get(0).hasAfter());
            assertFalse(entries.get(0).hasBefore());
            assertEquals(100L, entries.get(0).getAfter().orElseThrow().position());

            // Entry 1: UPDATE at position 150 (Op4)
            assertTrue(entries.get(1).hasAfter());
            assertTrue(entries.get(1).hasBefore());
            assertEquals(150L, entries.get(1).getAfter().orElseThrow().position());
        }

        // With limit=4 forward, we get raw entries: Op1, Op2, Op3, Op4
        // After FINALIZATION filter: DELETE(0), DELETE(50) = 2 entries
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterableOptions options = new ChangeLogIterableOptions.Builder()
                    .parentOperationKind(ParentOperationKind.FINALIZATION)
                    .begin(SequenceNumberSelector.firstGreaterOrEqual(beginSeq))
                    .end(SequenceNumberSelector.firstGreaterThan(endSeq))
                    .limit(4)
                    .build();
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace, options);

            List<ChangeLogEntry> entries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                entries.add(entry);
            }

            assertEquals(2, entries.size());

            // Entry 0: DELETE at position 0 (Op1)
            assertFalse(entries.get(0).hasAfter());
            assertTrue(entries.get(0).hasBefore());
            assertEquals(0L, entries.get(0).getBefore().orElseThrow().position());

            // Entry 1: DELETE at position 50 (Op2)
            assertFalse(entries.get(1).hasAfter());
            assertTrue(entries.get(1).hasBefore());
            assertEquals(50L, entries.get(1).getBefore().orElseThrow().position());
        }
    }
}