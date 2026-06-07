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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.volume.changelog.ChangeLogCoordinate;
import com.kronotop.volume.changelog.ChangeLogEntry;
import com.kronotop.volume.changelog.ChangeLogIterable;
import com.kronotop.volume.segment.SegmentAnalysis;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VolumeFacadeTest extends BaseVolumeIntegrationTest {

    @Test
    void shouldDeleteEntriesByVersionstampedEntry() throws IOException {
        // Behavior: Appends two entries via Volume, collects their VersionstampedEntry via getRange,
        // deletes them using VolumeFacade.deleteByVersionstampedEntry, and verifies entries are no longer readable.
        ByteBuffer[] entries = getEntries(2);
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }
        Versionstamp[] versionstampedKeys = appendResult.getVersionstampedKeys();

        List<VersionstampedEntry> versionstampedEntries = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            for (VolumeEntry volumeEntry : volume.getRange(session)) {
                versionstampedEntries.add(new VersionstampedEntry(volumeEntry.key(), EntryMetadata.decode(volumeEntry.metadata())));
            }
        }
        assertEquals(2, versionstampedEntries.size());

        VolumeFacade facade = new VolumeFacade(context);
        VolumeSubspace volumeSubspace = new VolumeSubspace(subspace);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            facade.deleteByVersionstampedEntry(volumeSubspace, session, versionstampedEntries.toArray(new VersionstampedEntry[0]));
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            for (Versionstamp versionstamp : versionstampedKeys) {
                assertNull(volume.get(session, versionstamp));
            }
        }
    }

    @Test
    void shouldThrowIllegalArgumentExceptionWithEmptyEntries() {
        // Behavior: Calling deleteByVersionstampedEntry with zero entries throws IllegalArgumentException.
        VolumeFacade facade = new VolumeFacade(context);
        VolumeSubspace volumeSubspace = new VolumeSubspace(subspace);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            assertThrows(IllegalArgumentException.class,
                    () -> facade.deleteByVersionstampedEntry(volumeSubspace, session));
        }
    }

    @Test
    void shouldUpdateSegmentCardinalityAfterDelete() throws IOException {
        // Behavior: Appends 10 entries, deletes 4 via VolumeFacade, and verifies
        // that segment analysis reflects the correct garbage bytes (4 entries × 10 bytes = 40).
        ByteBuffer[] entries = getEntries(10);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        List<VersionstampedEntry> versionstampedEntries = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            for (VolumeEntry volumeEntry : volume.getRange(session)) {
                versionstampedEntries.add(new VersionstampedEntry(volumeEntry.key(), EntryMetadata.decode(volumeEntry.metadata())));
            }
        }
        assertEquals(10, versionstampedEntries.size());

        // Delete entries at indices 3-6 (4 entries)
        VersionstampedEntry[] toDelete = versionstampedEntries.subList(3, 7).toArray(new VersionstampedEntry[0]);
        VolumeFacade facade = new VolumeFacade(context);
        VolumeSubspace volumeSubspace = new VolumeSubspace(subspace);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            facade.deleteByVersionstampedEntry(volumeSubspace, session, toDelete);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<SegmentAnalysis> analysis = volume.analyze(tr);
            SegmentAnalysis segmentAnalysis = analysis.getFirst();
            assertTrue(segmentAnalysis.garbagePercentage() > 0);
            long garbageBytes = segmentAnalysis.size() - segmentAnalysis.freeBytes() - segmentAnalysis.usedBytes();
            assertEquals(40, garbageBytes); // Deleted 4 items, test entry size is 10.
        }
    }

    @Test
    void shouldRecordDeleteOperationInChangeLog() throws IOException {
        // Behavior: Appends entries via Volume, deletes some via VolumeFacade.deleteByVersionstampedEntry,
        // and verifies the ChangeLog contains DELETE entries with correct metadata, versionstamps,
        // and coordinates.
        ByteBuffer[] entries = getEntries(3);
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }
        Versionstamp[] versionstampedKeys = appendResult.getVersionstampedKeys();

        List<VersionstampedEntry> versionstampedEntries = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            for (VolumeEntry volumeEntry : volume.getRange(session)) {
                versionstampedEntries.add(new VersionstampedEntry(volumeEntry.key(), EntryMetadata.decode(volumeEntry.metadata())));
            }
        }
        assertEquals(3, versionstampedEntries.size());

        // Delete the first 2 entries via VolumeFacade
        VersionstampedEntry[] toDelete = versionstampedEntries.subList(0, 2).toArray(new VersionstampedEntry[0]);
        VolumeFacade facade = new VolumeFacade(context);
        VolumeSubspace volumeSubspace = new VolumeSubspace(subspace);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            facade.deleteByVersionstampedEntry(volumeSubspace, session, toDelete);
            tr.commit().join();
        }

        // Read ChangeLog and verify DELETE entries
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace);
            List<ChangeLogEntry> changeLogEntries = new ArrayList<>();
            for (ChangeLogEntry entry : iterable) {
                changeLogEntries.add(entry);
            }

            // 3 APPENDs + 2 DELETEs = 5 entries
            assertEquals(5, changeLogEntries.size());

            // The first 3 are APPENDs
            for (int i = 0; i < 3; i++) {
                assertEquals(OperationKind.APPEND, changeLogEntries.get(i).getKind());
            }

            // The last 2 are DELETEs
            for (int i = 3; i < 5; i++) {
                ChangeLogEntry deleteEntry = changeLogEntries.get(i);
                assertEquals(OperationKind.DELETE, deleteEntry.getKind());

                // DELETE: before present, after absent
                assertTrue(deleteEntry.hasBefore());
                assertFalse(deleteEntry.hasAfter());

                // Verify coordinates match the deleted entry's metadata
                int deletedIndex = i - 3;
                EntryMetadata deletedMetadata = toDelete[deletedIndex].metadata();
                ChangeLogCoordinate before = deleteEntry.getBefore().orElseThrow();
                assertEquals(deletedMetadata.segmentId(), before.segmentId());
                assertEquals(deletedMetadata.position(), before.position());
                assertEquals(deletedMetadata.length(), before.length());

                // Verify versionstamp matches the original appended entry
                assertEquals(versionstampedKeys[deletedIndex], deleteEntry.getVersionstamp());

                // Verify prefix
                assertEquals(stashVolumeSyncerPrefix.asLong(), deleteEntry.getPrefix());
            }
        }
    }
}
