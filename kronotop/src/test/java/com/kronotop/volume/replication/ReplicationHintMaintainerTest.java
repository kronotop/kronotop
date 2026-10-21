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

package com.kronotop.volume.replication;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.AppendedEntry;
import com.kronotop.volume.BaseVolumeIntegrationTest;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.VolumeSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationHintMaintainerTest extends BaseVolumeIntegrationTest {
    private static final int NUMBER = 50;
    private static final int[] MARKED = {0, 3, 7, 20, 49};

    private record PositionAndIndex(long position, long indexId) {
    }

    private AppendResult appendMarkedEntries() throws IOException {
        ReplicationHint hint = new ReplicationHint();
        for (int index : MARKED) {
            hint.markHasVector(index, 1L);
        }
        hint.markHasVector(3, 2L);

        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            session.setReplicationHint(hint);
            AppendResult result = volume.append(session, getEntries(NUMBER));
            tr.commit().join();
            return result;
        }
    }

    private Set<PositionAndIndex> expectedHints(AppendResult result) {
        Set<PositionAndIndex> expected = new HashSet<>();
        AppendedEntry[] entries = result.getAppendedEntries();
        for (int index : MARKED) {
            expected.add(new PositionAndIndex(entries[index].metadata().position(), 1L));
        }
        expected.add(new PositionAndIndex(entries[3].metadata().position(), 2L));
        return expected;
    }

    private long segmentEnd(AppendResult result) {
        long end = 0;
        for (AppendedEntry entry : result.getAppendedEntries()) {
            end = Math.max(end, entry.metadata().position() + entry.metadata().length());
        }
        return end;
    }

    @Test
    void shouldFindEveryHintExactlyOnceWhenScanningByteByByte() throws IOException {
        // Behavior: Sequential scans over every byte boundary return each hint exactly once, never a hint
        // of an entry that ends after the scanned range, and the cursor never moves backwards.
        AppendResult result = appendMarkedEntries();
        long segmentId = result.getAppendedEntries()[0].metadata().segmentId();
        long end = segmentEnd(result);

        List<PositionAndIndex> found = new ArrayList<>();
        long cursor = 0;
        for (long to = 1; to <= end; to++) {
            ReplicationHintScan scan;
            try (Transaction tr = context.getTransactionManager().create()) {
                scan = ReplicationHintMaintainer.scan(tr, subspace, segmentId, cursor, to);
            }
            for (ReplicationHintEntry hint : scan.hints()) {
                assertTrue(hint.metadata().position() + hint.metadata().length() <= to, "Incomplete entry returned at to=" + to);
                assertEquals(ReplicationHintKind.VECTOR_NODE_ADD, hint.kind());
                found.add(new PositionAndIndex(hint.metadata().position(), hint.indexId()));
            }
            assertTrue(scan.cursor() >= cursor, "Cursor moved backwards at to=" + to);
            assertTrue(scan.cursor() <= to);
            cursor = scan.cursor();
        }

        Set<PositionAndIndex> expected = expectedHints(result);
        assertEquals(expected.size(), found.size(), "Duplicate or missing hints: " + found);
        assertEquals(expected, new HashSet<>(found));
        assertEquals(end, cursor);
    }

    @Test
    void shouldFindHintCursorAtEntryCoveringPosition() throws IOException {
        // Behavior: findHintCursor returns the start of the entry that covers the position, or the
        // position itself when it sits on an entry boundary or after the last entry.
        AppendResult result = appendMarkedEntries();
        AppendedEntry[] entries = result.getAppendedEntries();
        long segmentId = entries[0].metadata().segmentId();
        long end = segmentEnd(result);

        for (long position = 0; position <= end + 5; position++) {
            long expected = position;
            for (AppendedEntry entry : entries) {
                long start = entry.metadata().position();
                if (start < position && position < start + entry.metadata().length()) {
                    expected = start;
                }
            }
            long actual;
            try (Transaction tr = context.getTransactionManager().create()) {
                actual = ReplicationHintMaintainer.findHintCursor(tr, subspace, segmentId, position);
            }
            assertEquals(expected, actual, "Wrong cursor at position " + position);
        }
    }

    @Test
    void shouldReadVersionstampsByPosition() throws IOException {
        // Behavior: readVersionstamps returns the versionstamp of every entry in the range keyed by position.
        AppendResult result = appendMarkedEntries();
        AppendedEntry[] entries = result.getAppendedEntries();
        Versionstamp[] keys = result.getVersionstampedKeys();
        long segmentId = entries[0].metadata().segmentId();

        Map<Long, Versionstamp> versionstamps;
        try (Transaction tr = context.getTransactionManager().create()) {
            versionstamps = ReplicationHintMaintainer.readVersionstamps(tr, subspace, segmentId, 0, segmentEnd(result));
        }

        assertEquals(NUMBER, versionstamps.size());
        for (int i = 0; i < NUMBER; i++) {
            EntryMetadata metadata = entries[i].metadata();
            assertEquals(keys[i], versionstamps.get(metadata.position()));
        }
    }
}
