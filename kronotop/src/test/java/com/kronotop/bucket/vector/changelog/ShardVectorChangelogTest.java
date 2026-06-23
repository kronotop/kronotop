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

package com.kronotop.bucket.vector.changelog;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.bucket.index.MutationLogMarker;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;

class ShardVectorChangelogTest extends BaseStandaloneInstanceTest {

    private byte[] objectId(int seed) {
        byte[] objectId = new byte[ShardVectorChangelogValue.OBJECT_ID_LENGTH];
        Arrays.fill(objectId, (byte) seed);
        return objectId;
    }

    /** Spins until the wall clock has advanced at least {@code minDeltaMillis} past {@code fromMillis}. */
    private void awaitClockAdvance(long fromMillis, long minDeltaMillis) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (context.now() < fromMillis + minDeltaMillis && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        assertTrue(context.now() >= fromMillis + minDeltaMillis, "clock did not advance");
    }

    private void appendCommitted(ShardVectorChangelog changelog, long indexId, MutationLogMarker marker,
                                 byte[] objectId, int userVersion) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changelog.append(tr, indexId, marker, objectId, userVersion);
            tr.commit().join();
        }
    }

    @Test
    void shouldAppendAndReadBack_singleEntry() {
        // Behavior: a single appended entry round-trips through the value codec, and the stored
        // timestamp is bracketed by the wall clock observed around the append.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("vector-changelog");
        ShardVectorChangelog changelog = new ShardVectorChangelog(context, subspace);

        long before = context.now();
        appendCommitted(changelog, 42L, MutationLogMarker.INSERT, objectId(7), 0);
        long after = context.now();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> results = changelog.getRange(tr);
            assertEquals(1, results.size());

            ShardVectorChangelogValue value = ShardVectorChangelogValue.decode(results.getFirst().getValue());
            assertEquals(42L, value.indexId());
            assertEquals(MutationLogMarker.INSERT, value.marker());
            assertArrayEquals(objectId(7), value.objectId());
            assertTrue(value.timestamp() >= before && value.timestamp() <= after);
        }
    }

    @Test
    void shouldOrderEntriesByVersionstamp_acrossTransactions() {
        // Behavior: entries appended in separate transactions are stored in strictly increasing
        // versionstamp order.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("vector-changelog");
        ShardVectorChangelog changelog = new ShardVectorChangelog(context, subspace);

        for (int i = 0; i < 5; i++) {
            appendCommitted(changelog, i, MutationLogMarker.INSERT, objectId(i), 0);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> results = changelog.getRange(tr);
            assertEquals(5, results.size());

            Versionstamp previous = null;
            for (KeyValue kv : results) {
                Versionstamp current = subspace.unpack(kv.getKey()).getVersionstamp(1);
                if (previous != null) {
                    assertTrue(current.compareTo(previous) > 0, "versionstamps must strictly increase");
                }
                previous = current;
            }
        }
    }

    @Test
    void shouldWriteDistinctOrderedKeys_forMultipleIndexIdsInSameTransaction() {
        // Behavior: one document touching several vector indexes writes one entry per index in a
        // single transaction; distinct userVersions keep the keys distinct and ordered while
        // sharing the same transaction version.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("vector-changelog");
        ShardVectorChangelog changelog = new ShardVectorChangelog(context, subspace);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changelog.append(tr, 10L, MutationLogMarker.INSERT, objectId(1), 0);
            changelog.append(tr, 20L, MutationLogMarker.INSERT, objectId(2), 1);
            changelog.append(tr, 30L, MutationLogMarker.INSERT, objectId(3), 2);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> results = changelog.getRange(tr);
            assertEquals(3, results.size());

            long[] expectedIndexIds = {10L, 20L, 30L};
            byte[] sharedTransactionVersion = null;
            for (int i = 0; i < results.size(); i++) {
                KeyValue kv = results.get(i);
                assertEquals(expectedIndexIds[i], ShardVectorChangelogValue.decode(kv.getValue()).indexId());

                Versionstamp versionstamp = subspace.unpack(kv.getKey()).getVersionstamp(1);
                assertEquals(i, versionstamp.getUserVersion());
                if (sharedTransactionVersion == null) {
                    sharedTransactionVersion = versionstamp.getTransactionVersion();
                } else {
                    assertArrayEquals(sharedTransactionVersion, versionstamp.getTransactionVersion());
                }
            }
        }
    }

    @Test
    void shouldTruncateEntriesOlderThanRetention_andKeepNewer() {
        // Behavior: truncate clears entries older than the retention window and preserves the rest.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("vector-changelog");
        ShardVectorChangelog changelog = new ShardVectorChangelog(context, subspace);

        appendCommitted(changelog, 1L, MutationLogMarker.INSERT, objectId(1), 0);
        appendCommitted(changelog, 2L, MutationLogMarker.INSERT, objectId(2), 0);

        long tBefore = context.now();
        awaitClockAdvance(tBefore, 50);
        long midpoint = context.now();

        appendCommitted(changelog, 3L, MutationLogMarker.INSERT, objectId(3), 0);
        appendCommitted(changelog, 4L, MutationLogMarker.INSERT, objectId(4), 0);

        int cleared;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // cutoff = now() - retention = midpoint -> only the older batch (timestamps < midpoint) clears.
            long retentionMillis = context.now() - midpoint;
            cleared = changelog.truncate(tr, retentionMillis, TimeUnit.MILLISECONDS);
            tr.commit().join();
        }
        assertEquals(2, cleared);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> results = changelog.getRange(tr);
            assertEquals(2, results.size());
            for (KeyValue kv : results) {
                ShardVectorChangelogValue value = ShardVectorChangelogValue.decode(kv.getValue());
                assertTrue(value.timestamp() >= midpoint);
                assertTrue(value.indexId() == 3L || value.indexId() == 4L);
            }
        }
    }

    @Test
    void shouldTruncateAll_whenAllEntriesExpired() {
        // Behavior: when every entry is older than the retention window, truncate clears the whole log.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("vector-changelog");
        ShardVectorChangelog changelog = new ShardVectorChangelog(context, subspace);

        appendCommitted(changelog, 1L, MutationLogMarker.INSERT, objectId(1), 0);
        appendCommitted(changelog, 2L, MutationLogMarker.INSERT, objectId(2), 0);

        long last = context.now();
        awaitClockAdvance(last, 5);

        int cleared;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            cleared = changelog.truncate(tr, 1, TimeUnit.MILLISECONDS);
            tr.commit().join();
        }
        assertEquals(2, cleared);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertTrue(changelog.getRange(tr).isEmpty());
        }
    }

    @Test
    void shouldBeNoOp_whenTruncatingEmptyRange() {
        // Behavior: truncating an empty changelog clears nothing and leaves the range empty.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("vector-changelog");
        ShardVectorChangelog changelog = new ShardVectorChangelog(context, subspace);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertEquals(0, changelog.truncate(tr, 7, TimeUnit.DAYS));
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertTrue(changelog.getRange(tr).isEmpty());
        }
    }

    @Test
    void shouldBeNoOp_whenAllEntriesWithinWindow() {
        // Behavior: when every entry is within the retention window, truncate clears nothing.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("vector-changelog");
        ShardVectorChangelog changelog = new ShardVectorChangelog(context, subspace);

        appendCommitted(changelog, 1L, MutationLogMarker.INSERT, objectId(1), 0);
        appendCommitted(changelog, 2L, MutationLogMarker.INSERT, objectId(2), 0);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertEquals(0, changelog.truncate(tr, 7, TimeUnit.DAYS));
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertEquals(2, changelog.getRange(tr).size());
        }
    }

    @Test
    void shouldThrow_whenRetentionPeriodNotPositive() {
        // Behavior: a non-positive retention period is rejected.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("vector-changelog");
        ShardVectorChangelog changelog = new ShardVectorChangelog(context, subspace);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertThrows(IllegalArgumentException.class, () -> changelog.truncate(tr, 0, TimeUnit.DAYS));
            assertThrows(IllegalArgumentException.class, () -> changelog.truncate(tr, -1, TimeUnit.DAYS));
        }
    }
}
