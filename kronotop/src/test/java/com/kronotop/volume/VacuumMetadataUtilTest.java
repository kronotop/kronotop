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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class VacuumMetadataUtilTest extends BaseStandaloneInstanceTest {
    private VolumeSubspace subspace;

    @BeforeEach
    void setupSubspace() {
        DirectorySubspace ds = createOrOpenSubspaceUnderCluster("vacuum-metadata-util-test");
        subspace = new VolumeSubspace(ds);
    }

    @Test
    void shouldSaveAndLoad() {
        // Behavior: save persists status and startedAt; load reconstructs the same VacuumMetadata.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumSegmentMetadataUtil.save(tr, subspace, 42L, VacuumMetadataStatus.ANALYZE, 1000L);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumSegmentMetadata metadata = VacuumSegmentMetadataUtil.load(tr, subspace, 42L);
            assertNotNull(metadata);
            assertEquals(42L, metadata.segmentId());
            assertEquals(VacuumMetadataStatus.ANALYZE, metadata.status());
            assertEquals(1000L, metadata.startedAt());
        }
    }

    @Test
    void shouldReturnNullWhenNoMetadataExists() {
        // Behavior: load returns null for a segment that has never been analyzed.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumSegmentMetadata metadata = VacuumSegmentMetadataUtil.load(tr, subspace, 999L);
            assertNull(metadata);
        }
    }

    @Test
    void shouldReadStatus() {
        // Behavior: readStatus returns the persisted status byte.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumSegmentMetadataUtil.save(tr, subspace, 10L, VacuumMetadataStatus.ANALYZE, 5000L);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumMetadataStatus status = VacuumSegmentMetadataUtil.readStatus(tr, subspace, 10L);
            assertEquals(VacuumMetadataStatus.ANALYZE, status);
        }
    }

    @Test
    void shouldThrowWhenReadingStatusOfNonExistentSegment() {
        // Behavior: readStatus throws IllegalStateException for a missing segment.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertThrows(IllegalStateException.class, () -> VacuumSegmentMetadataUtil.readStatus(tr, subspace, 999L));
        }
    }

    @Test
    void shouldUpdateStatus() {
        // Behavior: updateStatus changes the status without affecting startedAt.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumSegmentMetadataUtil.save(tr, subspace, 7L, VacuumMetadataStatus.ANALYZE, 2000L);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumSegmentMetadataUtil.setStatus(tr, subspace, 7L, VacuumMetadataStatus.EVACUATING);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumSegmentMetadata metadata = VacuumSegmentMetadataUtil.load(tr, subspace, 7L);
            assertNotNull(metadata);
            assertEquals(VacuumMetadataStatus.EVACUATING, metadata.status());
            assertEquals(2000L, metadata.startedAt());
        }
    }

    @Test
    void shouldSavePrefixCardinalityAndIterateViaRange() {
        // Behavior: savePrefixCardinality stores correct values; getVacuumPrefixRange iterates over them.
        Prefix alpha = new Prefix("alpha");
        Prefix beta = new Prefix("beta");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumSegmentMetadataUtil.save(tr, subspace, 5L, VacuumMetadataStatus.ANALYZE, 3000L);
            VacuumSegmentMetadataUtil.savePrefixCardinality(tr, subspace, 5L, alpha, 100);
            VacuumSegmentMetadataUtil.savePrefixCardinality(tr, subspace, 5L, beta, 50);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = VacuumSegmentMetadataUtil.getVacuumPrefixRange(subspace, 5L);
            List<KeyValue> results = tr.getRange(range).asList().join();
            assertEquals(2, results.size());

            Map<String, Integer> cardinalities = new HashMap<>();
            for (KeyValue kv : results) {
                Tuple tuple = subspace.getDirectorySubspace().unpack(kv.getKey());
                byte[] prefixBytes = tuple.getBytes(4);
                Prefix prefix = Prefix.fromBytes(prefixBytes);
                int cardinality = ByteBuffer.wrap(kv.getValue()).order(ByteOrder.LITTLE_ENDIAN).getInt();
                cardinalities.put(prefix.toString(), cardinality);
            }

            assertEquals(100, cardinalities.get(alpha.toString()));
            assertEquals(50, cardinalities.get(beta.toString()));
        }
    }

    @Test
    void shouldDeleteAllVacuumMetadataForSegment() {
        // Behavior: delete clears status, startedAt, and all prefix cardinalities for the segment.
        Prefix alpha = new Prefix("alpha");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumSegmentMetadataUtil.save(tr, subspace, 3L, VacuumMetadataStatus.ANALYZE, 4000L);
            VacuumSegmentMetadataUtil.savePrefixCardinality(tr, subspace, 3L, alpha, 25);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumSegmentMetadataUtil.delete(tr, subspace, 3L);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertNull(VacuumSegmentMetadataUtil.load(tr, subspace, 3L));

            Range range = VacuumSegmentMetadataUtil.getVacuumPrefixRange(subspace, 3L);
            List<KeyValue> results = tr.getRange(range).asList().join();
            assertTrue(results.isEmpty());
        }
    }

    @Test
    void shouldNotAffectOtherSegmentsOnDelete() {
        // Behavior: deleting one segment's vacuum metadata leaves another segment's metadata intact.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumSegmentMetadataUtil.save(tr, subspace, 1L, VacuumMetadataStatus.ANALYZE, 1000L);
            VacuumSegmentMetadataUtil.save(tr, subspace, 2L, VacuumMetadataStatus.EVACUATING, 2000L);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumSegmentMetadataUtil.delete(tr, subspace, 1L);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertNull(VacuumSegmentMetadataUtil.load(tr, subspace, 1L));

            VacuumSegmentMetadata metadata = VacuumSegmentMetadataUtil.load(tr, subspace, 2L);
            assertNotNull(metadata);
            assertEquals(VacuumMetadataStatus.EVACUATING, metadata.status());
            assertEquals(2000L, metadata.startedAt());
        }
    }
}
