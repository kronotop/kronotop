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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VolumeMetadataUtilTest extends BaseStandaloneInstanceTest {
    private VolumeSubspace subspace;

    @BeforeEach
    void setupSubspace() {
        DirectorySubspace ds = createOrOpenSubspaceUnderCluster("volume-metadata-util-test");
        subspace = new VolumeSubspace(ds);
    }

    @Test
    void shouldCreateNewVolumeMetadata() {
        // Behavior: createOrOpen on an empty subspace initializes a new volume with READWRITE status and segment 0.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadata metadata = VolumeMetadataUtil.createOrOpen(tr, subspace);
            tr.commit().join();

            assertNotEquals(0L, metadata.id());
            assertEquals(VolumeStatus.READWRITE, metadata.status());
            assertEquals(List.of(0L), metadata.segmentIds());
        }
    }

    @Test
    void shouldOpenExistingVolumeMetadata() {
        // Behavior: createOrOpen on an already-initialized subspace returns the same metadata without reinitializing.
        long originalId;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadata created = VolumeMetadataUtil.createOrOpen(tr, subspace);
            originalId = created.id();
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadata opened = VolumeMetadataUtil.createOrOpen(tr, subspace);

            assertEquals(originalId, opened.id());
            assertEquals(VolumeStatus.READWRITE, opened.status());
            assertEquals(List.of(0L), opened.segmentIds());
        }
    }

    @Test
    void shouldReadVolumeId() {
        // Behavior: readVolumeId returns the same ID that createOrOpen generated.
        long expectedId;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadata metadata = VolumeMetadataUtil.createOrOpen(tr, subspace);
            expectedId = metadata.id();
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            long actualId = VolumeMetadataUtil.readVolumeId(tr, subspace);
            assertEquals(expectedId, actualId);
        }
    }

    @Test
    void shouldLoadSegmentIds() {
        // Behavior: loadSegmentIds returns all registered segments in ascending order.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadataUtil.createOrOpen(tr, subspace);

            tr.set(subspace.packVolumeSegmentIdKey(5L), VolumeMetadataUtil.NULL_BYTES);
            tr.set(subspace.packVolumeSegmentIdKey(3L), VolumeMetadataUtil.NULL_BYTES);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<Long> segmentIds = VolumeMetadataUtil.loadSegmentIds(tr, subspace);
            assertEquals(List.of(0L, 3L, 5L), segmentIds);
        }
    }

    @Test
    void shouldReturnEmptyListWhenNoSegmentsExist() {
        // Behavior: loadSegmentIds returns an empty list when no segment keys have been written.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<Long> segmentIds = VolumeMetadataUtil.loadSegmentIds(tr, subspace);
            assertTrue(segmentIds.isEmpty());
        }
    }

    @Test
    void shouldReadVolumeStatus() {
        // Behavior: readVolumeStatus returns the status persisted by createOrOpen.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadataUtil.createOrOpen(tr, subspace);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeStatus status = VolumeMetadataUtil.readVolumeStatus(tr, subspace);
            assertEquals(VolumeStatus.READWRITE, status);
        }
    }

    @Test
    void shouldReadUpdatedVolumeStatus() {
        // Behavior: readVolumeStatus reflects a status change written directly to the status key.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadataUtil.createOrOpen(tr, subspace);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] statusValue = VolumeStatus.READONLY.toString().getBytes(StandardCharsets.US_ASCII);
            tr.set(subspace.packVolumeStatusKey(), statusValue);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeStatus status = VolumeMetadataUtil.readVolumeStatus(tr, subspace);
            assertEquals(VolumeStatus.READONLY, status);
        }
    }

    @Test
    void shouldThrowWhenNoStatusExists() {
        // Behavior: readVolumeStatus throws IllegalStateException on an empty subspace.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertThrows(IllegalStateException.class, () -> VolumeMetadataUtil.readVolumeStatus(tr, subspace));
        }
    }
}
