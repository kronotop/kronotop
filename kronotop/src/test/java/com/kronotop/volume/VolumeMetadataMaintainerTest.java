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
import com.kronotop.directory.KronotopDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VolumeMetadataMaintainerTest extends BaseStandaloneInstanceTest {
    private VolumeSubspace subspace;

    @BeforeEach
    void setupSubspace() {
        DirectorySubspace ds = createOrOpenSubspaceUnderCluster("volume-metadata-maintainer-test");
        subspace = new VolumeSubspace(ds);
    }

    @Test
    void shouldCreateVolumeSubspace() {
        // Behavior: When the shard has no volume directory, createVolumeSubspace creates it in the given
        // transaction and returns a subspace whose path is the volume directory of that shard under the
        // cluster metadata.
        List<String> expectedPath = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                volumes().
                volume(Integer.toString(UNUSED_SHARD_ID)).
                toList();

        try (Transaction tr = context.getTransactionManager().create()) {
            DirectorySubspace volumeSubspace = VolumeMetadataMaintainer.createVolumeSubspace(context, tr, UNUSED_SHARD_ID);

            assertEquals(expectedPath, volumeSubspace.getPath());
        }
    }

    @Test
    void shouldThrowWhenVolumeSubspaceAlreadyExists() {
        // Behavior: When createVolumeSubspace has already created the volume directory of the shard in the
        // same transaction, a second call throws VolumeAlreadyExistsException with the message
        // "Volume already exists: '<volume name>'".
        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeMetadataMaintainer.createVolumeSubspace(context, tr, UNUSED_SHARD_ID);

            VolumeAlreadyExistsException exception = assertThrows(
                    VolumeAlreadyExistsException.class,
                    () -> VolumeMetadataMaintainer.createVolumeSubspace(context, tr, UNUSED_SHARD_ID)
            );
            assertEquals(
                    String.format("Volume already exists: '%s'", VolumeNames.format(UNUSED_SHARD_ID)),
                    exception.getMessage()
            );
        }
    }

    @Test
    void shouldCreateNewVolumeMetadata() {
        // Behavior: createOrOpen on an empty subspace initializes a new volume with READWRITE status and segment 0.
        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeMetadata metadata = VolumeMetadataMaintainer.createOrOpen(tr, subspace);
            tr.commit().join();

            assertNotEquals(0L, metadata.id());
            assertEquals(VolumeStatus.READWRITE, metadata.status());
            assertEquals(List.of(0L), metadata.segmentIds());
        }
    }

    @Test
    void shouldOpenExistingVolumeMetadata() {
        // Behavior: createOrOpen on a subspace that already holds volume metadata returns that metadata unchanged.
        long originalId;
        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeMetadata created = VolumeMetadataMaintainer.createOrOpen(tr, subspace);
            originalId = created.id();
            tr.commit().join();
        }

        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeMetadata opened = VolumeMetadataMaintainer.createOrOpen(tr, subspace);

            assertEquals(originalId, opened.id());
            assertEquals(VolumeStatus.READWRITE, opened.status());
            assertEquals(List.of(0L), opened.segmentIds());
        }
    }

    @Test
    void shouldReadVolumeId() {
        // Behavior: readVolumeId returns the same ID that createOrOpen generated.
        long expectedId;
        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeMetadata metadata = VolumeMetadataMaintainer.createOrOpen(tr, subspace);
            expectedId = metadata.id();
            tr.commit().join();
        }

        try (Transaction tr = context.getTransactionManager().create()) {
            long actualId = VolumeMetadataMaintainer.readVolumeId(tr, subspace);
            assertEquals(expectedId, actualId);
        }
    }

    @Test
    void shouldLoadSegmentIds() {
        // Behavior: loadSegmentIds returns all registered segments in ascending order.
        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeMetadataMaintainer.createOrOpen(tr, subspace);

            tr.set(subspace.packVolumeSegmentIdKey(5L), VolumeMetadataMaintainer.NULL_BYTES);
            tr.set(subspace.packVolumeSegmentIdKey(3L), VolumeMetadataMaintainer.NULL_BYTES);
            tr.commit().join();
        }

        try (Transaction tr = context.getTransactionManager().create()) {
            List<Long> segmentIds = VolumeMetadataMaintainer.loadSegmentIds(tr, subspace);
            assertEquals(List.of(0L, 3L, 5L), segmentIds);
        }
    }

    @Test
    void shouldReturnEmptyListWhenNoSegmentsExist() {
        // Behavior: loadSegmentIds returns an empty list when no segment keys have been written.
        try (Transaction tr = context.getTransactionManager().create()) {
            List<Long> segmentIds = VolumeMetadataMaintainer.loadSegmentIds(tr, subspace);
            assertTrue(segmentIds.isEmpty());
        }
    }

    @Test
    void shouldReadVolumeStatus() {
        // Behavior: readVolumeStatus returns the status that createOrOpen stored.
        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeMetadataMaintainer.createOrOpen(tr, subspace);
            tr.commit().join();
        }

        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeStatus status = VolumeMetadataMaintainer.readVolumeStatus(tr, subspace);
            assertEquals(VolumeStatus.READWRITE, status);
        }
    }

    @Test
    void shouldReadUpdatedVolumeStatus() {
        // Behavior: readVolumeStatus returns READONLY after that status is written directly to the status key.
        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeMetadataMaintainer.createOrOpen(tr, subspace);
            tr.commit().join();
        }

        try (Transaction tr = context.getTransactionManager().create()) {
            byte[] statusValue = VolumeStatus.READONLY.toString().getBytes(StandardCharsets.US_ASCII);
            tr.set(subspace.packVolumeStatusKey(), statusValue);
            tr.commit().join();
        }

        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeStatus status = VolumeMetadataMaintainer.readVolumeStatus(tr, subspace);
            assertEquals(VolumeStatus.READONLY, status);
        }
    }

    @Test
    void shouldThrowWhenNoStatusExists() {
        // Behavior: readVolumeStatus throws IllegalStateException on an empty subspace.
        try (Transaction tr = context.getTransactionManager().create()) {
            assertThrows(IllegalStateException.class, () -> VolumeMetadataMaintainer.readVolumeStatus(tr, subspace));
        }
    }
}
