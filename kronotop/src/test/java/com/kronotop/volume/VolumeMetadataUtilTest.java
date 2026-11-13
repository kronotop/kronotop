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
import com.kronotop.directory.KronotopDirectoryNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VolumeMetadataUtilTest extends BaseStandaloneInstanceTest {
    private static final String VOLUME_NAME = "volume-metadata-util-test";

    private VolumeSubspace subspace;

    @BeforeEach
    void setupSubspace() {
        DirectorySubspace ds = createOrOpenSubspaceUnderCluster(VOLUME_NAME);
        subspace = new VolumeSubspace(ds);
    }

    private KronotopDirectoryNode volumeDirectory() {
        return KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                volumes().
                volume(VOLUME_NAME);
    }

    @Test
    void shouldCreateVolumeSubspace() {
        // Behavior: When the given volume directory does not exist, createVolumeSubspace creates it in the
        // given transaction and returns a subspace whose path is that directory.
        KronotopDirectoryNode directory = volumeDirectory();

        try (Transaction tr = context.getTransactionManager().create()) {
            DirectorySubspace volumeSubspace = VolumeMetadataUtil.createVolumeSubspace(context, tr, VOLUME_NAME, directory);

            assertEquals(directory.toList(), volumeSubspace.getPath());
        }
    }

    @Test
    void shouldThrowWhenVolumeSubspaceAlreadyExists() {
        // Behavior: When createVolumeSubspace has already created the given volume directory in the same
        // transaction, a second call with the same name and directory throws VolumeAlreadyExistsException
        // with the message "Volume already exists: '<volume name>'".
        KronotopDirectoryNode directory = volumeDirectory();

        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeMetadataUtil.createVolumeSubspace(context, tr, VOLUME_NAME, directory);

            VolumeAlreadyExistsException exception = assertThrows(
                    VolumeAlreadyExistsException.class,
                    () -> VolumeMetadataUtil.createVolumeSubspace(context, tr, VOLUME_NAME, directory)
            );
            assertEquals(
                    String.format("Volume already exists: '%s'", VOLUME_NAME),
                    exception.getMessage()
            );
        }
    }

    @Test
    void shouldCreateNewVolumeMetadata() {
        // Behavior: createOrOpen on an empty subspace initializes a new volume with READWRITE status and segment 0.
        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeMetadata metadata = VolumeMetadataUtil.createOrOpen(tr, subspace);
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
            VolumeMetadata created = VolumeMetadataUtil.createOrOpen(tr, subspace);
            originalId = created.id();
            tr.commit().join();
        }

        try (Transaction tr = context.getTransactionManager().create()) {
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
        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeMetadata metadata = VolumeMetadataUtil.createOrOpen(tr, subspace);
            expectedId = metadata.id();
            tr.commit().join();
        }

        try (Transaction tr = context.getTransactionManager().create()) {
            long actualId = VolumeMetadataUtil.readVolumeId(tr, subspace);
            assertEquals(expectedId, actualId);
        }
    }

    @Test
    void shouldLoadSegmentIds() {
        // Behavior: loadSegmentIds returns all registered segments in ascending order.
        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeMetadataUtil.createOrOpen(tr, subspace);

            tr.set(subspace.packVolumeSegmentIdKey(5L), VolumeMetadataUtil.NULL_BYTES);
            tr.set(subspace.packVolumeSegmentIdKey(3L), VolumeMetadataUtil.NULL_BYTES);
            tr.commit().join();
        }

        try (Transaction tr = context.getTransactionManager().create()) {
            List<Long> segmentIds = VolumeMetadataUtil.loadSegmentIds(tr, subspace);
            assertEquals(List.of(0L, 3L, 5L), segmentIds);
        }
    }

    @Test
    void shouldReturnEmptyListWhenNoSegmentsExist() {
        // Behavior: loadSegmentIds returns an empty list when no segment keys have been written.
        try (Transaction tr = context.getTransactionManager().create()) {
            List<Long> segmentIds = VolumeMetadataUtil.loadSegmentIds(tr, subspace);
            assertTrue(segmentIds.isEmpty());
        }
    }

    @Test
    void shouldReadVolumeStatus() {
        // Behavior: readVolumeStatus returns the status that createOrOpen stored.
        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeMetadataUtil.createOrOpen(tr, subspace);
            tr.commit().join();
        }

        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeStatus status = VolumeMetadataUtil.readVolumeStatus(tr, subspace);
            assertEquals(VolumeStatus.READWRITE, status);
        }
    }

    @Test
    void shouldReadUpdatedVolumeStatus() {
        // Behavior: readVolumeStatus returns READONLY after that status is written directly to the status key.
        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeMetadataUtil.createOrOpen(tr, subspace);
            tr.commit().join();
        }

        try (Transaction tr = context.getTransactionManager().create()) {
            byte[] statusValue = VolumeStatus.READONLY.toString().getBytes(StandardCharsets.US_ASCII);
            tr.set(subspace.packVolumeStatusKey(), statusValue);
            tr.commit().join();
        }

        try (Transaction tr = context.getTransactionManager().create()) {
            VolumeStatus status = VolumeMetadataUtil.readVolumeStatus(tr, subspace);
            assertEquals(VolumeStatus.READONLY, status);
        }
    }

    @Test
    void shouldThrowWhenNoStatusExists() {
        // Behavior: readVolumeStatus throws IllegalStateException on an empty subspace.
        try (Transaction tr = context.getTransactionManager().create()) {
            assertThrows(IllegalStateException.class, () -> VolumeMetadataUtil.readVolumeStatus(tr, subspace));
        }
    }
}
