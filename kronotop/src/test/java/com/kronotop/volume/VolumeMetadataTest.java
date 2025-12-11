/*
 * Copyright (c) 2023-2025 Burak Sezer
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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VolumeMetadataTest extends BaseStandaloneInstanceTest {

    @Test
    void shouldThrowWhenAddingDuplicateSegment() {
        VolumeMetadata volumeMetadata = new VolumeMetadata();
        volumeMetadata.addSegment(1);

        assertThrows(IllegalArgumentException.class, () -> volumeMetadata.addSegment(1));
    }

    @Test
    void shouldThrowWhenRemovingNonExistentSegment() {
        VolumeMetadata volumeMetadata = new VolumeMetadata();
        volumeMetadata.addSegment(1);
        volumeMetadata.removeSegment(1);

        assertThrows(IllegalArgumentException.class, () -> volumeMetadata.removeSegment(1));
    }

    @Test
    void shouldReturnReadWriteAsDefaultStatus() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("volume-metadata-test");
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadata volumeMetadata = VolumeMetadata.load(tr, subspace);
            assertEquals(VolumeStatus.READWRITE, volumeMetadata.getStatus());
        }
    }

    @Test
    void shouldPersistStatusAfterCompute() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("volume-metadata-test");
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadata.compute(tr, subspace, volumeMetadata -> {
                volumeMetadata.setStatus(VolumeStatus.READONLY);
            });
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadata volumeMetadata = VolumeMetadata.load(tr, subspace);
            assertEquals(VolumeStatus.READONLY, volumeMetadata.getStatus());
        }
    }
}