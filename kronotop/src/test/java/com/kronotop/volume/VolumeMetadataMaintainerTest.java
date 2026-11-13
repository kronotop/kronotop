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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VolumeMetadataMaintainerTest extends BaseStandaloneInstanceTest {
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
            DirectorySubspace subspace = VolumeMetadataMaintainer.createVolumeSubspace(context, tr, UNUSED_SHARD_ID);

            assertEquals(expectedPath, subspace.getPath());
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
}
