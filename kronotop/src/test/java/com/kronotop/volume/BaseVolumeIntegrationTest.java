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
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

public class BaseVolumeIntegrationTest extends BaseVolumeTest {
    protected Volume volume;
    protected VolumeService service;
    protected DirectorySubspace subspace;
    protected Prefix stashVolumeSyncerPrefix;

    void setupVolumeTestEnv() throws IOException {
        // Creating the global prefixes subspace is required for volume integration tests.
        KronotopDirectoryNode prefixes = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                prefixes();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            context.getDirectoryLayer().createOrOpen(tr, prefixes.toList()).join();
            tr.commit().join();
        }

        VolumeConfigGenerator generator = new VolumeConfigGenerator(context, ShardKind.STASH, 1);
        VolumeConfig volumeConfig = generator.volumeConfig();
        service = context.getService(VolumeService.NAME);
        volume = service.newVolume(volumeConfig);
        stashVolumeSyncerPrefix = new Prefix(context.getConfig().getString("stash.volume_syncer.prefix").getBytes());
    }

    @BeforeEach
    public void setupIntegrationTest() {
        VolumeConfigGenerator generator = new VolumeConfigGenerator(context, ShardKind.STASH, 1);
        subspace = generator.createOrOpenVolumeSubspace();

        try {
            setupVolumeTestEnv();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    public void tearDownIntegrationTest() {
        if (volume != null) {
            volume.close();
        }
    }
}