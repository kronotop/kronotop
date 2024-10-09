/*
 * Copyright (c) 2023-2024 Kronotop
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
import com.kronotop.volume.replication.Host;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

public class BaseVolumeIntegrationTest extends BaseVolumeTest {
    protected Volume volume;
    protected VolumeService service;
    protected DirectorySubspace subspace;
    protected Prefix redisVolumeSyncerPrefix;

    void setupVolumeTestEnv() throws IOException {
        VolumeConfig volumeConfig = getVolumeConfig(config, subspace);
        service = context.getService(VolumeService.NAME);
        volume = service.newVolume(volumeConfig);
        redisVolumeSyncerPrefix = new Prefix(context.getConfig().getString("redis.volume_syncer.prefix").getBytes());

        // Set an owner for this new Volume instance
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadata.compute(tr, subspace, (volumeMetadata -> {
                Host host = new Host(Role.OWNER, context.getMember());
                volumeMetadata.setOwner(host);
            }));
            tr.commit().join();
        }
    }

    @BeforeEach
    public void setupIntegrationTest() {
        subspace = getSubspace(database, config);

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