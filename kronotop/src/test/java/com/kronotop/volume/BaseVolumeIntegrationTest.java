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

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.cluster.sharding.ShardKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

public class BaseVolumeIntegrationTest extends BaseVolumeTest {
    protected Volume volume;
    protected VolumeService service;
    protected DirectorySubspace subspace;
    protected Prefix redisVolumeSyncerPrefix;

    void setupVolumeTestEnv() throws IOException {
        VolumeConfigGenerator generator = new VolumeConfigGenerator(context, ShardKind.REDIS, 1);
        VolumeConfig volumeConfig = generator.volumeConfig();
        service = context.getService(VolumeService.NAME);
        volume = service.newVolume(volumeConfig);
        redisVolumeSyncerPrefix = new Prefix(context.getConfig().getString("redis.volume_syncer.prefix").getBytes());
    }

    @BeforeEach
    public void setupIntegrationTest() {
        VolumeConfigGenerator generator = new VolumeConfigGenerator(context, ShardKind.REDIS, 1);
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