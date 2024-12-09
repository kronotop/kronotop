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

package com.kronotop.volume.replication;

import com.apple.foundationdb.Database;
import com.kronotop.BaseClusterTestWithTCPServer;
import com.kronotop.Context;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.volume.*;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.UUID;

public class BaseNetworkedVolumeIntegrationTest extends BaseClusterTestWithTCPServer {
    protected final BaseVolumeTestWrapper baseVolumeTestWrapper = new BaseVolumeTestWrapper();

    protected Context context;
    protected Database database;
    protected KronotopTestInstance kronotopInstance;
    protected EmbeddedChannel channel;
    protected Volume volume;
    protected VolumeConfig volumeConfig;
    protected Prefix prefix = new Prefix("volume-test-prefix".getBytes());
    protected VolumeService volumeService;
    private VolumeConfigGenerator volumeConfigGenerator;

    @BeforeEach
    public void setup() {
        super.setup();
        kronotopInstance = getInstances().getFirst();
        volumeConfigGenerator = new VolumeConfigGenerator(kronotopInstance.getContext(), ShardKind.REDIS, 1);
        volumeService = kronotopInstance.getContext().getService(VolumeService.NAME);
        channel = kronotopInstance.getChannel();
        context = kronotopInstance.getContext();
        database = kronotopInstance.getContext().getFoundationDB();

        try {
            volumeConfig = volumeConfigGenerator.volumeConfig();
            volume = volumeService.newVolume(volumeConfig);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected Volume newStandbyVolume(Context standbyContext) {
        String dataDir = Paths.get(volumeConfigGenerator.getDataDir(), UUID.randomUUID().toString()).toString();
        VolumeConfig standbyVolumeConfig = volumeConfigGenerator.volumeConfig(dataDir);
        try {
            VolumeService standbyVolumeService = standbyContext.getService(VolumeService.NAME);
            return standbyVolumeService.newVolume(standbyVolumeConfig);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected static class BaseVolumeTestWrapper extends BaseVolumeTest {
    }
}
