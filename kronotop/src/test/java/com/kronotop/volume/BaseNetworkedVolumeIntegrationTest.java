/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume;

import com.apple.foundationdb.Database;
import com.google.common.base.Strings;
import com.kronotop.BaseClusterTestWithTCPServer;
import com.kronotop.Context;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.sharding.ShardKind;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

public class BaseNetworkedVolumeIntegrationTest extends BaseClusterTestWithTCPServer {
    protected final BaseVolumeTestWrapper baseVolumeTestWrapper = new BaseVolumeTestWrapper();

    protected final int SHARD_ID = 1;
    protected final ShardKind SHARD_KIND = ShardKind.REDIS;
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
        volumeConfigGenerator = new VolumeConfigGenerator(kronotopInstance.getContext(), SHARD_KIND, SHARD_ID);
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

    @Deprecated
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

    protected ByteBuffer[] getEntries(int number, int length) {
        ByteBuffer[] entries = new ByteBuffer[number];
        for (int i = 0; i < number; i++) {
            byte[] data = Strings.padStart(Integer.toString(i), length, '0').getBytes();
            entries[i] = ByteBuffer.allocate(length).put(data).flip();
        }
        return entries;
    }

    public byte[] sha1(String filePath) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            try (InputStream in = new DigestInputStream(new FileInputStream(filePath), digest)) {
                byte[] buf = new byte[4096];
                while (in.read(buf) != -1) { /* no-op */ }
            }
            return digest.digest();
        } catch (IOException | NoSuchAlgorithmException exp) {
            throw new RuntimeException(exp);
        }
    }

    protected static class BaseVolumeTestWrapper extends BaseVolumeTest {
    }
}
