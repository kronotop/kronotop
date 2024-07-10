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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.common.base.Strings;
import com.kronotop.Context;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.BaseClusterTest;
import com.kronotop.common.utils.DirectoryLayout;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

public class BaseVolumeTest extends BaseClusterTest {
    protected Database database;
    protected Volume volume;
    protected VolumeConfig volumeConfig;
    protected KronotopTestInstance coordinator;
    protected EmbeddedChannel channel;
    protected Context context;
    protected VolumeService service;
    protected DirectorySubspace subspace;

    protected ByteBuffer[] getEntries(int number) {
        int capacity = 10;
        ByteBuffer[] entries = new ByteBuffer[number];
        for (int i = 0; i < number; i++) {
            byte[] data = Strings.padStart(Integer.toString(i), capacity, '0').getBytes();
            entries[i] = ByteBuffer.allocate(capacity).put(data).flip();
        }
        return entries;
    }

    protected DirectorySubspace getSubspace() {
        try (Transaction tr = coordinator.getContext().getFoundationDB().createTransaction()) {
            String clusterName = coordinator.getContext().getConfig().getString("cluster.name");
            List<String> subpath = DirectoryLayout.Builder.clusterName(clusterName).add("volumes-test").add(UUID.randomUUID().toString()).asList();
            DirectorySubspace subspace = DirectoryLayer.getDefault().createOrOpen(tr, subpath).join();
            tr.commit().join();
            return subspace;
        }
    }

    void setupVolumeTestEnv() throws IOException {
        String name = context.getConfig().getString("volume_test.volume.name");
        String rootPath = context.getConfig().getString("volume_test.volume.root_path");
        Long segmentSize = context.getConfig().getLong("volume_test.volume.segment_size");
        Float allowedGarbageRatio = (float) context.getConfig().getDouble("volume_test.volume.allowed_garbage_ratio");
        volumeConfig = new VolumeConfig(subspace, name, rootPath, segmentSize, allowedGarbageRatio);
        volume = service.newVolume(volumeConfig);

        // Set an owner for this new Volume instance
        try (Transaction tr = coordinator.getContext().getFoundationDB().createTransaction()) {
            VolumeMetadata.compute(tr, subspace, (volumeMetadata -> {
                Host host = new Host(Role.OWNER, context.getMember());
                volumeMetadata.setOwner(host);
            }));
            tr.commit().join();
        }
    }

    @BeforeEach
    public void setupBaseVolumeTest() {
        coordinator = getClusterCoordinator();
        channel = coordinator.getChannel();
        database = coordinator.getContext().getFoundationDB();
        context = coordinator.getContext();
        service = coordinator.getContext().getService(VolumeService.NAME);
        subspace = getSubspace();
        try {
            setupVolumeTestEnv();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    public void tearDownBaseVolumeTest() {
        volume.close();
    }
}