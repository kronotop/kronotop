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
import com.kronotop.BaseMetadataStoreTest;
import com.kronotop.common.utils.DirectoryLayout;
import com.typesafe.config.Config;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

public class BaseVolumeTest extends BaseMetadataStoreTest {
    protected Prefix prefix = new Prefix("test-prefix".getBytes());

    public VolumeConfig getVolumeConfig(Config config, DirectorySubspace subspace) {
        String dataDir = config.getString("data_dir");
        return new VolumeConfig(subspace,
                VolumeConfiguration.name,
                dataDir,
                VolumeConfiguration.segmentSize,
                VolumeConfiguration.allowedGarbageRatio
        );
    }

    public ByteBuffer[] getEntries(int number) {
        int capacity = 10;
        ByteBuffer[] entries = new ByteBuffer[number];
        for (int i = 0; i < number; i++) {
            byte[] data = Strings.padStart(Integer.toString(i), capacity, '0').getBytes();
            entries[i] = ByteBuffer.allocate(capacity).put(data).flip();
        }
        return entries;
    }

    public DirectorySubspace getSubspace(Database database, Config config) {
        try (Transaction tr = database.createTransaction()) {
            String clusterName = config.getString("cluster.name");
            List<String> subpath = DirectoryLayout.Builder.clusterName(clusterName).add("volumes-test").add(UUID.randomUUID().toString()).asList();
            DirectorySubspace subspace = DirectoryLayer.getDefault().createOrOpen(tr, subpath).join();
            tr.commit().join();
            return subspace;
        }
    }
}