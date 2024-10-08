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
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.BaseClusterTestWithTCPServer;
import com.kronotop.volume.*;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

public class BaseNetworkedVolumeTest extends BaseClusterTestWithTCPServer {
    protected final BaseVolumeTestWrapper baseVolumeTestWrapper = new BaseVolumeTestWrapper();

    protected Context context;
    protected Database database;
    protected KronotopTestInstance kronotopInstance;
    protected Volume volume;
    protected VolumeConfig volumeConfig;
    protected Prefix prefix = new Prefix("test-prefix".getBytes());

    @BeforeEach
    public void setup() {
        super.setup();
        kronotopInstance = getClusterCoordinator();
        context = kronotopInstance.getContext();
        database = kronotopInstance.getContext().getFoundationDB();
        VolumeService volumeService = kronotopInstance.getContext().getService(VolumeService.NAME);

        DirectorySubspace subspace = baseVolumeTestWrapper.getSubspace(database, kronotopInstance.getContext().getConfig());
        volumeConfig = baseVolumeTestWrapper.getVolumeConfig(kronotopInstance.getContext().getConfig(), subspace);
        try {
            volume = volumeService.newVolume(volumeConfig);
            // Set an owner for this new Volume instance
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeMetadata.compute(tr, subspace, (volumeMetadata -> {
                    Host host = new Host(Role.OWNER, context.getMember());
                    volumeMetadata.setOwner(host);
                }));
                tr.commit().join();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected static class BaseVolumeTestWrapper extends BaseVolumeTest {
    }
}
