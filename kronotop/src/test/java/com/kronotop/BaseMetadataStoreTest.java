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

package com.kronotop;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.cluster.Member;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.volume.VolumeService;
import com.typesafe.config.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.UnknownHostException;
import java.util.List;

public class BaseMetadataStoreTest extends BaseTest {
    protected Database database;
    protected Config config;
    protected Context context;

    protected DirectorySubspace getClusterSubspace(String subspaceName) {
        try (Transaction tr = database.createTransaction()) {
            String clusterName = config.getString("cluster.name");
            List<String> subpath = KronotopDirectory.kronotop().cluster(clusterName).extend(subspaceName);
            DirectorySubspace subspace = DirectoryLayer.getDefault().createOrOpen(tr, subpath).join();
            tr.commit().join();
            return subspace;
        }
    }

    @BeforeEach
    public void setup() throws UnknownHostException {
        Member member = createMemberWithEphemeralPort();
        config = loadConfig("test.conf");
        database = FoundationDBFactory.newDatabase(config);
        context = new ContextImpl(config, member, database);
        context.registerService(VolumeService.NAME, new VolumeService(context));
    }

    @AfterEach
    public void tearDown() {
        for (KronotopService service : context.getServices()) {
            service.shutdown();
        }
        try (Transaction tr = database.createTransaction()) {
            String clusterName = config.getString("cluster.name");
            List<String> subpath = KronotopDirectory.kronotop().cluster(clusterName).toList();
            DirectoryLayer.getDefault().removeIfExists(tr, subpath).join();
            tr.commit().join();
        }
    }
}
