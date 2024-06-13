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
import com.kronotop.common.utils.DirectoryLayout;
import com.typesafe.config.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.UnknownHostException;
import java.util.List;

public class BaseMetadataStoreTest extends BaseTest {
    protected Database database;
    protected Config config;
    protected Context context;

    protected DirectorySubspace getClusterSubspace(String name) {
        try (Transaction tr = database.createTransaction()) {
            String clusterName = config.getString("cluster.name");
            List<String> subpath = DirectoryLayout.Builder.clusterName(clusterName).add(name).asList();
            DirectorySubspace subspace = DirectoryLayer.getDefault().createOrOpen(tr, subpath).join();
            tr.commit().join();
            return subspace;
        }
    }

    @BeforeEach
    public void setup() throws UnknownHostException {
        String address = String.format("localhost:[%s]", getEphemeralTCPPort());
        Member member = createMember(address);
        config = loadConfig("test.conf");
        database = FoundationDBFactory.newDatabase(config);
        context = new ContextImpl(config, member, database);
    }

    @AfterEach
    public void tearDown() {
        try (Transaction tr = database.createTransaction()) {
            String clusterName = config.getString("cluster.name");
            List<String> subpath = DirectoryLayout.Builder.clusterName(clusterName).asList();
            DirectoryLayer.getDefault().removeIfExists(tr, subpath).join();
            tr.commit().join();
        }
    }
}
