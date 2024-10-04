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

package com.kronotop.journal;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.kronotop.BaseTest;
import com.kronotop.FoundationDBFactory;
import com.kronotop.MissingConfigException;
import com.kronotop.common.utils.DirectoryLayout;
import com.typesafe.config.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;

public class BaseJournalTest extends BaseTest {
    protected String testJournal = "test-journal";
    protected Config config;
    protected Database database;
    protected String clusterName;

    @BeforeEach
    public void setup() {
        config = loadConfig("test.conf");
        database = FoundationDBFactory.newDatabase(config);
        if (!config.hasPath("cluster.name")) {
            throw new MissingConfigException("cluster.name is missing in configuration");
        }
        this.clusterName = config.getString("cluster.name");
    }

    @AfterEach
    public void tearDown() {
        database.run(tr -> {
            DirectoryLayer directoryLayer = DirectoryLayer.getDefault();
            List<String> subpath = DirectoryLayout.Builder.clusterName(this.clusterName).asList();
            return directoryLayer.removeIfExists(tr, subpath).join();
        });
    }
}
