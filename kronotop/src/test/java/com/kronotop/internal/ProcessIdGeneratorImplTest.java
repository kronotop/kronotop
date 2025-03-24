/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.internal;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseTest;
import com.kronotop.directory.KronotopDirectory;
import com.typesafe.config.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProcessIdGeneratorImplTest extends BaseTest {
    protected Database database;
    protected Config config;

    @BeforeEach
    public void setup() {
        config = loadConfig("test.conf");
        database = FoundationDBFactory.newDatabase(config);
    }

    @AfterEach
    public void tearDown() {
        try (Transaction tr = database.createTransaction()) {
            String clusterName = config.getString("cluster.name");
            DirectoryLayer directoryLayer = DirectoryLayer.getDefault();
            List<String> subpath = KronotopDirectory.kronotop().cluster(clusterName).toList();
            directoryLayer.remove(tr, subpath).join();
            tr.commit().join();
        }
    }

    @Test
    public void test_getProcessId() {
        ProcessIdGeneratorImpl processIDGenerator = new ProcessIdGeneratorImpl(config, database);
        Versionstamp first = processIDGenerator.getProcessID();
        Versionstamp second = processIDGenerator.getProcessID();
        assertTrue(first.compareTo(second) < 0);
    }
}