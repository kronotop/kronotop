/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.core;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.kronotop.ConfigTestUtil;
import com.kronotop.common.utils.DirectoryLayout;
import com.typesafe.config.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProcessIDGeneratorImplTest {
    protected Database database;
    protected Config config;

    @BeforeEach
    public void setup() {
        config = ConfigTestUtil.load("test.conf");
        database = FoundationDBFactory.newDatabase(config);
    }

    @AfterEach
    public void teardown() {
        try (Transaction tr = database.createTransaction()) {
            String clusterName = config.getString("cluster.name");
            DirectoryLayer directoryLayer = DirectoryLayer.getDefault();
            List<String> subpath = DirectoryLayout.Builder.clusterName(clusterName).asList();
            directoryLayer.remove(tr, subpath).join();
            tr.commit().join();
        }
    }

    @Test
    public void testGetProcessID() {
        ProcessIDGenerator processIDGenerator = new ProcessIDGeneratorImpl(config, database);
        assertEquals(1, processIDGenerator.getProcessID());
    }

    @Test
    public void testGetProcessID_SequentialIncrease() {
        ProcessIDGenerator processIDGenerator = new ProcessIDGeneratorImpl(config, database);
        for (int i = 1; i < 10; i++) {
            assertEquals(i, processIDGenerator.getProcessID());
        }
    }
}
