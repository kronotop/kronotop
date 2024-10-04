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

package com.kronotop.foundationdb;

import com.apple.foundationdb.Database;
import com.kronotop.BaseTest;
import com.kronotop.Context;
import com.kronotop.ContextImpl;
import com.kronotop.FoundationDBFactory;
import com.kronotop.cluster.Member;
import com.typesafe.config.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

public class FoundationDBServiceTest extends BaseTest {
    private FoundationDBService service;

    @BeforeEach
    public void setup() throws UnknownHostException {
        Config config = loadConfig("test.conf");
        Member member = createMemberWithEphemeralPort();
        Database database = FoundationDBFactory.newDatabase(config);
        Context context = new ContextImpl(config, member, database);
        service = new FoundationDBService(context);
    }

    @AfterEach
    public void tearDown() {
        service.shutdown();
    }

    @Test
    public void test_initializeDefaultDatabase() {
        String name = service.getContext().getConfig().getString("default_namespace");
        // TODO: Add an integration test
    }
}
