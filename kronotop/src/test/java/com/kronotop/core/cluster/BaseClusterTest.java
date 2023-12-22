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

package com.kronotop.core.cluster;

import com.apple.foundationdb.Database;
import com.kronotop.ConfigTestUtil;
import com.kronotop.common.KronotopException;
import com.kronotop.core.*;
import com.kronotop.core.network.Address;
import com.typesafe.config.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;

public class BaseClusterTest {
    private final ConcurrentHashMap<String, KronotopService> services = new ConcurrentHashMap<>();
    protected Config config;
    protected Database database;
    protected ProcessIdGenerator processIDGenerator = new MockProcessIdGeneratorImpl();

    @BeforeEach
    public void setup() {
        config = ConfigTestUtil.load("test.conf");
        database = FoundationDBFactory.newDatabase(config);
    }

    @AfterEach
    public void tearDown() {
        for (KronotopService service : services.values()) {
            service.shutdown();
        }
        // TODO:
        //FoundationDBFactory.closeDatabase();
    }

    protected void registerService(KronotopService service) {
        services.put(service.getName(), service);
    }

    protected Context newContext() {
        try {
            Address address = new Address("localhost", 0);
            Member member = new Member(address, processIDGenerator.getProcessID());
            return new ContextImpl(config, member, database);
        } catch (UnknownHostException e) {
            throw new KronotopException(e);
        }
    }
}
