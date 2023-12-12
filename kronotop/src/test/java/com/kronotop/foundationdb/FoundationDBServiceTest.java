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

package com.kronotop.foundationdb;

import com.apple.foundationdb.Database;
import com.kronotop.ConfigTestUtil;
import com.kronotop.core.Context;
import com.kronotop.core.ContextImpl;
import com.kronotop.core.FoundationDBFactory;
import com.kronotop.core.cluster.Member;
import com.kronotop.core.cluster.MockProcessIdGeneratorImpl;
import com.kronotop.core.network.Address;
import com.kronotop.server.resp.Handlers;
import com.typesafe.config.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

public class FoundationDBServiceTest {
    private FoundationDBService service;
    private Handlers handlers;

    @BeforeEach
    public void setup() throws UnknownHostException {
        MockProcessIdGeneratorImpl processIdGenerator = new MockProcessIdGeneratorImpl();
        Config config = ConfigTestUtil.load("test.conf");
        Address address = new Address("localhost", 0);
        Member member = new Member(address, processIdGenerator.getProcessID());
        Database database = FoundationDBFactory.newDatabase(config);
        Context context = new ContextImpl(config, member, database);
        handlers = new Handlers();
        service = new FoundationDBService(context, handlers);
    }

    @AfterEach
    public void teardown() {
        service.shutdown();
    }

    @Test
    public void testDatabaseConnection() {
        Database db = service.getContext().getFoundationDB();
        db.run(tr -> {
            tr.set("key".getBytes(), "value".getBytes());
            tr.clear("key".getBytes());
            return null;
        });
    }
}
