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

package com.kronotop.redis;

import com.apple.foundationdb.Database;
import com.kronotop.ConfigTestUtil;
import com.kronotop.core.Context;
import com.kronotop.core.ContextImpl;
import com.kronotop.core.FoundationDBFactory;
import com.kronotop.core.KronotopService;
import com.kronotop.core.cluster.Member;
import com.kronotop.core.cluster.MembershipService;
import com.kronotop.core.cluster.MockProcessIdGeneratorImpl;
import com.kronotop.core.cluster.coordinator.CoordinatorService;
import com.kronotop.core.network.Address;
import com.kronotop.core.watcher.Watcher;
import com.kronotop.server.resp.Handlers;
import com.kronotop.server.resp.Router;
import com.typesafe.config.Config;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.redis.RedisArrayAggregator;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class BaseHandlerTest {
    protected Handlers handlers;
    protected EmbeddedChannel channel;
    protected RedisService redisService;
    protected Context context;
    protected Database database;

    protected static EmbeddedChannel newChannel(Context context, Handlers commands) {
        return new EmbeddedChannel(new RedisDecoder(false), new RedisBulkStringAggregator(), new RedisArrayAggregator(), new Router(context, commands));
    }

    protected void setupCommon(Config config) throws UnknownHostException, InterruptedException {
        MockProcessIdGeneratorImpl processIdGenerator = new MockProcessIdGeneratorImpl();
        Address address = new Address("localhost", 0);
        Member member = new Member(address, processIdGenerator.getProcessID());
        database = FoundationDBFactory.newDatabase(config);
        context = new ContextImpl(config, member, database);

        CoordinatorService coordinatorService = new CoordinatorService(context);
        coordinatorService.start();
        context.registerService(CoordinatorService.NAME, coordinatorService);

        MembershipService membershipService = new MembershipService(context);
        membershipService.start();
        membershipService.waitUntilBootstrapped();
        context.registerService(MembershipService.NAME, membershipService);

        context.registerService(Watcher.NAME, new Watcher());
        handlers = new Handlers();
        redisService = new RedisService(context, handlers);
        context.registerService(RedisService.NAME, redisService);
        redisService.start();

        channel = newChannel(context, handlers);
    }

    @BeforeEach
    public void setup() throws UnknownHostException, InterruptedException {
        Config config = ConfigTestUtil.load("test.conf");
        setupCommon(config);
    }

    public EmbeddedChannel newChannel() {
        return newChannel(context, handlers);
    }

    @AfterEach
    public void teardown() {
        try {
            for (KronotopService service : context.getServices()) {
                service.shutdown();
            }
            handlers = new Handlers();
            assertFalse(channel.finish());
        } finally {
            database.close();
        }
    }

}
