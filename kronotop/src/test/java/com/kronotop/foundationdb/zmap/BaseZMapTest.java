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

package com.kronotop.foundationdb.zmap;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.kronotop.ConfigTestUtil;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.Context;
import com.kronotop.core.ContextImpl;
import com.kronotop.core.FoundationDBFactory;
import com.kronotop.core.cluster.Member;
import com.kronotop.core.cluster.MockProcessIdGeneratorImpl;
import com.kronotop.core.network.Address;
import com.kronotop.core.watcher.Watcher;
import com.kronotop.foundationdb.FoundationDBService;
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
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class BaseZMapTest {
    protected String namespace;
    protected Handlers handlers;
    protected EmbeddedChannel channel;
    protected FoundationDBService foundationDBService;
    protected ZMapService zMapService;
    protected Context context;

    private static EmbeddedChannel newChannel(Context context, Handlers commands) {
        return new EmbeddedChannel(new RedisDecoder(false), new RedisBulkStringAggregator(), new RedisArrayAggregator(), new Router(context, commands));
    }

    public EmbeddedChannel getChannel() {
        return channel;
    }

    @BeforeEach
    public void setup() throws UnknownHostException {
        MockProcessIdGeneratorImpl processIdGenerator = new MockProcessIdGeneratorImpl();
        Config config = ConfigTestUtil.load("test.conf");
        Address address = new Address("localhost", 0);
        Member member = new Member(address, processIdGenerator.getProcessID());
        Database database = FoundationDBFactory.newDatabase(config);
        context = new ContextImpl(config, member, database);
        context.registerService(Watcher.NAME, new Watcher());
        handlers = new Handlers();
        channel = newChannel(context, handlers);
        foundationDBService = new FoundationDBService(context, handlers);
        context.registerService(FoundationDBService.NAME, foundationDBService);

        zMapService = new ZMapService(context, handlers);
        context.registerService(ZMapService.NAME, zMapService);
        namespace = UUID.randomUUID().toString();
    }

    private void cleanupEphemeralCluster() {
        context.getFoundationDB().run(tr -> {
            DirectoryLayer directoryLayer = DirectoryLayer.getDefault();
            List<String> subpath = DirectoryLayout.Builder.clusterName(context.getClusterName()).asList();
            return directoryLayer.remove(tr, subpath).join();
        });
    }

    @AfterEach
    public void teardown() {
        cleanupEphemeralCluster();
        foundationDBService.shutdown();
        zMapService.shutdown();
        handlers = new Handlers();
        assertFalse(channel.finish());
    }
}