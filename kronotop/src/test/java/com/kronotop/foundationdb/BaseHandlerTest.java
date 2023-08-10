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
import com.apple.foundationdb.directory.DirectoryLayer;
import com.kronotop.ConfigTestUtil;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.Context;
import com.kronotop.core.ContextImpl;
import com.kronotop.core.FoundationDBFactory;
import com.kronotop.core.cluster.Member;
import com.kronotop.core.network.Address;
import com.kronotop.core.watcher.Watcher;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.resp.Handlers;
import com.kronotop.server.resp.Router;
import com.typesafe.config.Config;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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

public class BaseHandlerTest {
    protected String namespace;
    protected Handlers handlers;
    protected EmbeddedChannel channel;
    protected FoundationDBService service;
    protected Context context;

    private static EmbeddedChannel newChannel(Context context, Handlers commands) {
        return new EmbeddedChannel(new RedisDecoder(false), new RedisBulkStringAggregator(), new RedisArrayAggregator(), new Router(context, commands));
    }

    public EmbeddedChannel getChannel() {
        return channel;
    }

    @BeforeEach
    public void setup() throws UnknownHostException {
        Config config = ConfigTestUtil.load("test.conf");
        Address address = new Address("localhost", 0);
        Member member = new Member(address, System.currentTimeMillis());
        Database database = FoundationDBFactory.newDatabase(config);
        context = new ContextImpl(config, member, database);
        context.registerService(Watcher.NAME, new Watcher());
        namespace = UUID.randomUUID().toString();
        handlers = new Handlers();
        channel = newChannel(context, handlers);
        service = new FoundationDBService(context, handlers);
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
        service.shutdown();
        handlers = new Handlers();
        assertFalse(channel.finish());
    }

    public class TestTransaction {
        private Object response;

        void begin() {
            KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);

            channel.writeInbound(buf);
            response = channel.readOutbound();
        }

        void cancel() {
            KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
            ByteBuf buf = Unpooled.buffer();
            cmd.rollback().encode(buf);

            channel.writeInbound(buf);
            response = channel.readOutbound();
        }

        void commit() {
            KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
            ByteBuf buf = Unpooled.buffer();
            cmd.commit().encode(buf);

            channel.writeInbound(buf);
            response = channel.readOutbound();
        }

        Object getResponse() {
            return response;
        }
    }
}