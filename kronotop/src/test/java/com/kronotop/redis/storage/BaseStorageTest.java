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

package com.kronotop.redis.storage;

import com.kronotop.BaseTest;
import com.kronotop.Context;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.commandbuilder.kronotop.KrAdminCommandBuilder;
import com.kronotop.redis.RedisService;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.Prefix;
import com.typesafe.config.Config;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class BaseStorageTest extends BaseTest {
    protected KronotopTestInstance kronotopInstance;
    protected RedisService redisService;
    protected RoutingService routingService;
    protected Context context;
    protected EmbeddedChannel channel;
    protected Prefix redisVolumeSyncerPrefix;

    protected String makeKey(int number) {
        return String.format("key-%d", number);
    }

    @BeforeEach
    public void setup() throws UnknownHostException, InterruptedException {
        Config config = loadConfig("test.conf");
        kronotopInstance = new KronotopTestInstance(config);
        kronotopInstance.start();
        context = kronotopInstance.getContext();
        redisService = kronotopInstance.getContext().getService(RedisService.NAME);
        routingService = kronotopInstance.getContext().getService(RoutingService.NAME);
        channel = kronotopInstance.getChannel();
        redisVolumeSyncerPrefix = new Prefix(context.getConfig().getString("redis.volume_syncer.prefix").getBytes());
    }

    @AfterEach
    public void tearDown() {
        if (kronotopInstance == null) {
            return;
        }
        kronotopInstance.shutdown();
    }

    protected boolean areAllShardsReadOnly() {
        int shards = context.getConfig().getInt("redis.shards");
        for (int shardId = 0; shardId < shards; shardId++) {
            Route route = routingService.findRoute(ShardKind.REDIS, shardId);
            if (!route.shardStatus().equals(ShardStatus.READONLY)) {
                return false;
            }
        }
        return true;
    }

    protected void setShardStatus(ShardKind shardKind, ShardStatus shardStatus) {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus(shardKind.name(), shardStatus.name()).encode(buf);
        channel.writeInbound(buf);
        Object msg = channel.readOutbound();

        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    protected void makeAllShardsReadOnly() {
        setShardStatus(ShardKind.REDIS, ShardStatus.READONLY);
        await().atMost(5, TimeUnit.SECONDS).until(this::areAllShardsReadOnly);
    }
}
