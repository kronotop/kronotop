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

package com.kronotop.e2e;

import com.kronotop.BaseTest;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.RouteKind;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.commandbuilder.kronotop.KrAdminCommandBuilder;
import com.kronotop.commandbuilder.kronotop.VolumeAdminCommandBuilder;
import com.kronotop.commandbuilder.redis.RedisCommandBuilder;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.VolumeStatus;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class E2ETestUtils {

    static boolean areAllKeysSyncedToVolume(KronotopTestInstance instance) {
        RedisService service = instance.getContext().getService(RedisService.NAME);
        for (RedisShard shard : service.getServiceContext().shards().values()) {
            if (!shard.volumeSyncQueue().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    static void insertKeys(EmbeddedChannel channel, int numberOfKeys) {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        for (int i = 0; i < numberOfKeys; i++) {
            ByteBuf buf = Unpooled.buffer();
            cmd.set("key-" + i, "value-" + i).encode(buf);

            Object msg = BaseTest.runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }
    }

    static boolean checkReplicationSlots(KronotopTestInstance instance) {
        EmbeddedChannel channel = instance.getChannel();
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.replications().encode(buf);

        Object msg = BaseTest.runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;

        // One replication slot per Redis shard
        int shards = instance.getContext().getConfig().getInt("redis.shards");
        return actualMessage.children().size() == shards;
    }

    static void setShardStatus(EmbeddedChannel channel, ShardKind shardKind, ShardStatus status, int shardId) {
        KrAdminCommandBuilder<String, String> krAdmin = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        krAdmin.setShardStatus(shardKind.name(), shardId, status.name()).encode(buf);

        Object msg = BaseTest.runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    static List<String> listOpenVolumes(EmbeddedChannel channel) {
        VolumeAdminCommandBuilder<String, String> volumeAdmin = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        volumeAdmin.list().encode(buf);

        Object msg = BaseTest.runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        List<String> volumeNames = new ArrayList<>();
        for (RedisMessage child : actualMessage.children()) {
            SimpleStringRedisMessage name = (SimpleStringRedisMessage) child;
            volumeNames.add(name.content());
        }
        return volumeNames;
    }

    static void setVolumeStatus(EmbeddedChannel channel, VolumeStatus status, List<String> volumeNames) {
        VolumeAdminCommandBuilder<String, String> volumeAdmin = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        for (String volumeName : volumeNames) {
            ByteBuf buf = Unpooled.buffer();
            volumeAdmin.setStatus(volumeName, status.name()).encode(buf);

            Object msg = BaseTest.runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }
    }

    static void setRoute(EmbeddedChannel channel, String operationKind, RouteKind routeKind, ShardKind shardKind, int shardId, KronotopTestInstance instance) {
        KrAdminCommandBuilder<String, String> krAdmin = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        krAdmin.route(operationKind, routeKind.name(), shardKind.name(), shardId, instance.getMember().getId()).encode(buf);

        Object msg = BaseTest.runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    static boolean areReplicationSlotsUpToDate(KronotopTestInstance instance) {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.replications().encode(buf);

        EmbeddedChannel channel = instance.getChannel();
        Object msg = BaseTest.runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;

        class Bundle {
            String latest;
            String received;
        }

        HashMap<String, Bundle> result = new HashMap<>();
        for (RedisMessage key : actualMessage.children().keySet()) {
            SimpleStringRedisMessage slotId = (SimpleStringRedisMessage) key;
            MapRedisMessage slot = (MapRedisMessage) actualMessage.children().get(slotId);

            Bundle bundle = new Bundle();
            for (RedisMessage slotKey : slot.children().keySet()) {
                RedisMessage slotValue = slot.children().get(slotKey);
                if (((SimpleStringRedisMessage) slotKey).content().equals("received_versionstamped_key")) {
                    bundle.received = ((SimpleStringRedisMessage) slotValue).content();
                }
                if (((SimpleStringRedisMessage) slotKey).content().equals("latest_versionstamped_key")) {
                    bundle.latest = ((SimpleStringRedisMessage) slotValue).content();
                }
            }
            result.put(slotId.content(), bundle);
        }

        for (Map.Entry<String, Bundle> entry : result.entrySet()) {
            if (!entry.getValue().latest.equals(entry.getValue().received)) {
                return false;
            }
        }
        return true;
    }
}
