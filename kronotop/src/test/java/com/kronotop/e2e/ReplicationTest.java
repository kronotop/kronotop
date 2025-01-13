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

import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RouteKind;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.commandbuilder.kronotop.KrAdminCommandBuilder;
import com.kronotop.commandbuilder.kronotop.VolumeAdminCommandBuilder;
import com.kronotop.server.resp3.*;
import com.kronotop.volume.VolumeStatus;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class ReplicationTest extends BaseE2ETest {

    @Test
    void insert_keys_then_check_replication() {
        // Create a two nodes test cluster.
        Cluster cluster = setupCluster();

        // Insert some keys
        E2ETestUtils.insertKeys(cluster.primary().getChannel(), 100);

        // Wait for syncing to the volumes
        await().atMost(Duration.ofSeconds(5)).until(() -> E2ETestUtils.areAllKeysSyncedToVolume(cluster.primary()));

        // Wait for replicating all data to the standby node
        await().atMost(Duration.ofSeconds(5)).until(() -> E2ETestUtils.areReplicationSlotsUpToDate(cluster.primary()));
    }

    @Test
    void insert_keys_then_check_replication_then_transfer_shard_ownership() {
        // Create a two nodes test cluster.
        Cluster cluster = setupCluster();

        EmbeddedChannel channel = cluster.primary().getChannel();
        // Insert some keys
        E2ETestUtils.insertKeys(channel, 100);

        // Wait for syncing to the volumes
        await().atMost(Duration.ofSeconds(5)).until(() -> E2ETestUtils.areAllKeysSyncedToVolume(cluster.primary()));

        // Wait for replicating all data to the standby node
        await().atMost(Duration.ofSeconds(5)).until(() -> E2ETestUtils.areReplicationSlotsUpToDate(cluster.primary()));

        int shards = cluster.primary().getContext().getConfig().getInt("redis.shards");

        // Mark all shards READONLY
        for (int shardId = 0; shardId < shards; shardId++) {
            E2ETestUtils.setShardStatus(channel, ShardKind.REDIS, ShardStatus.READONLY, shardId);
        }

        // Mark all volumes READONLY
        List<String> volumes = E2ETestUtils.listOpenVolumes(channel);
        E2ETestUtils.setVolumeStatus(channel, VolumeStatus.READONLY, volumes);

        KronotopTestInstance standby = cluster.standbys().getFirst();
        // Transfer all shard ownership to the standby
        for (int shardId = 0; shardId < shards; shardId++) {
            E2ETestUtils.setRoute(
                    channel,
                    "SET",
                    RouteKind.PRIMARY,
                    ShardKind.REDIS,
                    shardId,
                    standby
            );
        }

        KrAdminCommandBuilder<String, String> krAdmin = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        for (int shardId = 0; shardId < shards; shardId++) {

            ByteBuf buf = Unpooled.buffer();
            krAdmin.describeShard(ShardKind.REDIS.name(), shardId).encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage actualMessage = (MapRedisMessage) msg;
            for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
                SimpleStringRedisMessage key = (SimpleStringRedisMessage) entry.getKey();
                // Primary changed
                if (key.content().equals("primary")) {
                    String primaryMemberId = ((SimpleStringRedisMessage) entry.getValue()).content();
                    assertEquals(standby.getMember().getId(), primaryMemberId);
                }

                // No standby member anymore
                if (key.content().equals("standbys")) {
                    List<RedisMessage> standbys = ((ArrayRedisMessage) entry.getValue()).children();
                    assertEquals(0, standbys.size());
                }

                // No standby member anymore
                if (key.content().equals("sync_standbys")) {
                    List<RedisMessage> standbys = ((ArrayRedisMessage) entry.getValue()).children();
                    assertEquals(0, standbys.size());
                }
            }
        }

        // Control routing tables
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            List<KronotopTestInstance> members = new ArrayList<>(List.of(cluster.primary()));
            members.addAll(cluster.standbys());
            for (KronotopTestInstance member : members) {
                for (int shardId = 0; shardId < shards; shardId++) {
                    RoutingService routingService = member.getContext().getService(RoutingService.NAME);
                    Route route = routingService.findRoute(ShardKind.REDIS, shardId);
                    // Standby is the new primary
                    if (!route.primary().getId().equals(standby.getMember().getId())) {
                        return false;
                    }
                    if (!route.standbys().isEmpty()) {
                        return false;
                    }
                    if (!route.syncStandbys().isEmpty()) {
                        return false;
                    }
                }
            }
            return true;
        });

        // Check replication slots
        // All replication slots must be inactive and stale
        {
            VolumeAdminCommandBuilder<String, String> volumeAdmin = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
            ByteBuf buf = Unpooled.buffer();
            volumeAdmin.replications().encode(buf);
            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage actualMessage = (MapRedisMessage) msg;
            for (RedisMessage value : actualMessage.children().values()) {
                MapRedisMessage mapRedisMessage = (MapRedisMessage) value;
                for (Map.Entry<RedisMessage, RedisMessage> entry : mapRedisMessage.children().entrySet()) {
                    SimpleStringRedisMessage key = (SimpleStringRedisMessage) entry.getKey();
                    if (key.content().equals("active")) {
                        assertFalse(((BooleanRedisMessage) entry.getValue()).value());
                    }
                    if (key.content().equals("stale")) {
                        assertTrue(((BooleanRedisMessage) entry.getValue()).value());
                    }
                }
            }
        }
    }
}