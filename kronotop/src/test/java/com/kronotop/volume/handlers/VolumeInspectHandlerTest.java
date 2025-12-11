/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.commandbuilder.kronotop.KrAdminCommandBuilder;
import com.kronotop.commandbuilder.kronotop.VolumeInspectCommandBuilder;
import com.kronotop.instance.KronotopInstance;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.BaseNetworkedVolumeIntegrationTest;
import com.kronotop.volume.VolumeSession;
import com.kronotop.volume.replication.ReplicationService;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.EnumMap;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class VolumeInspectHandlerTest extends BaseNetworkedVolumeIntegrationTest {

    private void appendEntries(int number, int length) throws IOException {
        ByteBuffer[] entries = getEntries(number, length);
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }
        assertEquals(number, result.getVersionstampedKeys().length);
    }

    private void setStandbyMember(KronotopInstance standby) {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.route("SET", "STANDBY", SHARD_KIND.name(), SHARD_ID, standby.getMember().getId()).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());

        RoutingService routing = standby.getContext().getService(RoutingService.NAME);
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            Route route = routing.findRoute(SHARD_KIND, SHARD_ID);
            if (route == null) {
                return false;
            }
            return route.standbys().contains(standby.getMember());
        });
    }

    @Test
    void shouldReturnCursorForVolumeWithData() throws IOException {
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(10);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        VolumeInspectCommandBuilder<String, String> cmd = new VolumeInspectCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.cursor("redis-shard-1").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;

        for (var entry : actualMessage.children().entrySet()) {
            String key = ((SimpleStringRedisMessage) entry.getKey()).content();
            RedisMessage value = entry.getValue();
            switch (key) {
                case "active_segment_id" -> {
                    IntegerRedisMessage v = (IntegerRedisMessage) value;
                    assertEquals(0L, v.value());
                }
                case "versionstamp" -> {
                    SimpleStringRedisMessage v = (SimpleStringRedisMessage) value;
                    assertFalse(v.content().isEmpty());
                }
                case "next_position" -> {
                    IntegerRedisMessage v = (IntegerRedisMessage) value;
                    assertTrue(v.value() > 0);
                }
                case "sequence_number" -> {
                    IntegerRedisMessage v = (IntegerRedisMessage) value;
                    assertTrue(v.value() >= 0);
                }
            }
        }
    }

    @Test
    void shouldReturnCursorForEmptyVolume() {
        VolumeInspectCommandBuilder<String, String> cmd = new VolumeInspectCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.cursor("redis-shard-1").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;

        for (var entry : actualMessage.children().entrySet()) {
            String key = ((SimpleStringRedisMessage) entry.getKey()).content();
            RedisMessage value = entry.getValue();
            switch (key) {
                case "active_segment_id", "next_position" -> {
                    IntegerRedisMessage v = (IntegerRedisMessage) value;
                    assertEquals(0L, v.value());
                }
                case "versionstamp" -> {
                    SimpleStringRedisMessage v = (SimpleStringRedisMessage) value;
                    assertTrue(v.content().isEmpty());
                }
                case "sequence_number" -> {
                    IntegerRedisMessage v = (IntegerRedisMessage) value;
                    assertEquals(-1L, v.value());
                }
            }
        }
    }

    @Test
    void shouldReturnReplicationStatusForActiveReplication() throws IOException {
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);
        appendEntries(number, length);

        KronotopInstance standby = addNewInstance();
        setStandbyMember(standby);

        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);

        // Wait for replication to start
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            EnumMap<ShardKind, List<Integer>> activeReplications = replicationService.listActiveReplicationsByShard();
            List<Integer> shardIds = activeReplications.get(SHARD_KIND);
            return shardIds != null && shardIds.contains(SHARD_ID);
        });

        VolumeInspectCommandBuilder<String, String> cmd = new VolumeInspectCommandBuilder<>(StringCodec.ASCII);

        // Wait until stage is populated (replication state written to FDB)
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            ByteBuf b = Unpooled.buffer();
            cmd.replication(SHARD_KIND.name(), SHARD_ID, standby.getMember().getId()).encode(b);
            Object m = runCommand(channel, b);
            if (m instanceof MapRedisMessage mapMsg) {
                for (var entry : mapMsg.children().entrySet()) {
                    String key = ((SimpleStringRedisMessage) entry.getKey()).content();
                    if ("stage".equals(key)) {
                        SimpleStringRedisMessage v = (SimpleStringRedisMessage) entry.getValue();
                        return !v.content().isEmpty();
                    }
                }
            }
            return false;
        });

        // Query replication status
        ByteBuf buf = Unpooled.buffer();
        cmd.replication(SHARD_KIND.name(), SHARD_ID, standby.getMember().getId()).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;

        for (var entry : actualMessage.children().entrySet()) {
            String key = ((SimpleStringRedisMessage) entry.getKey()).content();
            RedisMessage value = entry.getValue();
            switch (key) {
                case "stage", "status" -> {
                    SimpleStringRedisMessage v = (SimpleStringRedisMessage) value;
                    assertFalse(v.content().isEmpty());
                }
                case "cursor", "segment_replication_stage", "cdc_stage" -> {
                    assertInstanceOf(MapRedisMessage.class, value);
                    MapRedisMessage cursorMap = (MapRedisMessage) value;
                    assertFalse(cursorMap.children().isEmpty());
                }
            }
        }
    }
}
