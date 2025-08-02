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

package com.kronotop.cluster.handlers;

import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.MemberIdGenerator;
import com.kronotop.cluster.MemberStatus;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.commandbuilder.kronotop.KrAdminCommandBuilder;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import com.kronotop.volume.replication.BaseNetworkedVolumeIntegrationTest;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class KrAdminHandlerTest extends BaseNetworkedVolumeIntegrationTest {

    @Test
    public void test_initializeCluster_already_initialized() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.initializeCluster().encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR cluster has already been initialized", actualMessage.content());
    }

    @Test
    public void test_listMembers() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.listMembers().encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;
        assertEquals(1, mapRedisMessage.children().size()); // only one member

        mapRedisMessage.children().forEach((memberIdMessage, memberPropertiesMessage) -> {
            assertInstanceOf(SimpleStringRedisMessage.class, memberIdMessage);
            SimpleStringRedisMessage memberId = (SimpleStringRedisMessage) memberIdMessage;
            assertEquals(context.getMember().getId(), memberId.content());

            MapRedisMessage m = (MapRedisMessage) memberPropertiesMessage;
            m.children().forEach((keyMessage, valueMessage) -> {
                SimpleStringRedisMessage key = (SimpleStringRedisMessage) keyMessage;

                if (key.content().equals("status")) {
                    SimpleStringRedisMessage value = (SimpleStringRedisMessage) valueMessage;
                    assertEquals(MemberStatus.valueOf(value.content()), context.getMember().getStatus());
                }

                if (key.content().equals("process_id")) {
                    SimpleStringRedisMessage value = (SimpleStringRedisMessage) valueMessage;
                    assertEquals(VersionstampUtil.base32HexDecode(value.content()), context.getMember().getProcessId());
                }

                if (key.content().equals("external_host")) {
                    SimpleStringRedisMessage value = (SimpleStringRedisMessage) valueMessage;
                    assertEquals(value.content(), context.getMember().getExternalAddress().getHost());
                }

                if (key.content().equals("external_port")) {
                    IntegerRedisMessage value = (IntegerRedisMessage) valueMessage;
                    assertEquals(value.value(), context.getMember().getExternalAddress().getPort());
                }

                if (key.content().equals("internal_host")) {
                    SimpleStringRedisMessage value = (SimpleStringRedisMessage) valueMessage;
                    assertEquals(value.content(), context.getMember().getInternalAddress().getHost());
                }

                if (key.content().equals("internal_port")) {
                    IntegerRedisMessage value = (IntegerRedisMessage) valueMessage;
                    assertEquals(value.value(), context.getMember().getInternalAddress().getPort());
                }

                if (key.content().equals("latest_heartbeat")) {
                    // Check the existence.
                    IntegerRedisMessage value = (IntegerRedisMessage) valueMessage;
                    assertTrue(value.value() >= 0);
                }
            });
        });
    }

    @Test
    public void test_describeMember() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.describeMember().encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        ((MapRedisMessage) msg).children().forEach((keyMessage, valueMessage) -> {
            SimpleStringRedisMessage key = (SimpleStringRedisMessage) keyMessage;

            if (key.content().equals("member_id")) {
                SimpleStringRedisMessage value = (SimpleStringRedisMessage) valueMessage;
                assertEquals(value.content(), context.getMember().getId());
            }

            if (key.content().equals("status")) {
                SimpleStringRedisMessage value = (SimpleStringRedisMessage) valueMessage;
                assertEquals(MemberStatus.valueOf(value.content()), context.getMember().getStatus());
            }

            if (key.content().equals("process_id")) {
                SimpleStringRedisMessage value = (SimpleStringRedisMessage) valueMessage;
                assertEquals(VersionstampUtil.base32HexDecode(value.content()), context.getMember().getProcessId());
            }

            if (key.content().equals("external_host")) {
                SimpleStringRedisMessage value = (SimpleStringRedisMessage) valueMessage;
                assertEquals(value.content(), context.getMember().getExternalAddress().getHost());
            }

            if (key.content().equals("external_port")) {
                IntegerRedisMessage value = (IntegerRedisMessage) valueMessage;
                assertEquals(value.value(), context.getMember().getExternalAddress().getPort());
            }

            if (key.content().equals("internal_host")) {
                SimpleStringRedisMessage value = (SimpleStringRedisMessage) valueMessage;
                assertEquals(value.content(), context.getMember().getInternalAddress().getHost());
            }

            if (key.content().equals("internal_port")) {
                IntegerRedisMessage value = (IntegerRedisMessage) valueMessage;
                assertEquals(value.value(), context.getMember().getInternalAddress().getPort());
            }

            if (key.content().equals("latest_heartbeat")) {
                // Check the existence.
                IntegerRedisMessage value = (IntegerRedisMessage) valueMessage;
                assertTrue(value.value() >= 0);
            }
        });
    }

    @Test
    void test_findMember() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.findMember(kronotopInstance.getMember().getId()).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        ((MapRedisMessage) msg).children().forEach((keyMessage, valueMessage) -> {
            SimpleStringRedisMessage key = (SimpleStringRedisMessage) keyMessage;

            if (key.content().equals("status")) {
                SimpleStringRedisMessage value = (SimpleStringRedisMessage) valueMessage;
                assertEquals(MemberStatus.valueOf(value.content()), context.getMember().getStatus());
            }

            if (key.content().equals("process_id")) {
                SimpleStringRedisMessage value = (SimpleStringRedisMessage) valueMessage;
                assertEquals(VersionstampUtil.base32HexDecode(value.content()), context.getMember().getProcessId());
            }

            if (key.content().equals("external_host")) {
                SimpleStringRedisMessage value = (SimpleStringRedisMessage) valueMessage;
                assertEquals(value.content(), context.getMember().getExternalAddress().getHost());
            }

            if (key.content().equals("external_port")) {
                IntegerRedisMessage value = (IntegerRedisMessage) valueMessage;
                assertEquals(value.value(), context.getMember().getExternalAddress().getPort());
            }

            if (key.content().equals("internal_host")) {
                SimpleStringRedisMessage value = (SimpleStringRedisMessage) valueMessage;
                assertEquals(value.content(), context.getMember().getInternalAddress().getHost());
            }

            if (key.content().equals("internal_port")) {
                IntegerRedisMessage value = (IntegerRedisMessage) valueMessage;
                assertEquals(value.value(), context.getMember().getInternalAddress().getPort());
            }

            if (key.content().equals("latest_heartbeat")) {
                // Check the existence.
                IntegerRedisMessage value = (IntegerRedisMessage) valueMessage;
                assertTrue(value.value() >= 0);
            }
        });
    }

    @Test
    public void test_setMemberStatus() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setMemberStatus(kronotopInstance.getMember().getId(), "STOPPED").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    public void test_setMemberStatus_invalid_status() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setMemberStatus(kronotopInstance.getMember().getId(), "some-status").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Invalid member status some-status", actualMessage.content());
    }

    @Test
    public void test_setMemberStatus_member_not_found() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        String memberId = MemberIdGenerator.generateId();
        cmd.setMemberStatus(memberId, "RUNNING").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals(String.format("ERR Member: %s not registered", memberId), actualMessage.content());
    }

    @Test
    public void test_removeMember() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        KronotopTestInstance secondInstance = addNewInstance();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.setMemberStatus(secondInstance.getMember().getId(), "STOPPED").encode(buf);

            runCommand(channel, buf); // consume the response
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.removeMember(secondInstance.getMember().getId()).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }
    }

    @Test
    public void test_listSilentMembers() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.listSilentMembers().encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(0, actualMessage.children().size());
    }

    @Test
    public void test_removeMember_RUNNING_status() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        KronotopTestInstance secondInstance = addNewInstance();

        ByteBuf buf = Unpooled.buffer();
        cmd.removeMember(secondInstance.getMember().getId()).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Member in RUNNING status cannot be removed", actualMessage.content());
    }

    @Test
    public void test_setShardStatus() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus("redis", 1, "READONLY").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    public void test_setShardStatus_all_shards() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus("redis", "READONLY").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    public void test_setShardStatus_when_negative_shardId() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus("redis", -1, "READONLY").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR invalid shard id", actualMessage.content());
    }

    @Test
    public void test_setShardStatus_when_bigger_than_number_of_shards() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus("redis", 1231253, "READONLY").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR invalid shard id", actualMessage.content());
    }

    @Test
    public void test_describeShard() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.describeShard("REDIS", 1).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        actualMessage.children().forEach((messageKey, messageValue) -> {
            SimpleStringRedisMessage key = (SimpleStringRedisMessage) messageKey;
            switch (key.content()) {
                case "primary" -> {
                    SimpleStringRedisMessage value = (SimpleStringRedisMessage) messageValue;
                    assertFalse(value.content().isEmpty());
                }
                case "standbys", "sync_standbys" -> {
                    ArrayRedisMessage value = (ArrayRedisMessage) messageValue;
                    assertEquals(0, value.children().size());
                }
                case "status" -> {
                    SimpleStringRedisMessage value = (SimpleStringRedisMessage) messageValue;
                    assertEquals(ShardStatus.READWRITE.name(), value.content());
                }
                case "linked_volumes" -> {
                    ArrayRedisMessage value = (ArrayRedisMessage) messageValue;
                    assertEquals(1, value.children().size());
                    SimpleStringRedisMessage item = (SimpleStringRedisMessage) value.children().getFirst();
                    assertEquals(volume.getConfig().name(), item.content());
                }
            }
        });
    }

    private void checkSyncStandbys(KrAdminCommandBuilder<String, String> cmd, String standbyMemberId, int shardId) {
        ByteBuf buf = Unpooled.buffer();
        cmd.describeShard("redis", 1).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        actualMessage.children().forEach((messageKey, messageValue) -> {
            SimpleStringRedisMessage key = (SimpleStringRedisMessage) messageKey;
            switch (key.content()) {
                case "standbys", "sync_standbys" -> {
                    ArrayRedisMessage value = (ArrayRedisMessage) messageValue;
                    assertEquals(1, value.children().size());
                    RedisMessage first = value.children().getFirst();
                    SimpleStringRedisMessage memberId = (SimpleStringRedisMessage) first;
                    assertEquals(standbyMemberId, memberId.content());
                }
            }
        });
    }

    @Test
    public void test_set_syncStandby() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        // Start a new instance and set a standby id
        KronotopTestInstance secondInstance = addNewInstance();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.route("set", "standby", "redis", 1, secondInstance.getMember().getId()).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        // set a sync standby
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.syncStandby("set", "redis", 1, secondInstance.getMember().getId()).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        // control it
        checkSyncStandbys(cmd, secondInstance.getMember().getId(), 1);
    }

    @Test
    public void test_unset_syncStandby() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        // Start a new instance and set a standby id
        KronotopTestInstance secondInstance = addNewInstance();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.route("set", "standby", "redis", 1, secondInstance.getMember().getId()).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        // set a sync standby
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.syncStandby("set", "redis", 1, secondInstance.getMember().getId()).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        // unset a sync standby
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.syncStandby("unset", "redis", 1, secondInstance.getMember().getId()).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        // control it
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.describeShard("redis", 1).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage actualMessage = (MapRedisMessage) msg;
            actualMessage.children().forEach((messageKey, messageValue) -> {
                SimpleStringRedisMessage key = (SimpleStringRedisMessage) messageKey;
                if (key.content().equals("sync_standbys")) {
                    ArrayRedisMessage value = (ArrayRedisMessage) messageValue;
                    assertEquals(0, value.children().size());
                }
            });
        }
    }

    @Test
    public void try_to_set_syncStandby_when_member_not_registered() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        String memberId = MemberIdGenerator.generateId();
        ByteBuf buf = Unpooled.buffer();
        cmd.syncStandby("set", "redis", 1, memberId).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals(String.format("ERR Member: %s not registered", memberId), actualMessage.content());
    }

    @Test
    public void test_unset_syncStandby_member_not_standby() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        // Start a new instance and set a standby id
        KronotopTestInstance secondInstance = addNewInstance();

        // set a sync standby
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.syncStandby("set", "redis", 1, secondInstance.getMember().getId()).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
            assertEquals("ERR member is not a standby", actualMessage.content());
        }
    }

    @Test
    public void test_set_syncStandby_all_shards_with_asterisk() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        // Start a new instance and set a standby id
        KronotopTestInstance secondInstance = addNewInstance();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.route("set", "standby", "redis", secondInstance.getMember().getId()).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        // set a sync standby
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.syncStandby("set", "redis", secondInstance.getMember().getId()).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        // control it
        int shards = kronotopInstance.getContext().getConfig().getInt("redis.shards");
        for (int shardId = 0; shardId < shards; shardId++) {
            checkSyncStandbys(cmd, secondInstance.getMember().getId(), shardId);
        }
    }
}

