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
import com.kronotop.volume.BaseNetworkedVolumeIntegrationTest;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KrAdminHandlerTest extends BaseNetworkedVolumeIntegrationTest {

    @Test
    void shouldReturnErrorWhenClusterAlreadyInitialized() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.initializeCluster().encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR cluster has already been initialized", actualMessage.content());
    }

    @Test
    void shouldListMembers() {
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
    void shouldDescribeMember() {
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
    void shouldFindMember() {
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
    void shouldSetMemberStatus() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setMemberStatus(kronotopInstance.getMember().getId(), "STOPPED").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenSetMemberStatusWithInvalidStatus() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setMemberStatus(kronotopInstance.getMember().getId(), "some-status").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Invalid member status some-status", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenSetMemberStatusWithMemberNotFound() {
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
    void shouldRemoveMember() {
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
    void shouldListSilentMembers() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.listSilentMembers().encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(0, actualMessage.children().size());
    }

    @Test
    void shouldReturnErrorWhenRemoveMemberWithRunningStatus() {
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
    void shouldSetShardStatus() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus("redis", 1, "READONLY").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    void shouldSetShardStatusForAllShards() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus("redis", "READONLY").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenSetShardStatusWithNegativeShardId() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus("redis", -1, "READONLY").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR invalid shard id", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenSetShardStatusWithShardIdBiggerThanNumberOfShards() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus("redis", 1231253, "READONLY").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR invalid shard id", actualMessage.content());
    }

    @Test
    void shouldDescribeShard() {
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
                case "standbys" -> {
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

}

