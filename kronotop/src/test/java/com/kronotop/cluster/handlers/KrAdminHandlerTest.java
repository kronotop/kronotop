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

package com.kronotop.cluster.handlers;

import com.kronotop.KronotopTestInstance;
import com.kronotop.VersionstampUtils;
import com.kronotop.cluster.MemberStatus;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.commandbuilder.kronotop.KrAdminCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import com.kronotop.volume.replication.BaseNetworkedVolumeTest;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class KrAdminHandlerTest extends BaseNetworkedVolumeTest {

    @Test
    public void test_initializeCluster_already_initialized() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.initializeCluster().encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR cluster has already been initialized", actualMessage.content());
    }

    @Test
    public void test_listMembers() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.listMembers().encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
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
                    assertEquals(VersionstampUtils.base64Decode(value.content()), context.getMember().getProcessId());
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
    public void test_findMember() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.findMember(kronotopInstance.getMember().getId()).encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(MapRedisMessage.class, msg);

        ((MapRedisMessage) msg).children().forEach((keyMessage, valueMessage) -> {
            SimpleStringRedisMessage key = (SimpleStringRedisMessage) keyMessage;

            if (key.content().equals("status")) {
                SimpleStringRedisMessage value = (SimpleStringRedisMessage) valueMessage;
                assertEquals(MemberStatus.valueOf(value.content()), context.getMember().getStatus());
            }

            if (key.content().equals("process_id")) {
                SimpleStringRedisMessage value = (SimpleStringRedisMessage) valueMessage;
                assertEquals(VersionstampUtils.base64Decode(value.content()), context.getMember().getProcessId());
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

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    public void test_setMemberStatus_invalid_status() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setMemberStatus(kronotopInstance.getMember().getId(), "some-status").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Invalid member status some-status", actualMessage.content());
    }

    @Test
    public void test_setMemberStatus_member_not_found() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setMemberStatus("ccd59ec6-41e4-4f31-80ab-941c19238a6a", "RUNNING").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Member: ccd59ec6-41e4-4f31-80ab-941c19238a6a not registered", actualMessage.content());
    }

    @Test
    public void test_removeMember() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        KronotopTestInstance secondInstance = addNewInstance();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.setMemberStatus(secondInstance.getMember().getId(), "STOPPED").encode(buf);

            channel.writeInbound(buf);
            channel.readOutbound(); // consume the response
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.removeMember(secondInstance.getMember().getId()).encode(buf);
            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
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

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
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
        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Member in RUNNING status cannot be removed", actualMessage.content());
    }

    @Test
    public void test_setShardStatus() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus("redis", 1, "READONLY").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    public void test_setShardStatus_all_shards() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus("redis", "READONLY").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    public void test_setShardStatus_when_negative_shardId() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus("redis", -1, "READONLY").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR invalid shard id", actualMessage.content());
    }

    @Test
    public void test_setShardStatus_when_bigger_than_number_of_shards() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus("redis", 1231253, "READONLY").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR invalid shard id", actualMessage.content());
    }

    @Test
    public void test_describeShard() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.describeShard("redis", 1).encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
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
            }
        });
    }
}
