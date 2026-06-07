/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.commands.KrAdminCommandBuilder;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import com.kronotop.volume.BaseNetworkedVolumeIntegrationTest;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

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
            assertInstanceOf(FullBulkStringRedisMessage.class, memberIdMessage);
            FullBulkStringRedisMessage memberId = (FullBulkStringRedisMessage) memberIdMessage;
            assertEquals(context.getMember().getId(), memberId.content().toString(StandardCharsets.UTF_8));

            MapRedisMessage m = (MapRedisMessage) memberPropertiesMessage;
            m.children().forEach((keyMessage, valueMessage) -> {
                FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) keyMessage;
                String keyStr = key.content().toString(StandardCharsets.UTF_8);

                if (keyStr.equals("status")) {
                    FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) valueMessage;
                    assertEquals(MemberStatus.valueOf(value.content().toString(StandardCharsets.UTF_8)), context.getMember().getStatus());
                }

                if (keyStr.equals("process_id")) {
                    FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) valueMessage;
                    assertEquals(VersionstampUtil.base32HexDecode(value.content().toString(StandardCharsets.UTF_8)), context.getMember().getProcessId());
                }

                if (keyStr.equals("external_host")) {
                    FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) valueMessage;
                    assertEquals(value.content().toString(StandardCharsets.UTF_8), context.getMember().getExternalAddress().getHost());
                }

                if (keyStr.equals("external_port")) {
                    IntegerRedisMessage value = (IntegerRedisMessage) valueMessage;
                    assertEquals(value.value(), context.getMember().getExternalAddress().getPort());
                }

                if (keyStr.equals("internal_host")) {
                    FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) valueMessage;
                    assertEquals(value.content().toString(StandardCharsets.UTF_8), context.getMember().getInternalAddress().getHost());
                }

                if (keyStr.equals("internal_port")) {
                    IntegerRedisMessage value = (IntegerRedisMessage) valueMessage;
                    assertEquals(value.value(), context.getMember().getInternalAddress().getPort());
                }

                if (keyStr.equals("latest_heartbeat")) {
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
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) keyMessage;
            String keyStr = key.content().toString(StandardCharsets.UTF_8);

            if (keyStr.equals("member_id")) {
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) valueMessage;
                assertEquals(value.content().toString(StandardCharsets.UTF_8), context.getMember().getId());
            }

            if (keyStr.equals("status")) {
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) valueMessage;
                assertEquals(MemberStatus.valueOf(value.content().toString(StandardCharsets.UTF_8)), context.getMember().getStatus());
            }

            if (keyStr.equals("process_id")) {
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) valueMessage;
                assertEquals(VersionstampUtil.base32HexDecode(value.content().toString(StandardCharsets.UTF_8)), context.getMember().getProcessId());
            }

            if (keyStr.equals("external_host")) {
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) valueMessage;
                assertEquals(value.content().toString(StandardCharsets.UTF_8), context.getMember().getExternalAddress().getHost());
            }

            if (keyStr.equals("external_port")) {
                IntegerRedisMessage value = (IntegerRedisMessage) valueMessage;
                assertEquals(value.value(), context.getMember().getExternalAddress().getPort());
            }

            if (keyStr.equals("internal_host")) {
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) valueMessage;
                assertEquals(value.content().toString(StandardCharsets.UTF_8), context.getMember().getInternalAddress().getHost());
            }

            if (keyStr.equals("internal_port")) {
                IntegerRedisMessage value = (IntegerRedisMessage) valueMessage;
                assertEquals(value.value(), context.getMember().getInternalAddress().getPort());
            }

            if (keyStr.equals("latest_heartbeat")) {
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
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) keyMessage;
            String keyStr = key.content().toString(StandardCharsets.UTF_8);

            if (keyStr.equals("status")) {
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) valueMessage;
                assertEquals(MemberStatus.valueOf(value.content().toString(StandardCharsets.UTF_8)), context.getMember().getStatus());
            }

            if (keyStr.equals("process_id")) {
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) valueMessage;
                assertEquals(VersionstampUtil.base32HexDecode(value.content().toString(StandardCharsets.UTF_8)), context.getMember().getProcessId());
            }

            if (keyStr.equals("external_host")) {
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) valueMessage;
                assertEquals(value.content().toString(StandardCharsets.UTF_8), context.getMember().getExternalAddress().getHost());
            }

            if (keyStr.equals("external_port")) {
                IntegerRedisMessage value = (IntegerRedisMessage) valueMessage;
                assertEquals(value.value(), context.getMember().getExternalAddress().getPort());
            }

            if (keyStr.equals("internal_host")) {
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) valueMessage;
                assertEquals(value.content().toString(StandardCharsets.UTF_8), context.getMember().getInternalAddress().getHost());
            }

            if (keyStr.equals("internal_port")) {
                IntegerRedisMessage value = (IntegerRedisMessage) valueMessage;
                assertEquals(value.value(), context.getMember().getInternalAddress().getPort());
            }

            if (keyStr.equals("latest_heartbeat")) {
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
    void shouldRemoveMemberAndVerifyMemberNotRegistered() {
        // Behavior: REMOVE-MEMBER persists member removal to FDB so the member is no longer findable
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        KronotopTestInstance secondInstance = addNewInstance();
        String memberId = secondInstance.getMember().getId();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.setMemberStatus(memberId, "STOPPED").encode(buf);
            runCommand(channel, buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.removeMember(memberId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.findMember(memberId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, msg);
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
        cmd.setShardStatus("stash", 1, "READONLY").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    void shouldSetShardStatusForAllShards() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus("stash", "READONLY").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenSetShardStatusWithNegativeShardId() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus("stash", -1, "READONLY").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR invalid shard id", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenSetShardStatusWithShardIdBiggerThanNumberOfShards() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus("stash", 1231253, "READONLY").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR invalid shard id", actualMessage.content());
    }

    @Test
    void shouldDescribeCluster() {
        // Behavior: DESCRIBE-CLUSTER returns metadata_version, cluster_name, and a section for each enabled shard kind.
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.describeCluster().encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage result = (MapRedisMessage) msg;

        Set<String> topLevelKeys = new HashSet<>();
        result.children().forEach((keyMessage, valueMessage) -> {
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) keyMessage;
            topLevelKeys.add(key.content().toString(StandardCharsets.UTF_8));
        });

        assertTrue(topLevelKeys.contains("metadata_version"));
        assertTrue(topLevelKeys.contains("cluster_name"));

        Set<ShardKind> enabledKinds = context.getShardRegistry().getShardKinds();
        for (ShardKind kind : enabledKinds) {
            assertTrue(topLevelKeys.contains(kind.toString().toLowerCase()));
        }

        assertEquals(2 + enabledKinds.size(), topLevelKeys.size());
    }

    @Test
    void shouldDescribeClusterShardStructure() {
        // Behavior: Each shard in DESCRIBE-CLUSTER contains primary, standbys, status, and linked_volumes.
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.describeCluster().encode(buf);

        Object msg = runCommand(channel, buf);
        MapRedisMessage result = (MapRedisMessage) msg;

        assertNotNull(result);
        result.children().forEach((keyMessage, valueMessage) -> {
            String key = ((FullBulkStringRedisMessage) keyMessage).content().toString(StandardCharsets.UTF_8);
            if (key.equals("metadata_version") || key.equals("cluster_name")) {
                return;
            }

            assertInstanceOf(MapRedisMessage.class, valueMessage);
            MapRedisMessage shardsByKind = (MapRedisMessage) valueMessage;
            assertFalse(shardsByKind.children().isEmpty());

            shardsByKind.children().forEach((shardIdMessage, shardDetailsMessage) -> {
                assertInstanceOf(IntegerRedisMessage.class, shardIdMessage);
                assertInstanceOf(MapRedisMessage.class, shardDetailsMessage);

                MapRedisMessage shardDetails = (MapRedisMessage) shardDetailsMessage;
                Set<String> shardKeys = new HashSet<>();
                shardDetails.children().forEach((sk, sv) ->
                        shardKeys.add(((FullBulkStringRedisMessage) sk).content().toString(StandardCharsets.UTF_8))
                );

                assertEquals(Set.of("primary", "standbys", "status", "linked_volumes"), shardKeys);
            });
        });
    }

    @Test
    void shouldDescribeShard() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.describeShard("STASH", 1).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        actualMessage.children().forEach((messageKey, messageValue) -> {
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) messageKey;
            switch (key.content().toString(StandardCharsets.UTF_8)) {
                case "primary" -> {
                    FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) messageValue;
                    assertFalse(value.content().toString(StandardCharsets.UTF_8).isEmpty());
                }
                case "standbys" -> {
                    ArrayRedisMessage value = (ArrayRedisMessage) messageValue;
                    assertEquals(0, value.children().size());
                }
                case "status" -> {
                    FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) messageValue;
                    assertEquals(ShardStatus.READWRITE.name(), value.content().toString(StandardCharsets.UTF_8));
                }
                case "linked_volumes" -> {
                    ArrayRedisMessage value = (ArrayRedisMessage) messageValue;
                    assertEquals(1, value.children().size());
                    FullBulkStringRedisMessage item = (FullBulkStringRedisMessage) value.children().getFirst();
                    assertEquals(volume.getConfig().name(), item.content().toString(StandardCharsets.UTF_8));
                }
            }
        });
    }

    @Test
    void shouldReturnTokenWhenDropClusterRequested() {
        // Behavior: Phase 1 returns a UUID token when called with a matching cluster name.
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.dropCluster(context.getClusterName()).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(FullBulkStringRedisMessage.class, msg);
        FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) msg;
        String token = actualMessage.content().toString(StandardCharsets.UTF_8);
        assertFalse(token.isEmpty());
        assertDoesNotThrow(() -> UUID.fromString(token));
    }

    @Test
    void shouldReturnErrorWhenClusterNameDoesNotMatch() {
        // Behavior: Phase 1 returns an error when the cluster name does not match.
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.dropCluster("wrong-cluster-name").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR cluster name does not match", actualMessage.content());
    }

    @Test
    void shouldDropClusterWithValidToken() {
        // Behavior: Phase 2 deletes the cluster directory from FDB when given a valid token.
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf1 = Unpooled.buffer();
        cmd.dropCluster(context.getClusterName()).encode(buf1);
        Object msg1 = runCommand(channel, buf1);
        assertInstanceOf(FullBulkStringRedisMessage.class, msg1);
        String token = ((FullBulkStringRedisMessage) msg1).content().toString(StandardCharsets.UTF_8);

        ByteBuf buf2 = Unpooled.buffer();
        cmd.dropCluster(context.getClusterName(), token).encode(buf2);
        Object msg2 = runCommand(channel, buf2);
        assertInstanceOf(SimpleStringRedisMessage.class, msg2);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) msg2).content());

        boolean exists = context.getFoundationDB().run(tr -> {
            List<String> subpath = KronotopDirectory.kronotop().cluster(context.getClusterName()).toList();
            return context.getDirectoryLayer().exists(tr, subpath).join();
        });
        assertFalse(exists);
    }

    @Test
    void shouldReturnErrorWhenTokenIsInvalid() {
        // Behavior: Phase 2 returns an error when the token does not match.
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf1 = Unpooled.buffer();
        cmd.dropCluster(context.getClusterName()).encode(buf1);
        runCommand(channel, buf1);

        ByteBuf buf2 = Unpooled.buffer();
        cmd.dropCluster(context.getClusterName(), UUID.randomUUID().toString()).encode(buf2);
        Object msg2 = runCommand(channel, buf2);
        assertInstanceOf(ErrorRedisMessage.class, msg2);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg2;
        assertEquals("ERR invalid drop-cluster token", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenTokenIsAlreadyConsumed() {
        // Behavior: A token cannot be used twice — the second attempt fails.
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf1 = Unpooled.buffer();
        cmd.dropCluster(context.getClusterName()).encode(buf1);
        Object msg1 = runCommand(channel, buf1);
        assertNotNull(msg1);
        String token = ((FullBulkStringRedisMessage) msg1).content().toString(StandardCharsets.UTF_8);

        ByteBuf buf2 = Unpooled.buffer();
        cmd.dropCluster(context.getClusterName(), token).encode(buf2);
        Object msg2 = runCommand(channel, buf2);
        assertInstanceOf(SimpleStringRedisMessage.class, msg2);

        ByteBuf buf3 = Unpooled.buffer();
        cmd.dropCluster(context.getClusterName(), token).encode(buf3);
        Object msg3 = runCommand(channel, buf3);
        assertInstanceOf(ErrorRedisMessage.class, msg3);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg3;
        assertEquals("ERR no pending drop-cluster token for this cluster", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenNoTokenHasBeenRequested() {
        // Behavior: Phase 2 returns an error when no token was requested.
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.dropCluster(context.getClusterName(), UUID.randomUUID().toString()).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR no pending drop-cluster token for this cluster", actualMessage.content());
    }

}

