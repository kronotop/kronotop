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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.CollectStalePrefixesTask;
import com.kronotop.cluster.MemberIdGenerator;
import com.kronotop.cluster.MemberStatus;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.commands.KrAdminCommandBuilder;
import com.kronotop.commands.TaskAdminCommandBuilder;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.internal.UUIDUtil;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.network.Address;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.ServerKind;
import com.kronotop.server.resp3.*;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.volume.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class KrAdminHandlerTest extends BaseNetworkedVolumeIntegrationTest {

    @BeforeEach
    public void setup() {
        super.setup();
        // The tests assume the negotiated protocol is RESP3.
        switchProtocol(channel, RESPVersion.RESP3);
    }

    @Test
    void shouldReturnErrorWhenClusterAlreadyInitialized() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.initializeCluster().encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Cluster has already been initialized", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenInitializeClusterWithExtraArguments() {
        // Behavior: INITIALIZE-CLUSTER SHARDS 1 followed by one more argument is rejected with
        // "Wrong number of arguments for 'KR.ADMIN INITIALIZE-CLUSTER' command".
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*5\r\n$8\r\nKR.ADMIN\r\n$18\r\nINITIALIZE-CLUSTER\r\n$6\r\nSHARDS\r\n$1\r\n1\r\n$5\r\nextra\r\n".getBytes());

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Wrong number of arguments for 'KR.ADMIN INITIALIZE-CLUSTER' command", actualMessage.content());
    }

    @Test
    void shouldAddShard() {
        // Behavior: ADD-SHARD on an initialized cluster replies with the integer 0.
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.addShard().encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(IntegerRedisMessage.class, msg);
        IntegerRedisMessage actualMessage = (IntegerRedisMessage) msg;
        assertEquals(0, actualMessage.value());
    }

    @Test
    void shouldListMembers() {
        // Behavior: LIST-MEMBERS returns every known member with its status, process id and advertise lists.
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

                if (keyStr.equals("external_advertise")) {
                    assertAdvertise(valueMessage, context.getMember().getExternalAdvertise());
                }

                if (keyStr.equals("internal_advertise")) {
                    assertAdvertise(valueMessage, context.getMember().getInternalAdvertise());
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
    void shouldReturnErrorWhenListMembersWithExtraArguments() {
        // Behavior: LIST-MEMBERS rejects extra arguments with "Wrong number of arguments for 'KR.ADMIN LIST-MEMBERS' command".
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*3\r\n$8\r\nKR.ADMIN\r\n$12\r\nLIST-MEMBERS\r\n$5\r\nextra\r\n".getBytes());

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Wrong number of arguments for 'KR.ADMIN LIST-MEMBERS' command", actualMessage.content());
    }

    @Test
    void shouldDescribeMember() {
        // Behavior: DESCRIBE-MEMBER returns the local member with its id, status, process id and advertise lists.
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

            if (keyStr.equals("external_advertise")) {
                assertAdvertise(valueMessage, context.getMember().getExternalAdvertise());
            }

            if (keyStr.equals("internal_advertise")) {
                assertAdvertise(valueMessage, context.getMember().getInternalAdvertise());
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
        // Behavior: FIND-MEMBER returns the requested member with its status, process id and advertise lists.
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

            if (keyStr.equals("external_advertise")) {
                assertAdvertise(valueMessage, context.getMember().getExternalAdvertise());
            }

            if (keyStr.equals("internal_advertise")) {
                assertAdvertise(valueMessage, context.getMember().getInternalAdvertise());
            }

            if (keyStr.equals("latest_heartbeat")) {
                // Check the existence.
                IntegerRedisMessage value = (IntegerRedisMessage) valueMessage;
                assertTrue(value.value() >= 0);
            }
        });
    }

    @Test
    void shouldReturnErrorWhenFindMemberWithoutMemberId() {
        // Behavior: FIND-MEMBER rejects a call without the member id argument.
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$8\r\nKR.ADMIN\r\n$11\r\nFIND-MEMBER\r\n".getBytes());

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Member id is required", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenFindMemberWithExtraArguments() {
        // Behavior: FIND-MEMBER rejects extra arguments with "Wrong number of arguments for 'KR.ADMIN FIND-MEMBER' command".
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*4\r\n$8\r\nKR.ADMIN\r\n$11\r\nFIND-MEMBER\r\n$4\r\nabcd\r\n$5\r\nextra\r\n".getBytes());

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Wrong number of arguments for 'KR.ADMIN FIND-MEMBER' command", actualMessage.content());
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
        assertEquals("ERR Unknown member status: 'some-status'", actualMessage.content());
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
        assertEquals(String.format("ERR No such member: '%s'", memberId), actualMessage.content());
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
    void shouldReturnErrorWhenListSilentMembersWithExtraArguments() {
        // Behavior: LIST-SILENT-MEMBERS rejects extra arguments with "Wrong number of arguments for 'KR.ADMIN LIST-SILENT-MEMBERS' command".
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*3\r\n$8\r\nKR.ADMIN\r\n$19\r\nLIST-SILENT-MEMBERS\r\n$5\r\nextra\r\n".getBytes());

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Wrong number of arguments for 'KR.ADMIN LIST-SILENT-MEMBERS' command", actualMessage.content());
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
    void shouldReturnErrorWhenRemoveMemberWithInvalidNumberOfArguments() {
        // Behavior: REMOVE-MEMBER rejects a call without the member id argument.
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$8\r\nKR.ADMIN\r\n$13\r\nREMOVE-MEMBER\r\n".getBytes());

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Wrong number of arguments for 'KR.ADMIN REMOVE-MEMBER' command", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenRemoveMemberWithInvalidMemberId() {
        // Behavior: REMOVE-MEMBER rejects a member id whose length is not a valid member id length.
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.removeMember("not-a-valid-member-id").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Invalid member id: 'not-a-valid-member-id'", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenRemoveMemberWithUnknownPrefix() {
        // Behavior: REMOVE-MEMBER rejects a 4-character prefix that matches no known member.
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.removeMember("zzzz").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR No member found with prefix: 'zzzz'", actualMessage.content());
    }

    @Test
    void shouldSetShardStatus() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus(1, "READONLY").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    void shouldSetShardStatusForAllShards() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus("READONLY").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenSetShardStatusWithNegativeShardId() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus(-1, "READONLY").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Invalid shard id: '-1'", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenSetShardStatusWithShardIdBiggerThanNumberOfShards() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus(1231253, "READONLY").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Invalid shard id: '1231253'", actualMessage.content());
    }

    @Test
    void shouldDescribeCluster() {
        // Behavior: DESCRIBE-CLUSTER returns exactly three top-level keys: metadata_version, cluster_name and shards.
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.describeCluster().encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage result = (MapRedisMessage) msg;

        Set<String> topLevelKeys = new HashSet<>();
        result.children().forEach((keyMessage, ignored) -> {
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) keyMessage;
            topLevelKeys.add(key.content().toString(StandardCharsets.UTF_8));
        });

        assertTrue(topLevelKeys.contains("metadata_version"));
        assertTrue(topLevelKeys.contains("cluster_name"));

        assertTrue(topLevelKeys.contains("shards"));
        assertEquals(3, topLevelKeys.size());
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
            MapRedisMessage shards = (MapRedisMessage) valueMessage;
            assertFalse(shards.children().isEmpty());

            shards.children().forEach((shardIdMessage, shardDetailsMessage) -> {
                assertInstanceOf(IntegerRedisMessage.class, shardIdMessage);
                assertInstanceOf(MapRedisMessage.class, shardDetailsMessage);

                MapRedisMessage shardDetails = (MapRedisMessage) shardDetailsMessage;
                Set<String> shardKeys = new HashSet<>();
                shardDetails.children().forEach((sk, ignored) ->
                        shardKeys.add(((FullBulkStringRedisMessage) sk).content().toString(StandardCharsets.UTF_8))
                );

                assertEquals(Set.of("primary", "standbys", "status", "linked_volumes"), shardKeys);
            });
        });
    }

    @Test
    void shouldReturnErrorWhenDescribeClusterWithExtraArguments() {
        // Behavior: DESCRIBE-CLUSTER rejects extra arguments with "Wrong number of arguments for 'KR.ADMIN DESCRIBE-CLUSTER' command".
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*3\r\n$8\r\nKR.ADMIN\r\n$16\r\nDESCRIBE-CLUSTER\r\n$5\r\nextra\r\n".getBytes());

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("Wrong number of arguments for 'KR.ADMIN DESCRIBE-CLUSTER' command"));
    }

    @Test
    void shouldDescribeShard() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.describeShard(1).encode(buf);

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
    void shouldReturnErrorWhenDescribeShardWithNonIntegerShardId() {
        // Behavior: DESCRIBE-SHARD rejects a shard id that is not an integer.
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*3\r\n$8\r\nKR.ADMIN\r\n$14\r\nDESCRIBE-SHARD\r\n$3\r\nabc\r\n".getBytes());

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Invalid shard id: 'abc'", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenDescribeShardWithNonExistentShardId() {
        // Behavior: DESCRIBE-SHARD rejects a shard id that does not exist in the cluster.
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.describeShard(1231253).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Invalid shard id: '1231253'", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenDescribeShardWithInvalidNumberOfArguments() {
        // Behavior: DESCRIBE-SHARD rejects a call without the shard id argument.
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$8\r\nKR.ADMIN\r\n$14\r\nDESCRIBE-SHARD\r\n".getBytes());

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Wrong number of arguments for 'KR.ADMIN DESCRIBE-SHARD' command", actualMessage.content());
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
        assertEquals("ERR Cluster name does not match", actualMessage.content());
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

        boolean exists = TransactionUtil.execute(context, tr -> {
            List<String> subpath = KronotopDirectory.kronotop().cluster(context.getClusterName()).toList();
            return context.getDirectoryLayer().exists(tr, subpath).join();
        });
        assertFalse(exists);
    }

    @Test
    void shouldReturnErrorWhenDropClusterWithoutClusterName() {
        // Behavior: DROP-CLUSTER rejects a call without the cluster name argument.
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$8\r\nKR.ADMIN\r\n$12\r\nDROP-CLUSTER\r\n".getBytes());

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Cluster name is required", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenDropClusterWithExtraArguments() {
        // Behavior: DROP-CLUSTER rejects more than two arguments with "Wrong number of arguments for 'KR.ADMIN DROP-CLUSTER' command".
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*5\r\n$8\r\nKR.ADMIN\r\n$12\r\nDROP-CLUSTER\r\n$4\r\nname\r\n$5\r\ntoken\r\n$5\r\nextra\r\n".getBytes());

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Wrong number of arguments for 'KR.ADMIN DROP-CLUSTER' command", actualMessage.content());
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
        assertEquals("ERR Invalid drop-cluster token", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenTokenIsAlreadyConsumed() {
        // Behavior: A token cannot be used twice. The second attempt fails.
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
        assertEquals("ERR No pending drop-cluster token for this cluster", actualMessage.content());
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
        assertEquals("ERR No pending drop-cluster token for this cluster", actualMessage.content());
    }

    private DropClusterSubcommand dropClusterSubcommand() {
        KrAdminHandler handler = (KrAdminHandler) context.getHandlers(ServerKind.INTERNAL).get("KR.ADMIN").handler();
        return (DropClusterSubcommand) handler.handlers.get(KrAdminSubcommand.DROP_CLUSTER);
    }

    private String requestDropClusterToken(KrAdminCommandBuilder<String, String> cmd) {
        ByteBuf buf = Unpooled.buffer();
        cmd.dropCluster(context.getClusterName()).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(FullBulkStringRedisMessage.class, msg);
        return ((FullBulkStringRedisMessage) msg).content().toString(StandardCharsets.UTF_8);
    }

    private void putExpiredDropClusterToken(String token) {
        long createdAtNanos = System.nanoTime() - TimeUnit.SECONDS.toNanos(DropClusterSubcommand.TOKEN_TTL_MILLIS * 2);
        dropClusterSubcommand().pendingTokens.put(context.getClusterName(),
                new DropClusterSubcommand.DropClusterToken(token, createdAtNanos));
    }

    @Test
    void shouldReturnSameTokenWhenPhase1CalledTwice() {
        // Behavior: Phase 1 returns the pending token again while it is still valid.
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        String first = requestDropClusterToken(cmd);
        String second = requestDropClusterToken(cmd);
        assertEquals(first, second);
    }

    @Test
    void shouldKeepPendingTokenAfterInvalidToken() {
        // Behavior: A mismatched token does not remove the pending token. The correct token still works.
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        String token = requestDropClusterToken(cmd);

        ByteBuf buf = Unpooled.buffer();
        cmd.dropCluster(context.getClusterName(), UUID.randomUUID().toString()).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);

        ByteBuf buf2 = Unpooled.buffer();
        cmd.dropCluster(context.getClusterName(), token).encode(buf2);
        Object msg2 = runCommand(channel, buf2);
        assertInstanceOf(SimpleStringRedisMessage.class, msg2);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) msg2).content());
    }

    @Test
    void shouldReturnErrorAndRemoveTokenWhenExpired() {
        // Behavior: Phase 2 rejects an expired token and removes it. The next attempt finds no pending token.
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        String token = UUID.randomUUID().toString();
        putExpiredDropClusterToken(token);

        ByteBuf buf = Unpooled.buffer();
        cmd.dropCluster(context.getClusterName(), token).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        assertEquals("ERR drop-cluster token has expired", ((ErrorRedisMessage) msg).content());

        ByteBuf buf2 = Unpooled.buffer();
        cmd.dropCluster(context.getClusterName(), token).encode(buf2);
        Object msg2 = runCommand(channel, buf2);
        assertInstanceOf(ErrorRedisMessage.class, msg2);
        assertEquals("ERR No pending drop-cluster token for this cluster", ((ErrorRedisMessage) msg2).content());
    }

    @Test
    void shouldIssueNewTokenWhenPreviousExpired() {
        // Behavior: Phase 1 ignores an expired pending token and issues a new one.
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        String expired = UUID.randomUUID().toString();
        putExpiredDropClusterToken(expired);

        String fresh = requestDropClusterToken(cmd);
        assertNotEquals(expired, fresh);
    }

    private void createStalePrefixes(int count) {
        String clusterName = context.getConfig().getString("cluster.name");
        List<String> subpath = KronotopDirectory.kronotop().cluster(clusterName).extend("test-stale-prefixes");
        try (Transaction tr = database.createTransaction()) {
            DirectorySubspace subspace = context.getDirectoryLayer().createOrOpen(tr, subpath).join();
            byte[] prefixPointer = subspace.pack("stale-pointer");
            for (int i = 0; i < count; i++) {
                Prefix testPrefix = new Prefix(UUID.randomUUID().toString());
                PrefixUtil.register(context, tr, testPrefix, new PrefixEntry(prefixPointer, EntryKind.DOCUMENT, UUIDUtil.toBytes(UUID.randomUUID())));
            }
            // Clear the pointer to make all prefixes stale
            tr.clear(prefixPointer);
            tr.commit().join();
        }
    }

    @Test
    void shouldStartCollectStalePrefixesTask() {
        {
            KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
            ByteBuf buf = Unpooled.buffer();
            cmd.collectStalePrefixes("START").encode(buf);

            Object msg = runCommand(channel, buf);

            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            TaskAdminCommandBuilder<String, String> taskAdmin = new TaskAdminCommandBuilder<>(StringCodec.ASCII);

            ByteBuf buf = Unpooled.buffer();
            taskAdmin.list().encode(buf);

            KronotopTestInstance instance = getInstances().getFirst();
            instance.getChannel().writeInbound(buf);
            Object msg = instance.getChannel().readOutbound();
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage actualMessage = (MapRedisMessage) msg;
            boolean found = false;
            for (RedisMessage message : actualMessage.children().keySet()) {
                FullBulkStringRedisMessage taskName = (FullBulkStringRedisMessage) message;
                if (CollectStalePrefixesTask.NAME.equals(taskName.content().toString(StandardCharsets.UTF_8))) {
                    found = true;
                    break;
                }
            }
            assertTrue(found);
        }
    }

    @Test
    void shouldReturnErrorWhenCollectStalePrefixesTaskAlreadyExists() {
        // Behavior: Starting a collect-stale-prefixes task while one is already running returns an error.

        // Create stale prefixes so the task has real work and doesn't finish instantly.
        createStalePrefixes(500);

        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.collectStalePrefixes("START").encode(buf);

            Object msg = runCommand(channel, buf);

            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        ByteBuf buf = Unpooled.buffer();
        cmd.collectStalePrefixes("START").encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Task already exists: 'cluster:collect-stale-prefixes-task'", actualMessage.content());

        // Cleanup: stop the running task
        ByteBuf stopBuf = Unpooled.buffer();
        cmd.collectStalePrefixes("STOP").encode(stopBuf);
        runCommand(channel, stopBuf);
    }

    @Test
    void shouldReturnErrorWhenStoppingNonExistentCollectStalePrefixesTask() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.collectStalePrefixes("STOP").encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR No such task: 'cluster:collect-stale-prefixes-task'", actualMessage.content());
    }

    boolean hasCollectStalePrefixesTask() {
        TaskAdminCommandBuilder<String, String> taskAdmin = new TaskAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        taskAdmin.list().encode(buf);

        KronotopTestInstance instance = getInstances().getFirst();
        instance.getChannel().writeInbound(buf);
        Object msg = instance.getChannel().readOutbound();
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        for (RedisMessage message : actualMessage.children().keySet()) {
            FullBulkStringRedisMessage taskName = (FullBulkStringRedisMessage) message;
            if (CollectStalePrefixesTask.NAME.equals(taskName.content().toString(StandardCharsets.UTF_8))) {
                return true;
            }
        }
        return false;
    }

    @Test
    void shouldStartThenStopCollectStalePrefixesTask() {
        {
            KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
            ByteBuf buf = Unpooled.buffer();
            cmd.collectStalePrefixes("START").encode(buf);

            Object msg = runCommand(channel, buf);

            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

            ByteBuf buf = Unpooled.buffer();
            cmd.collectStalePrefixes("STOP").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        assertFalse(hasCollectStalePrefixesTask());
    }

    @Test
    void shouldRemoveCollectStalePrefixesTask() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.collectStalePrefixes("START").encode(buf);

            Object msg = runCommand(channel, buf);

            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.collectStalePrefixes("REMOVE").encode(buf);

            Object msg = runCommand(channel, buf);

            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        assertFalse(hasCollectStalePrefixesTask());
    }

    @Test
    void shouldLocateCollectStalePrefixesTask() {
        // Behavior: LOCATE returns a map with member_id, process_id and the advertise lists of the task owner.
        createStalePrefixes(500);

        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        // Start the task
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.collectStalePrefixes("START").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        // Locate the task
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.collectStalePrefixes("LOCATE").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapMsg = (MapRedisMessage) msg;

            boolean foundMemberId = false;
            boolean foundProcessId = false;
            boolean foundExternalAdvertise = false;
            boolean foundInternalAdvertise = false;
            for (var entry : mapMsg.children().entrySet()) {
                String key = ((FullBulkStringRedisMessage) entry.getKey()).content().toString(StandardCharsets.US_ASCII);
                if ("member_id".equals(key)) {
                    String value = ((FullBulkStringRedisMessage) entry.getValue()).content().toString(StandardCharsets.US_ASCII);
                    assertFalse(value.isEmpty());
                    assertEquals(context.getMember().getId(), value);
                    foundMemberId = true;
                } else if ("process_id".equals(key)) {
                    String value = ((FullBulkStringRedisMessage) entry.getValue()).content().toString(StandardCharsets.US_ASCII);
                    assertFalse(value.isEmpty());
                    assertEquals(context.getMember().getProcessId(), VersionstampUtil.base32HexDecode(value));
                    foundProcessId = true;
                } else if ("external_advertise".equals(key)) {
                    assertAdvertise(entry.getValue(), context.getMember().getExternalAdvertise());
                    foundExternalAdvertise = true;
                } else if ("internal_advertise".equals(key)) {
                    assertAdvertise(entry.getValue(), context.getMember().getInternalAdvertise());
                    foundInternalAdvertise = true;
                }
            }
            assertTrue(foundMemberId);
            assertTrue(foundProcessId);
            assertTrue(foundInternalAdvertise);
            assertTrue(foundExternalAdvertise);
        }

        // Stop the task
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.collectStalePrefixes("STOP").encode(buf);
            runCommand(channel, buf);
        }
    }

    @Test
    void shouldReturnErrorWhenRemovingNonExistentCollectStalePrefixesTask() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.collectStalePrefixes("REMOVE").encode(buf);

            Object msg = runCommand(channel, buf);

            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
            assertEquals("ERR No such task: 'cluster:collect-stale-prefixes-task'", actualMessage.content());
        }
    }

    private void assertAdvertise(RedisMessage message, List<Address> expected) {
        ArrayRedisMessage array = (ArrayRedisMessage) message;
        assertEquals(expected.size(), array.children().size());
        for (int i = 0; i < expected.size(); i++) {
            FullBulkStringRedisMessage item = (FullBulkStringRedisMessage) array.children().get(i);
            assertEquals(expected.get(i).toString(), item.content().toString(StandardCharsets.UTF_8));
        }
    }
}
