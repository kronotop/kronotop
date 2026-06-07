/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.cluster.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.MemberIdGenerator;
import com.kronotop.cluster.MembershipUtil;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.commands.KrAdminCommandBuilder;
import com.kronotop.commands.VolumeAdminCommandBuilder;
import com.kronotop.instance.KronotopInstance;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.BaseNetworkedVolumeIntegrationTest;
import com.kronotop.volume.VolumeNames;
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
import java.util.Set;

import static com.kronotop.cluster.sharding.ShardStatus.READONLY;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class RouteSubcommandHandlerTest extends BaseNetworkedVolumeIntegrationTest {

    @Test
    void shouldSetRouteStandby() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        // Add a second instance to use as standby
        KronotopTestInstance secondInstance = addNewInstance();
        String standbyMemberId = secondInstance.getMember().getId();

        ByteBuf buf = Unpooled.buffer();
        cmd.route("set", "standby", "stash", SHARD_ID, standbyMemberId).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());

        // Verify standby was added to metadata
        DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(SHARD_KIND, SHARD_ID);
        try (Transaction tr = database.createTransaction()) {
            Set<String> standbyMemberIds = MembershipUtil.loadStandbyMemberIds(tr, shardSubspace);
            assertTrue(standbyMemberIds.contains(standbyMemberId));
        }
    }

    @Test
    void shouldSetRouteStandbyForAllShards() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        // Add a second instance to use as standby
        KronotopTestInstance secondInstance = addNewInstance();
        String standbyMemberId = secondInstance.getMember().getId();

        ByteBuf buf = Unpooled.buffer();
        // Use "*" for all shards
        cmd.route("set", "standby", "stash", standbyMemberId).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());

        // Verify standby was added to all Stash shards
        int numberOfShards = context.getConfig().getInt("stash.shards");
        try (Transaction tr = database.createTransaction()) {
            for (int shardId = 0; shardId < numberOfShards; shardId++) {
                DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(SHARD_KIND, shardId);
                Set<String> standbyMemberIds = MembershipUtil.loadStandbyMemberIds(tr, shardSubspace);
                assertTrue(standbyMemberIds.contains(standbyMemberId), "Standby not found in shard " + shardId);
            }
        }
    }

    @Test
    void shouldUnsetRouteStandby() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        // Add a second instance and set it as standby first
        KronotopTestInstance secondInstance = addNewInstance();
        String standbyMemberId = secondInstance.getMember().getId();

        ByteBuf setBuf = Unpooled.buffer();
        cmd.route("set", "standby", "stash", SHARD_ID, standbyMemberId).encode(setBuf);
        Object setMsg = runCommand(channel, setBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, setMsg);

        // Verify standby was added
        DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(SHARD_KIND, SHARD_ID);
        try (Transaction tr = database.createTransaction()) {
            Set<String> standbyMemberIds = MembershipUtil.loadStandbyMemberIds(tr, shardSubspace);
            assertTrue(standbyMemberIds.contains(standbyMemberId));
        }

        // Now unset the standby
        ByteBuf unsetBuf = Unpooled.buffer();
        cmd.route("unset", "standby", "stash", SHARD_ID, standbyMemberId).encode(unsetBuf);

        Object msg = runCommand(channel, unsetBuf);

        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());

        // Verify standby was removed from metadata
        try (Transaction tr = database.createTransaction()) {
            Set<String> standbyMemberIds = MembershipUtil.loadStandbyMemberIds(tr, shardSubspace);
            assertFalse(standbyMemberIds.contains(standbyMemberId));
        }
    }

    @Test
    void shouldUnsetRouteStandbyForAllShards() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        // Add a second instance and set it as standby for all shards
        KronotopTestInstance secondInstance = addNewInstance();
        String standbyMemberId = secondInstance.getMember().getId();

        ByteBuf setBuf = Unpooled.buffer();
        cmd.route("set", "standby", "stash", standbyMemberId).encode(setBuf);
        Object setMsg = runCommand(channel, setBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, setMsg);

        // Verify standby was added to all shards
        int numberOfShards = context.getConfig().getInt("stash.shards");
        try (Transaction tr = database.createTransaction()) {
            for (int shardId = 0; shardId < numberOfShards; shardId++) {
                DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(SHARD_KIND, shardId);
                Set<String> standbyMemberIds = MembershipUtil.loadStandbyMemberIds(tr, shardSubspace);
                assertTrue(standbyMemberIds.contains(standbyMemberId), "Standby not found in shard " + shardId);
            }
        }

        // Now unset the standby for all shards
        ByteBuf unsetBuf = Unpooled.buffer();
        cmd.route("unset", "standby", "stash", standbyMemberId).encode(unsetBuf);

        Object msg = runCommand(channel, unsetBuf);

        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());

        // Verify standby was removed from all shards
        try (Transaction tr = database.createTransaction()) {
            for (int shardId = 0; shardId < numberOfShards; shardId++) {
                DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(SHARD_KIND, shardId);
                Set<String> standbyMemberIds = MembershipUtil.loadStandbyMemberIds(tr, shardSubspace);
                assertFalse(standbyMemberIds.contains(standbyMemberId), "Standby still present in shard " + shardId);
            }
        }
    }

    @Test
    void shouldReturnErrorWhenSetRouteStandbyWithPrimaryAsMember() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        // Try to set the primary member as a standby
        String primaryMemberId = kronotopInstance.getMember().getId();

        ByteBuf buf = Unpooled.buffer();
        cmd.route("set", "standby", "stash", SHARD_ID, primaryMemberId).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("primary cannot be assigned as a standby"));
    }

    @Test
    void shouldReturnErrorWhenSetRouteStandbyAlreadyAssigned() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        // Add a second instance and set it as standby
        KronotopTestInstance secondInstance = addNewInstance();
        String standbyMemberId = secondInstance.getMember().getId();

        ByteBuf setBuf = Unpooled.buffer();
        cmd.route("set", "standby", "stash", SHARD_ID, standbyMemberId).encode(setBuf);
        Object setMsg = runCommand(channel, setBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, setMsg);

        // Try to set the same member as standby again
        ByteBuf buf = Unpooled.buffer();
        cmd.route("set", "standby", "stash", SHARD_ID, standbyMemberId).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("already assigned as a standby"));
    }

    @Test
    void shouldReturnErrorWhenUnsetRouteStandbyNotAStandby() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        // Add a second instance but don't set it as standby
        KronotopTestInstance secondInstance = addNewInstance();
        String memberId = secondInstance.getMember().getId();

        // Try to unset a member that is not a standby
        ByteBuf buf = Unpooled.buffer();
        cmd.route("unset", "standby", "stash", SHARD_ID, memberId).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("member is not a standby"));
    }

    @Test
    void shouldReturnErrorWhenUnsetRoutePrimary() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        String primaryMemberId = kronotopInstance.getMember().getId();

        ByteBuf buf = Unpooled.buffer();
        cmd.route("unset", "primary", "stash", SHARD_ID, primaryMemberId).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("UNSET PRIMARY is not supported"));
    }

    @Test
    void shouldReturnErrorWhenRouteMemberNotFound() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        // Generate a valid member ID that is not registered
        String nonExistentMemberId = MemberIdGenerator.generateId();

        ByteBuf buf = Unpooled.buffer();
        cmd.route("set", "standby", "stash", SHARD_ID, nonExistentMemberId).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("member not found"));
    }

    @Test
    void shouldReturnErrorWhenRouteWithInvalidNumberOfParameters() {
        // Send raw command with missing parameters
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*3\r\n$8\r\nKR.ADMIN\r\n$5\r\nROUTE\r\n$3\r\nSET\r\n".getBytes());

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("invalid number of parameters"));
    }

    @Test
    void shouldReturnErrorWhenRouteWithInvalidOperationKind() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        KronotopTestInstance secondInstance = addNewInstance();
        String memberId = secondInstance.getMember().getId();

        ByteBuf buf = Unpooled.buffer();
        cmd.route("invalid", "standby", "stash", SHARD_ID, memberId).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("Invalid operation kind: invalid"));
    }

    @Test
    void shouldReturnErrorWhenRouteWithInvalidRouteKind() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        KronotopTestInstance secondInstance = addNewInstance();
        String memberId = secondInstance.getMember().getId();

        ByteBuf buf = Unpooled.buffer();
        cmd.route("set", "invalid", "stash", SHARD_ID, memberId).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("Invalid route kind: invalid"));
    }

    @Test
    void shouldReturnErrorWhenSetPrimaryAndShardStatusIsReadWrite() {
        // Behavior: SET PRIMARY rejects when shard status is READWRITE (default state after init)
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        KronotopTestInstance secondInstance = addNewInstance();
        String memberId = secondInstance.getMember().getId();

        ByteBuf buf = Unpooled.buffer();
        cmd.route("SET", "PRIMARY", SHARD_KIND.name(), SHARD_ID, memberId).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("Shard status must not be READWRITE"));
    }

    @Test
    void shouldReturnErrorWhenSetPrimaryAndMemberIsNotStandby() {
        // Behavior: SET PRIMARY rejects when the target member is not a standby for the shard
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        KronotopTestInstance secondInstance = addNewInstance();
        String memberId = secondInstance.getMember().getId();

        // Set shard status to READONLY so we pass the shard status check
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.setShardStatus(SHARD_KIND.name(), SHARD_ID, READONLY.name()).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        ByteBuf buf = Unpooled.buffer();
        cmd.route("SET", "PRIMARY", SHARD_KIND.name(), SHARD_ID, memberId).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("is not a standby"));
    }

    @Test
    void shouldReturnErrorWhenSetPrimaryAndVolumeStatusIsNotReadOnly() {
        // Behavior: SET PRIMARY rejects when volume status is not READONLY
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        KronotopTestInstance secondInstance = addNewInstance();
        String standbyMemberId = secondInstance.getMember().getId();

        // Set as standby first
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.route("SET", "STANDBY", SHARD_KIND.name(), SHARD_ID, standbyMemberId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        // Wait for the routing table to be updated on standby
        RoutingService routing = secondInstance.getContext().getService(RoutingService.NAME);
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            Route route = routing.findRoute(SHARD_KIND, SHARD_ID);
            if (route == null) {
                return false;
            }
            return route.standbys().contains(secondInstance.getMember());
        });

        // Set shard status to READONLY
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.setShardStatus(SHARD_KIND.name(), SHARD_ID, READONLY.name()).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        // Do NOT set volume to READONLY — this should trigger the volume status error

        ByteBuf buf = Unpooled.buffer();
        cmd.route("SET", "PRIMARY", SHARD_KIND.name(), SHARD_ID, standbyMemberId).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("Volume status must be"));
    }

    @Test
    void shouldSetPrimaryAndVerifyRouteMetadata() throws IOException {
        // Behavior: Full primary transition — updates primary in FDB, removes from standbys, updates routing table
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);

        ByteBuffer[] entries = getEntries(number, length);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            AppendResult result = volume.append(session, entries);
            tr.commit().join();
            assertEquals(number, result.getVersionstampedKeys().length);
        }

        KronotopInstance standby = addNewInstance();
        String standbyMemberId = standby.getMember().getId();

        // Set as standby
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.route("SET", "STANDBY", SHARD_KIND.name(), SHARD_ID, standbyMemberId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        // Wait for replication to become active on standby
        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            EnumMap<ShardKind, List<Integer>> activeReplications = replicationService.listActiveReplicationsByShard();
            List<Integer> shardIds = activeReplications.get(SHARD_KIND);
            return shardIds != null && shardIds.contains(SHARD_ID);
        });

        // Set shard status to READONLY
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.setShardStatus(SHARD_KIND.name(), SHARD_ID, READONLY.name()).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        // Set volume status to READONLY
        {
            VolumeAdminCommandBuilder<String, String> cmd2 = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
            ByteBuf buf = Unpooled.buffer();
            cmd2.setStatus(VolumeNames.format(SHARD_KIND, SHARD_ID), READONLY.name()).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        // Poll SET PRIMARY until replication catches up
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            ByteBuf buf = Unpooled.buffer();
            cmd.route("SET", "PRIMARY", SHARD_KIND.name(), SHARD_ID, standbyMemberId).encode(buf);
            Object msg = runCommand(channel, buf);
            if (msg instanceof SimpleStringRedisMessage actualMessage) {
                return Response.OK.equals(actualMessage.content());
            }
            return false;
        });

        // Verify FDB: primary is now the promoted member
        DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(SHARD_KIND, SHARD_ID);
        try (Transaction tr = database.createTransaction()) {
            String primaryMemberId = MembershipUtil.loadPrimaryMemberId(tr, shardSubspace);
            assertEquals(standbyMemberId, primaryMemberId);

            Set<String> standbyMemberIds = MembershipUtil.loadStandbyMemberIds(tr, shardSubspace);
            assertFalse(standbyMemberIds.contains(standbyMemberId));
        }

        // Verify routing table on standby reflects the new primary
        RoutingService routing = standby.getContext().getService(RoutingService.NAME);
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            Route route = routing.findRoute(SHARD_KIND, SHARD_ID);
            if (route == null) {
                return false;
            }
            return route.primary().equals(standby.getMember());
        });
    }
}
