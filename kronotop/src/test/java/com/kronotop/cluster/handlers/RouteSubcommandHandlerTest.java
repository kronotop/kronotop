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

package com.kronotop.cluster.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.MemberIdGenerator;
import com.kronotop.cluster.MembershipUtils;
import com.kronotop.commandbuilder.kronotop.KrAdminCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.BaseNetworkedVolumeIntegrationTest;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RouteSubcommandHandlerTest extends BaseNetworkedVolumeIntegrationTest {

    @Test
    void shouldSetRouteStandby() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        // Add a second instance to use as standby
        KronotopTestInstance secondInstance = addNewInstance();
        String standbyMemberId = secondInstance.getMember().getId();

        ByteBuf buf = Unpooled.buffer();
        cmd.route("set", "standby", "redis", SHARD_ID, standbyMemberId).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());

        // Verify standby was added to metadata
        DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(SHARD_KIND, SHARD_ID);
        try (Transaction tr = database.createTransaction()) {
            Set<String> standbyMemberIds = MembershipUtils.loadStandbyMemberIds(tr, shardSubspace);
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
        cmd.route("set", "standby", "redis", standbyMemberId).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());

        // Verify standby was added to all Redis shards
        int numberOfShards = context.getConfig().getInt("redis.shards");
        try (Transaction tr = database.createTransaction()) {
            for (int shardId = 0; shardId < numberOfShards; shardId++) {
                DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(SHARD_KIND, shardId);
                Set<String> standbyMemberIds = MembershipUtils.loadStandbyMemberIds(tr, shardSubspace);
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
        cmd.route("set", "standby", "redis", SHARD_ID, standbyMemberId).encode(setBuf);
        Object setMsg = runCommand(channel, setBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, setMsg);

        // Verify standby was added
        DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(SHARD_KIND, SHARD_ID);
        try (Transaction tr = database.createTransaction()) {
            Set<String> standbyMemberIds = MembershipUtils.loadStandbyMemberIds(tr, shardSubspace);
            assertTrue(standbyMemberIds.contains(standbyMemberId));
        }

        // Now unset the standby
        ByteBuf unsetBuf = Unpooled.buffer();
        cmd.route("unset", "standby", "redis", SHARD_ID, standbyMemberId).encode(unsetBuf);

        Object msg = runCommand(channel, unsetBuf);

        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());

        // Verify standby was removed from metadata
        try (Transaction tr = database.createTransaction()) {
            Set<String> standbyMemberIds = MembershipUtils.loadStandbyMemberIds(tr, shardSubspace);
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
        cmd.route("set", "standby", "redis", standbyMemberId).encode(setBuf);
        Object setMsg = runCommand(channel, setBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, setMsg);

        // Verify standby was added to all shards
        int numberOfShards = context.getConfig().getInt("redis.shards");
        try (Transaction tr = database.createTransaction()) {
            for (int shardId = 0; shardId < numberOfShards; shardId++) {
                DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(SHARD_KIND, shardId);
                Set<String> standbyMemberIds = MembershipUtils.loadStandbyMemberIds(tr, shardSubspace);
                assertTrue(standbyMemberIds.contains(standbyMemberId), "Standby not found in shard " + shardId);
            }
        }

        // Now unset the standby for all shards
        ByteBuf unsetBuf = Unpooled.buffer();
        cmd.route("unset", "standby", "redis", standbyMemberId).encode(unsetBuf);

        Object msg = runCommand(channel, unsetBuf);

        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());

        // Verify standby was removed from all shards
        try (Transaction tr = database.createTransaction()) {
            for (int shardId = 0; shardId < numberOfShards; shardId++) {
                DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(SHARD_KIND, shardId);
                Set<String> standbyMemberIds = MembershipUtils.loadStandbyMemberIds(tr, shardSubspace);
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
        cmd.route("set", "standby", "redis", SHARD_ID, primaryMemberId).encode(buf);

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
        cmd.route("set", "standby", "redis", SHARD_ID, standbyMemberId).encode(setBuf);
        Object setMsg = runCommand(channel, setBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, setMsg);

        // Try to set the same member as standby again
        ByteBuf buf = Unpooled.buffer();
        cmd.route("set", "standby", "redis", SHARD_ID, standbyMemberId).encode(buf);

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
        cmd.route("unset", "standby", "redis", SHARD_ID, memberId).encode(buf);

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
        cmd.route("unset", "primary", "redis", SHARD_ID, primaryMemberId).encode(buf);

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
        cmd.route("set", "standby", "redis", SHARD_ID, nonExistentMemberId).encode(buf);

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
        cmd.route("invalid", "standby", "redis", SHARD_ID, memberId).encode(buf);

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
        cmd.route("set", "invalid", "redis", SHARD_ID, memberId).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("Invalid route kind: invalid"));
    }
}
