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

import com.kronotop.BaseUninitializedKronotopInstanceTest;
import com.kronotop.cluster.MemberIdGenerator;
import com.kronotop.cluster.RouteKind;
import com.kronotop.commands.KrAdminCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.Command;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class KrAdminIntegrationTest extends BaseUninitializedKronotopInstanceTest {
    private final KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

    private void initializeCluster(Command<String, String, String> command, List<Integer> expectedShardIds) {
        ByteBuf buf = Unpooled.buffer();
        command.encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());

        // Shard ids are discovered from FoundationDB after the cluster is initialized.
        await().atMost(5, TimeUnit.SECONDS).until(() ->
                kronotopInstance.getContext().getShardRegistry().getShardIds().equals(expectedShardIds)
        );
    }

    private void assertInitializeClusterError(String expectedError, String... arguments) {
        List<String> rawArgs = new ArrayList<>(List.of("INITIALIZE-CLUSTER"));
        rawArgs.addAll(List.of(arguments));

        Object msg = runRaw(channel, KrAdminCommandBuilder.CommandType.KR_ADMIN, rawArgs);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        assertEquals(expectedError, ((ErrorRedisMessage) msg).content());
    }

    @Test
    void shouldInitializeClusterWithOneShardByDefault() {
        // Behavior: On an uninitialized cluster, INITIALIZE-CLUSTER without the SHARDS argument returns OK
        // and the cluster ends up with exactly one shard, shard 0.
        initializeCluster(cmd.initializeCluster(), List.of(0));
    }

    @Test
    void shouldInitializeClusterWithGivenShardCount() {
        // Behavior: On an uninitialized cluster, INITIALIZE-CLUSTER SHARDS 3 returns OK and the cluster
        // ends up with exactly three shards, with ids 0, 1 and 2.
        initializeCluster(cmd.initializeCluster(3), List.of(0, 1, 2));
    }

    @Test
    void shouldReturnErrorWhenShardCountIsNotPositive() {
        // Behavior: INITIALIZE-CLUSTER SHARDS 0 is rejected with
        // "SHARDS argument must be followed by a positive integer".
        assertInitializeClusterError("ERR SHARDS argument must be followed by a positive integer", "SHARDS", "0");
    }

    @Test
    void shouldReturnErrorWhenShardCountIsNotAnInteger() {
        // Behavior: INITIALIZE-CLUSTER SHARDS abc is rejected with "Value is not an integer or out of range".
        assertInitializeClusterError("ERR Value is not an integer or out of range", "SHARDS", "abc");
    }

    @Test
    void shouldReturnErrorWhenShardsHasNoValue() {
        // Behavior: INITIALIZE-CLUSTER SHARDS without a value is rejected with
        // "SHARDS argument must be followed by a positive integer".
        assertInitializeClusterError("ERR SHARDS argument must be followed by a positive integer", "SHARDS");
    }

    @Test
    void shouldReturnErrorWhenInitializeClusterWithUnknownArgument() {
        // Behavior: INITIALIZE-CLUSTER FOO is rejected with "Unknown argument: 'FOO'".
        assertInitializeClusterError("ERR Unknown argument: 'FOO'", "FOO");
    }

    @Test
    void shouldReturnErrorWhenClusterUninitialized() {
        // Behavior: On a cluster that has not been initialized, ROUTE SET STANDBY for shard 1 is rejected
        // with "Cluster has not been initialized yet".

        ByteBuf buf = Unpooled.buffer();
        // it can be any command that requires an initialized cluster.
        cmd.route("SET", "STANDBY", 1, MemberIdGenerator.generateId()).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Cluster has not been initialized yet", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenAssigningStandbyWithoutPrimary() {
        // Behavior: On an initialized cluster where shard 1 has no primary, ROUTE SET STANDBY for shard 1
        // with the member's own id is rejected with "No primary member assigned yet".
        initializeCluster(cmd.initializeCluster(2), List.of(0, 1));

        ByteBuf buf = Unpooled.buffer();
        cmd.route("SET", "STANDBY", 1, kronotopInstance.getMember().getId()).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR No primary member assigned yet", actualMessage.content());
    }

    @Test
    void shouldReturnErrorWhenSettingRouteWithMemberNotFound() {
        // Behavior: On an initialized cluster, ROUTE SET for shard 1 with a member id that is not registered
        // is rejected with "No such member: '<member-id>'". This holds for every route kind.
        initializeCluster(cmd.initializeCluster(2), List.of(0, 1));

        for (RouteKind routeKind : RouteKind.values()) {
            ByteBuf buf = Unpooled.buffer();
            String memberId = MemberIdGenerator.generateId();
            cmd.route("SET", routeKind.name(), 1, memberId).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
            assertEquals(String.format("ERR No such member: '%s'", memberId), actualMessage.content());
        }
    }
}
