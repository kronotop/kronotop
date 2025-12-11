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

package com.kronotop.cluster;

import com.kronotop.BaseClusterTestWithTCPServer;
import com.kronotop.KronotopTestInstance;
import com.kronotop.MemberAttributes;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.commandbuilder.kronotop.KrAdminCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

import static org.junit.jupiter.api.Assertions.*;

class RoutingServiceTest extends BaseClusterTestWithTCPServer {

    @Test
    void shouldReturnNullWhenRouteNotSet() {
        KronotopTestInstance instance = getInstances().getFirst();
        RoutingService routing = instance.getContext().getService(RoutingService.NAME);

        // Use a shard ID that doesn't exist (beyond configured shards)
        int nonExistentShardId = 9999;
        Route route = routing.findRoute(ShardKind.REDIS, nonExistentShardId);

        assertNull(route);
    }

    @Test
    void shouldFindRouteForRedisShard() {
        KronotopTestInstance instance = getInstances().getFirst();
        RoutingService routing = instance.getContext().getService(RoutingService.NAME);

        Route route = routing.findRoute(ShardKind.REDIS, 0);

        assertNotNull(route);
        assertEquals(instance.getMember(), route.primary());
        assertNotNull(route.shardStatus());
        assertNotNull(route.standbys());
    }

    @Test
    void shouldFindRouteForBucketShard() {
        KronotopTestInstance instance = getInstances().getFirst();
        RoutingService routing = instance.getContext().getService(RoutingService.NAME);

        Route route = routing.findRoute(ShardKind.BUCKET, 0);

        assertNotNull(route);
        assertEquals(instance.getMember(), route.primary());
        assertNotNull(route.shardStatus());
        assertNotNull(route.standbys());
    }

    @Test
    void shouldRegisterAndExecuteHook() throws InterruptedException {
        KronotopTestInstance primary = getInstances().getFirst();
        KronotopTestInstance standby = addNewInstance();

        CountDownLatch latch = new CountDownLatch(1);
        RoutingService standbyRouting = standby.getContext().getService(RoutingService.NAME);
        standbyRouting.registerHook(RoutingEventKind.START_REPLICATION, (shardKind, shardId) -> {
            latch.countDown();
        });

        // Assign standby via primary's channel
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.route("SET", "STANDBY", "REDIS", 0, standby.getMember().getId()).encode(buf);

        Object msg = runCommand(primary.getChannel(), buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());

        // Wait for hook to be triggered on standby
        assertTrue(latch.await(15, TimeUnit.SECONDS), "Hook was not executed within timeout");
    }

    @Test
    void shouldRegisterMultipleHooksForSameEvent() throws InterruptedException {
        KronotopTestInstance primary = getInstances().getFirst();
        KronotopTestInstance standby = addNewInstance();

        CountDownLatch latch = new CountDownLatch(2);
        RoutingService standbyRouting = standby.getContext().getService(RoutingService.NAME);

        // Register two hooks for the same event
        standbyRouting.registerHook(RoutingEventKind.START_REPLICATION, (shardKind, shardId) -> {
            latch.countDown();
        });
        standbyRouting.registerHook(RoutingEventKind.START_REPLICATION, (shardKind, shardId) -> {
            latch.countDown();
        });

        // Assign standby via primary's channel
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.route("SET", "STANDBY", "REDIS", 0, standby.getMember().getId()).encode(buf);

        Object msg = runCommand(primary.getChannel(), buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());

        // Wait for both hooks to be triggered
        assertTrue(latch.await(15, TimeUnit.SECONDS), "Both hooks were not executed within timeout");
    }

    @Test
    void shouldNotFailWhenHookThrowsException() throws InterruptedException {
        KronotopTestInstance primary = getInstances().getFirst();
        KronotopTestInstance standby = addNewInstance();

        CountDownLatch latch = new CountDownLatch(1);
        RoutingService standbyRouting = standby.getContext().getService(RoutingService.NAME);

        // The first hook throws an exception
        standbyRouting.registerHook(RoutingEventKind.START_REPLICATION, (shardKind, shardId) -> {
            throw new RuntimeException("Test exception");
        });

        // The second hook should still execute
        standbyRouting.registerHook(RoutingEventKind.START_REPLICATION, (shardKind, shardId) -> {
            latch.countDown();
        });

        // Assign standby via the primary channel
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.route("SET", "STANDBY", "REDIS", 0, standby.getMember().getId()).encode(buf);

        Object msg = runCommand(primary.getChannel(), buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());

        // The second hook should still be triggered despite the first hook throwing
        assertTrue(latch.await(15, TimeUnit.SECONDS), "Second hook was not executed after first hook threw exception");
    }

    @Test
    void shouldTriggerStartReplicationWhenStandbyAssigned() throws InterruptedException {
        KronotopTestInstance primary = getInstances().getFirst();
        KronotopTestInstance standby = addNewInstance();

        int targetShardId = 0;
        CountDownLatch latch = new CountDownLatch(1);
        RoutingService standbyRouting = standby.getContext().getService(RoutingService.NAME);

        standbyRouting.registerHook(RoutingEventKind.START_REPLICATION, (shardKind, shardId) -> {
            assertEquals(ShardKind.REDIS, shardKind);
            assertEquals(targetShardId, shardId);
            latch.countDown();
        });

        // Assign standby
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.route("SET", "STANDBY", "REDIS", targetShardId, standby.getMember().getId()).encode(buf);

        Object msg = runCommand(primary.getChannel(), buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());

        assertTrue(latch.await(15, TimeUnit.SECONDS), "START_REPLICATION hook was not triggered");
    }

    @Test
    void shouldTriggerStopReplicationWhenStandbyRemoved() throws InterruptedException {
        KronotopTestInstance primary = getInstances().getFirst();
        KronotopTestInstance standby = addNewInstance();

        int targetShardId = 0;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch stopLatch = new CountDownLatch(1);
        RoutingService standbyRouting = standby.getContext().getService(RoutingService.NAME);

        standbyRouting.registerHook(RoutingEventKind.START_REPLICATION, (shardKind, shardId) -> {
            startLatch.countDown();
        });

        standbyRouting.registerHook(RoutingEventKind.STOP_REPLICATION, (shardKind, shardId) -> {
            assertEquals(ShardKind.REDIS, shardKind);
            assertEquals(targetShardId, shardId);
            stopLatch.countDown();
        });

        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        // First, assign standby
        ByteBuf setBuf = Unpooled.buffer();
        cmd.route("SET", "STANDBY", "REDIS", targetShardId, standby.getMember().getId()).encode(setBuf);
        Object setMsg = runCommand(primary.getChannel(), setBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, setMsg);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) setMsg).content());

        assertTrue(startLatch.await(15, TimeUnit.SECONDS), "START_REPLICATION hook was not triggered");

        // Now, remove standby
        ByteBuf unsetBuf = Unpooled.buffer();
        cmd.route("UNSET", "STANDBY", "REDIS", targetShardId, standby.getMember().getId()).encode(unsetBuf);
        Object unsetMsg = runCommand(primary.getChannel(), unsetBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, unsetMsg);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) unsetMsg).content());

        assertTrue(stopLatch.await(15, TimeUnit.SECONDS), "STOP_REPLICATION hook was not triggered");
    }

    @Test
    void shouldLoadRoutingTableWithStandbys() {
        KronotopTestInstance primary = getInstances().getFirst();
        KronotopTestInstance standby = addNewInstance();

        int targetShardId = 0;

        // Assign standby
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.route("SET", "STANDBY", "REDIS", targetShardId, standby.getMember().getId()).encode(buf);
        Object msg = runCommand(primary.getChannel(), buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());

        // Wait for the routing table to be updated on primary
        RoutingService primaryRouting = primary.getContext().getService(RoutingService.NAME);
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            Route route = primaryRouting.findRoute(ShardKind.REDIS, targetShardId);
            return route != null && route.standbys().contains(standby.getMember());
        });

        Route route = primaryRouting.findRoute(ShardKind.REDIS, targetShardId);
        assertNotNull(route);
        assertEquals(primary.getMember(), route.primary());
        assertTrue(route.standbys().contains(standby.getMember()));
    }

    @Test
    void shouldLoadCorrectShardStatus() {
        KronotopTestInstance instance = getInstances().getFirst();
        RoutingService routing = instance.getContext().getService(RoutingService.NAME);

        int targetShardId = 0;

        // Initially shards are READWRITE
        Route initialRoute = routing.findRoute(ShardKind.REDIS, targetShardId);
        assertNotNull(initialRoute);
        assertEquals(ShardStatus.READWRITE, initialRoute.shardStatus());

        // Change shard status to READONLY
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus("REDIS", targetShardId, ShardStatus.READONLY.name()).encode(buf);
        Object msg = runCommand(instance.getChannel(), buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());

        // Wait for routing table to be updated
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            Route route = routing.findRoute(ShardKind.REDIS, targetShardId);
            return route != null && route.shardStatus() == ShardStatus.READONLY;
        });

        Route updatedRoute = routing.findRoute(ShardKind.REDIS, targetShardId);
        assertNotNull(updatedRoute);
        assertEquals(ShardStatus.READONLY, updatedRoute.shardStatus());
    }

    @Test
    void shouldStartWithClusterInitialized() {
        KronotopTestInstance instance = getInstances().getFirst();
        RoutingService routing = instance.getContext().getService(RoutingService.NAME);

        // Verify cluster is initialized
        Boolean clusterInitialized = instance.getContext().getMemberAttributes()
                .attr(MemberAttributes.CLUSTER_INITIALIZED).get();
        assertTrue(clusterInitialized);

        // Verify routing table is loaded with routes for all configured shards
        int redisShards = instance.getContext().getConfig().getInt("redis.shards");
        for (int shardId = 0; shardId < redisShards; shardId++) {
            Route route = routing.findRoute(ShardKind.REDIS, shardId);
            assertNotNull(route, "Route should be loaded for Redis shard " + shardId);
            assertEquals(instance.getMember(), route.primary());
        }

        int bucketShards = instance.getContext().getConfig().getInt("bucket.shards");
        for (int shardId = 0; shardId < bucketShards; shardId++) {
            Route route = routing.findRoute(ShardKind.BUCKET, shardId);
            assertNotNull(route, "Route should be loaded for Bucket shard " + shardId);
            assertEquals(instance.getMember(), route.primary());
        }
    }
}
