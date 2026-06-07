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

package com.kronotop.bucket.handlers;

import com.kronotop.*;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RouteKind;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.commands.KrAdminCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import com.kronotop.stash.StashService;
import com.typesafe.config.Config;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.Attribute;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Abstract base class for multi-node bucket tests. Provides a common cluster setup
 * with two nodes and shard routing configuration.
 */
abstract class BaseBucketMultiNodeTest extends BaseClusterTest {
    protected KronotopTestInstance node1;
    protected KronotopTestInstance node2;

    /**
     * Controls whether test instances start TCP servers. Override to return {@code true}
     * when tests need real network communication (e.g., SEGMENT.RANGE RPC).
     */
    protected boolean runWithTCPServer() {
        return false;
    }

    @Override
    @BeforeEach
    public void setup() {
        // Do not call super.setup() — we handle initialization manually.
        node1 = addNewInstanceWithoutInit();
        node2 = addNewInstanceWithoutInit();

        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        // Initialize the cluster on node1
        ByteBuf buf = Unpooled.buffer();
        cmd.initializeCluster().encode(buf);
        Object raw = runCommand(node1.getChannel(), buf);
        assertInstanceOf(SimpleStringRedisMessage.class, raw);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) raw).content());

        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            Attribute<Boolean> attr = node1.getContext().getMemberAttributes().attr(MemberAttributes.CLUSTER_INITIALIZED);
            return attr.get() != null && attr.get();
        });

        String node1Id = node1.getContext().getMember().getId();
        String node2Id = node2.getContext().getMember().getId();

        // Assign BUCKET shards 0-3 to node1, shards 4-6 to node2
        for (int shardId = 0; shardId < 4; shardId++) {
            setRoute(node1, ShardKind.BUCKET, shardId, node1Id);
        }
        for (int shardId = 4; shardId < 7; shardId++) {
            setRoute(node1, ShardKind.BUCKET, shardId, node2Id);
        }

        // Assign STASH shards 0-3 to node1, shards 4-6 to node2
        for (int shardId = 0; shardId < 4; shardId++) {
            setRoute(node1, ShardKind.STASH, shardId, node1Id);
        }
        for (int shardId = 4; shardId < 7; shardId++) {
            setRoute(node1, ShardKind.STASH, shardId, node2Id);
        }

        // Wait for owned shards to become operable
        await().atMost(5, TimeUnit.SECONDS).until(() -> areShardsOperable(node1, BucketService.NAME, List.of(0, 1, 2, 3)));
        await().atMost(5, TimeUnit.SECONDS).until(() -> areShardsOperable(node2, BucketService.NAME, List.of(4, 5, 6)));
        await().atMost(5, TimeUnit.SECONDS).until(() -> areShardsOperable(node1, StashService.NAME, List.of(0, 1, 2, 3)));
        await().atMost(5, TimeUnit.SECONDS).until(() -> areShardsOperable(node2, StashService.NAME, List.of(4, 5, 6)));

        // Set all shards to READWRITE
        setAllShardsReadWrite(node1, ShardKind.BUCKET);
        setAllShardsReadWrite(node1, ShardKind.STASH);

        List<Integer> bucketShardIds = node1.getContext().getShardRegistry().getShardIds(ShardKind.BUCKET);
        List<Integer> stashShardIds = node1.getContext().getShardRegistry().getShardIds(ShardKind.STASH);

        await().atMost(5, TimeUnit.SECONDS).until(() -> areShardsWritable(node1, ShardKind.BUCKET, bucketShardIds));
        await().atMost(5, TimeUnit.SECONDS).until(() -> areShardsWritable(node1, ShardKind.STASH, stashShardIds));
        await().atMost(5, TimeUnit.SECONDS).until(() -> areShardsWritable(node2, ShardKind.BUCKET, bucketShardIds));
        await().atMost(5, TimeUnit.SECONDS).until(() -> areShardsWritable(node2, ShardKind.STASH, stashShardIds));
    }

    @Override
    @AfterEach
    public void tearDown() {
        super.tearDown();
    }

    protected KronotopTestInstance addNewInstanceWithoutInit() {
        Config config = loadConfig("test.conf");
        KronotopTestInstance instance = new KronotopTestInstance(config, runWithTCPServer(), false);
        try {
            instance.start();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KronotopException(e);
        } catch (UnknownHostException e) {
            throw new KronotopException(e);
        }
        kronotopInstances.put(instance.getMember(), instance);
        return instance;
    }

    protected void setRoute(KronotopTestInstance instance, ShardKind shardKind, int shardId, String memberId) {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.route("SET", RouteKind.PRIMARY.name(), shardKind.name(), shardId, memberId).encode(buf);

        Object raw = runCommand(instance.getChannel(), buf);
        if (raw instanceof ErrorRedisMessage err) {
            fail(err.content());
        }
        assertInstanceOf(SimpleStringRedisMessage.class, raw);
    }

    protected void setAllShardsReadWrite(KronotopTestInstance instance, ShardKind shardKind) {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus(shardKind.name(), "READWRITE").encode(buf);

        Object raw = runCommand(instance.getChannel(), buf);
        if (raw instanceof ErrorRedisMessage err) {
            fail(err.content());
        }
        assertInstanceOf(SimpleStringRedisMessage.class, raw);
    }

    protected boolean areShardsOperable(KronotopTestInstance instance, String serviceName, List<Integer> shardIds) {
        ShardOwnerService<?> service = instance.getContext().getService(serviceName);
        for (int shardId : shardIds) {
            Object shard = service.getServiceContext().shards().get(shardId);
            if (shard == null) {
                return false;
            }
        }
        return true;
    }

    protected boolean areShardsWritable(KronotopTestInstance instance, ShardKind shardKind, List<Integer> shardIds) {
        RoutingService routing = instance.getContext().getService(RoutingService.NAME);
        for (int shardId : shardIds) {
            Route route = routing.findRoute(shardKind, shardId);
            if (route == null) {
                return false;
            }
            if (!route.shardStatus().equals(ShardStatus.READWRITE)) {
                return false;
            }
        }
        return true;
    }

    protected RedisMessage findInMapMessage(MapRedisMessage mapRedisMessage, String key) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : mapRedisMessage.children().entrySet()) {
            FullBulkStringRedisMessage keyMessage = (FullBulkStringRedisMessage) entry.getKey();
            if (keyMessage.content().toString(StandardCharsets.UTF_8).equals(key)) {
                return entry.getValue();
            }
        }
        return null;
    }

    protected List<ObjectId> extractObjectIds(Object response) {
        assertInstanceOf(MapRedisMessage.class, response);
        RedisMessage msg = findInMapMessage((MapRedisMessage) response, "object_ids");
        assertNotNull(msg);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        return TestUtil.extractObjectIds((ArrayRedisMessage) msg);
    }

    protected List<BsonDocument> extractEntries(Object response) {
        assertInstanceOf(MapRedisMessage.class, response);
        RedisMessage msg = findInMapMessage((MapRedisMessage) response, "entries");
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage array = (ArrayRedisMessage) msg;
        List<BsonDocument> result = new ArrayList<>();
        for (RedisMessage child : array.children()) {
            FullBulkStringRedisMessage bulk = (FullBulkStringRedisMessage) child;
            byte[] docBytes = ByteBufUtil.getBytes(bulk.content());
            result.add(BSONUtil.toBsonDocument(docBytes));
        }
        return result;
    }
}
