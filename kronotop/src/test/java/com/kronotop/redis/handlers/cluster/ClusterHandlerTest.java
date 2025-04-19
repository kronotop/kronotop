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

package com.kronotop.redis.handlers.cluster;

import com.kronotop.BaseTest;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.RoutingService;
import com.kronotop.commandbuilder.redis.RedisCommandBuilder;
import com.kronotop.redis.handlers.BaseRedisHandlerTest;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.typesafe.config.Config;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class ClusterHandlerTest extends BaseRedisHandlerTest {
    private KronotopTestInstance secondInstance;

    @AfterEach
    public void stopSecondInstance() {
        secondInstance.shutdown();
    }

    @BeforeEach
    public void startSecondInstance() throws UnknownHostException, InterruptedException {
        Config config = loadConfig("test.conf");
        secondInstance = new KronotopTestInstance(config);
        secondInstance.start();
    }

    private boolean isJoinCompleted(int numMembers) {
        // TODO: CLUSTER-REFACTOR
        RoutingService service = instance.getContext().getService(RoutingService.NAME);
        return service.isClusterInitialized();
    }

    @Test
    public void test_CLUSTER_NODES() {
        Map<Integer, KronotopTestInstance> instances = new HashMap<>();
        instances.put(0, instance);
        instances.put(1, secondInstance);

        await().atMost(5, TimeUnit.SECONDS).until(() -> isJoinCompleted(instances.size()));

        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.clusterNodes().encode(buf);

        Object msg = BaseTest.runCommand(channel, buf);
        assertInstanceOf(FullBulkStringRedisMessage.class, msg);
        FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) msg;
        String response = actualMessage.content().toString(CharsetUtil.US_ASCII);

        // 9069ca78e7450a285173431b3e52c5c25299e473 127.0.0.1:5585@5585,127.0.0.1 master - 0 1 17 connected 0-2338
        String[] lines = response.split("\n");
        for (String line : lines) {
            String[] member = line.split("\\s+");
            assertEquals(9, member.length);
            // TODO: This test should be improved
        }
    }

    @Test
    public void test_CLUSTER_SLOTS() {
        Map<Integer, KronotopTestInstance> instances = new HashMap<>();
        instances.put(0, instance);
        instances.put(1, secondInstance);

        await().atMost(5, TimeUnit.SECONDS).until(() -> isJoinCompleted(instances.size()));

        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.clusterSlots().encode(buf);

        Object msg = BaseTest.runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertFalse(actualMessage.children().isEmpty());
        for (RedisMessage child : actualMessage.children()) {
            assertInstanceOf(ArrayRedisMessage.class, child);
            ArrayRedisMessage slot = (ArrayRedisMessage) child;
            assertEquals(3, slot.children().size());
            RedisMessage master = slot.children().get(slot.children().size() - 1);
            assertInstanceOf(ArrayRedisMessage.class, master);
            ArrayRedisMessage slotMaster = (ArrayRedisMessage) master;
            assertFalse(slotMaster.children().isEmpty());
        }
    }

    @Test
    public void test_CLUSTER_MYID() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.clusterMyId().encode(buf);

        Object msg = BaseTest.runCommand(channel, buf);
        assertInstanceOf(FullBulkStringRedisMessage.class, msg);
        FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) msg;
        assertEquals(instance.getContext().getMember().getId(), actualMessage.content().toString(CharsetUtil.US_ASCII));
    }

    @Test
    public void test_CLUSTER_KEYSLOT() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.clusterKeyslot("somekey").encode(buf);

            Object msg = BaseTest.runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, msg);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) msg;
            assertEquals(11058, actualMessage.value());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.clusterKeyslot("foo{hash_tag}").encode(buf);

            Object msg = BaseTest.runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, msg);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) msg;
            assertEquals(2515, actualMessage.value());
        }
    }
}