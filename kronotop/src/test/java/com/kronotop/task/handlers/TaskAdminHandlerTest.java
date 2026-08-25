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

package com.kronotop.task.handlers;

import com.kronotop.BaseClusterTest;
import com.kronotop.commands.TaskAdminCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TaskAdminHandlerTest extends BaseClusterTest {
    private static final List<String> EXPECTED_KEYS = List.of("running", "finished", "started_at", "last_run");

    private EmbeddedChannel channel() {
        return getInstances().getFirst().getChannel();
    }

    private Object listTasks(EmbeddedChannel channel) {
        TaskAdminCommandBuilder<String, String> cmd = new TaskAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.list().encode(buf);
        return runCommand(channel, buf);
    }

    /**
     * Reads a flattened key/value list into a map. Keys are bulk strings.
     */
    private Map<String, RedisMessage> pairsToMap(List<RedisMessage> children) {
        assertEquals(0, children.size() % 2, "flattened list must hold key/value pairs");
        Map<String, RedisMessage> result = new LinkedHashMap<>();
        for (int index = 0; index < children.size(); index += 2) {
            result.put(stringValueOf((FullBulkStringRedisMessage) children.get(index)), children.get(index + 1));
        }
        return result;
    }

    @Test
    void shouldReturnMapWhenRESP3() {
        // Behavior: RESP3 clients get a map of task name to a nested map of task fields.
        EmbeddedChannel channel = channel();
        switchProtocol(channel, RESPVersion.RESP3);

        Object msg = listTasks(channel);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertFalse(actualMessage.children().isEmpty());
        actualMessage.children().forEach((name, properties) -> {
            assertInstanceOf(FullBulkStringRedisMessage.class, name);
            assertInstanceOf(MapRedisMessage.class, properties);

            MapRedisMessage task = (MapRedisMessage) properties;
            assertEquals(EXPECTED_KEYS.size(), task.children().size());

            Map<String, RedisMessage> fields = new LinkedHashMap<>();
            task.children().forEach((key, value) -> fields.put(stringValueOf((FullBulkStringRedisMessage) key), value));
            assertEquals(EXPECTED_KEYS, List.copyOf(fields.keySet()));

            assertInstanceOf(BooleanRedisMessage.class, fields.get("running"));
            assertInstanceOf(BooleanRedisMessage.class, fields.get("finished"));
            assertInstanceOf(IntegerRedisMessage.class, fields.get("started_at"));
            assertInstanceOf(IntegerRedisMessage.class, fields.get("last_run"));
        });
    }

    @Test
    void shouldReturnArrayWhenRESP2() {
        // Behavior: RESP2 has no map type, so the reply is a flat array of task name and field list.
        EmbeddedChannel channel = channel();
        switchProtocol(channel, RESPVersion.RESP2);

        Object msg = listTasks(channel);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertFalse(actualMessage.children().isEmpty());
        assertEquals(0, actualMessage.children().size() % 2);

        Map<String, RedisMessage> tasks = pairsToMap(actualMessage.children());
        tasks.forEach((name, properties) -> {
            assertInstanceOf(ArrayRedisMessage.class, properties);

            ArrayRedisMessage task = (ArrayRedisMessage) properties;
            assertEquals(EXPECTED_KEYS.size() * 2, task.children().size());

            Map<String, RedisMessage> fields = pairsToMap(task.children());
            assertEquals(EXPECTED_KEYS, List.copyOf(fields.keySet()));
        });
    }

    @Test
    void shouldRenderBooleanFieldsAsIntegersWhenRESP2() {
        // Behavior: RESP2 has no boolean type, so running and finished are sent as 1 or 0.
        EmbeddedChannel channel = channel();
        switchProtocol(channel, RESPVersion.RESP2);

        Object msg = listTasks(channel);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        Map<String, RedisMessage> tasks = pairsToMap(((ArrayRedisMessage) msg).children());
        assertFalse(tasks.isEmpty());
        tasks.forEach((name, properties) -> {
            Map<String, RedisMessage> fields = pairsToMap(((ArrayRedisMessage) properties).children());
            for (String key : List.of("running", "finished")) {
                RedisMessage value = fields.get(key);
                assertInstanceOf(IntegerRedisMessage.class, value);
                long flag = ((IntegerRedisMessage) value).value();
                assertTrue(flag == 0 || flag == 1, key + " must be 0 or 1");
            }
        });
    }
}
