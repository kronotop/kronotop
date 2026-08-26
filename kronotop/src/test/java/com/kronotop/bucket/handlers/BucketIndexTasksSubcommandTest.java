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

import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BucketIndexTasksSubcommandTest extends BaseIndexHandlerTest {

    @Test
    void shouldReturnTasksForNewlyCreatedIndex() {
        // Behavior: On a RESP3 session, after creating a secondary index, BUCKET.INDEX TASKS
        // returns a map with at least one task entry. Each task has a kind, status, and
        // error field. BUILD tasks additionally have cursor, lower, and upper fields.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Create an index which triggers tasks
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"username\": {\"bson_type\": \"string\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }

        // Query tasks for the index
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexTasks(TEST_BUCKET, "selector:username.bsonType:STRING").encode(buf);
            Object msg = runCommand(channel, buf);
            if (msg instanceof ErrorRedisMessage errorMsg) {
                fail("Unexpected error: " + errorMsg.content());
            }
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage tasksMap = (MapRedisMessage) msg;
            assertFalse(tasksMap.children().isEmpty(), "Expected at least one task");

            for (Map.Entry<RedisMessage, RedisMessage> entry : tasksMap.children().entrySet()) {
                assertInstanceOf(MapRedisMessage.class, entry.getValue());
                MapRedisMessage taskDetails = (MapRedisMessage) entry.getValue();

                // All task kinds have these fields
                RedisMessage kindMsg = findInMapMessage(taskDetails, "kind");
                assertNotNull(kindMsg);
                assertNotNull(findInMapMessage(taskDetails, "status"));
                assertNotNull(findInMapMessage(taskDetails, "error"));

                String kind = ((FullBulkStringRedisMessage) kindMsg).content().toString(StandardCharsets.UTF_8);
                if (kind.equals("BUILD")) {
                    assertNotNull(findInMapMessage(taskDetails, "cursor"));
                    assertNotNull(findInMapMessage(taskDetails, "lower"));
                    assertNotNull(findInMapMessage(taskDetails, "upper"));
                }
            }
        }
    }

    @Test
    void shouldReturnEmptyMapForPrimaryIndex() {
        // Behavior: The primary index has no background maintenance tasks, so on a RESP3
        // session BUCKET.INDEX TASKS returns an empty map.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(cmd, RESPVersion.RESP3);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexTasks(TEST_BUCKET, "primary-index").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage tasksMap = (MapRedisMessage) msg;
        assertTrue(tasksMap.children().isEmpty(), "Primary index should have no tasks");
    }

    /**
     * Reads a flattened key/value list into a map. Keys are bulk strings.
     */
    private Map<String, RedisMessage> pairsToMap(List<RedisMessage> children) {
        assertEquals(0, children.size() % 2, "flattened list must hold key/value pairs");
        Map<String, RedisMessage> result = new LinkedHashMap<>();
        for (int index = 0; index < children.size(); index += 2) {
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) children.get(index);
            result.put(key.content().toString(StandardCharsets.UTF_8), children.get(index + 1));
        }
        return result;
    }

    @Test
    void shouldReturnArrayWhenRESP2() {
        // Behavior: RESP2 has no map type, so BUCKET.INDEX TASKS returns a flat array of
        // task id and field list.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"username\": {\"bson_type\": \"string\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }

        ByteBuf buf = Unpooled.buffer();
        cmd.indexTasks(TEST_BUCKET, "selector:username.bsonType:STRING").encode(buf);
        Object msg = runCommand(channel, buf);
        if (msg instanceof ErrorRedisMessage errorMsg) {
            fail("Unexpected error: " + errorMsg.content());
        }
        assertInstanceOf(ArrayRedisMessage.class, msg);

        Map<String, RedisMessage> tasks = pairsToMap(((ArrayRedisMessage) msg).children());
        assertFalse(tasks.isEmpty(), "Expected at least one task");

        for (RedisMessage value : tasks.values()) {
            assertInstanceOf(ArrayRedisMessage.class, value);
            Map<String, RedisMessage> details = pairsToMap(((ArrayRedisMessage) value).children());
            assertNotNull(details.get("kind"));
            assertNotNull(details.get("status"));
            assertNotNull(details.get("error"));

            String kind = ((FullBulkStringRedisMessage) details.get("kind")).content().toString(StandardCharsets.UTF_8);
            if (kind.equals("BUILD")) {
                assertNotNull(details.get("cursor"));
                assertNotNull(details.get("lower"));
                assertNotNull(details.get("upper"));
            }
        }
    }

    @Test
    void shouldReturnEmptyArrayForPrimaryIndexWhenRESP2() {
        // Behavior: The primary index has no background maintenance tasks, so on a RESP2
        // session BUCKET.INDEX TASKS returns an empty array.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexTasks(TEST_BUCKET, "primary-index").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        assertTrue(((ArrayRedisMessage) msg).children().isEmpty(), "Primary index should have no tasks");
    }
}
