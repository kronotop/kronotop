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

package com.kronotop.bucket.handlers;

import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.commandbuilder.kronotop.BucketQueryArgs;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class BucketQueryHandlerTest extends BaseBucketHandlerTest {

    @Test
    void shouldDoPhysicalFullScanWithoutOperator() {
        Map<String, byte[]> expectedDocument = insertDocuments(List.of(DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(BUCKET_NAME, "{}", BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertEquals(expectedDocument.size(), actualMessage.children().size());
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            // Check key
            SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
            String id = keyMessage.content();
            assertNotNull(id);
            assertNotNull(expectedDocument.get(id));

            // Check value
            FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) entry.getValue();
            assertArrayEquals(expectedDocument.get(id), ByteBufUtil.getBytes(value.content()));
        }
    }

    @Test
    void shouldDoPhysicalFullScanWithoutOperator_RESP2() {
        Map<String, byte[]> expectedDocument = insertDocuments(List.of(DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP2);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(BUCKET_NAME, "{}", BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        int index = 0;
        String latestId = "";
        for (RedisMessage entry : actualMessage.children()) {
            if (index % 2 == 0) {
                // Check key
                SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry;
                String id = keyMessage.content();
                assertNotNull(id);
                assertNotNull(expectedDocument.get(id));
                latestId = id;
            } else {
                // Check value
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) entry;
                assertArrayEquals(expectedDocument.get(latestId), ByteBufUtil.getBytes(value.content()));
            }
            index++;
        }
    }

    @Test
    void shouldDoPhysicalIndexScanWithSingleOperator_DefaultIDIndex_EQ() {
        Map<String, byte[]> expectedDocument = insertDocuments(makeDummyDocument(3));

        // Find the document in the middle
        String[] keys = expectedDocument.keySet().toArray(new String[0]);
        String expectedKey = keys[1];

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(BUCKET_NAME, String.format("{_id: {$eq: \"%s\"}}", expectedKey), BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertEquals(1, actualMessage.children().size());
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            assertInstanceOf(SimpleStringRedisMessage.class, entry.getKey());
            assertInstanceOf(FullBulkStringRedisMessage.class, entry.getValue());

            SimpleStringRedisMessage resultKey = (SimpleStringRedisMessage) entry.getKey();
            assertEquals(expectedKey, resultKey.content());

            FullBulkStringRedisMessage resultMessageValue = (FullBulkStringRedisMessage) entry.getValue();
            byte[] resultValue = ByteBufUtil.getBytes(resultMessageValue.content());
            assertArrayEquals(expectedDocument.get(expectedKey), resultValue);
        }
    }

    @Test
    void shouldDoPhysicalIndexScanWithSingleOperator_DefaultIDIndex_GTE() {
        Map<String, byte[]> expectedDocument = insertDocuments(makeDummyDocument(10));

        // Find the document in the middle
        String[] keys = expectedDocument.keySet().toArray(new String[0]);
        String key = keys[4];
        Set<String> excludedKeys = new HashSet<>(Arrays.asList(keys).subList(0, 4));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(BUCKET_NAME, String.format("{_id: {$gte: \"%s\"}}", key), BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertEquals(6, actualMessage.children().size());
        int index = 4;
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            assertInstanceOf(SimpleStringRedisMessage.class, entry.getKey());
            assertInstanceOf(FullBulkStringRedisMessage.class, entry.getValue());

            SimpleStringRedisMessage resultKey = (SimpleStringRedisMessage) entry.getKey();
            assertEquals(keys[index], resultKey.content());
            assertFalse(excludedKeys.contains(resultKey.content()));

            FullBulkStringRedisMessage resultMessageValue = (FullBulkStringRedisMessage) entry.getValue();
            byte[] resultValue = ByteBufUtil.getBytes(resultMessageValue.content());
            assertArrayEquals(expectedDocument.get(resultKey.content()), resultValue);
            index++;
        }
    }

    @Test
    void shouldDoPhysicalIndexScanWithSingleOperator_DefaultIDIndex_GT() {
        Map<String, byte[]> expectedDocument = insertDocuments(makeDummyDocument(10));

        String[] keys = expectedDocument.keySet().toArray(new String[0]);
        String key = keys[4];
        Set<String> excludedKeys = new HashSet<>(Arrays.asList(keys).subList(0, 5));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(BUCKET_NAME, String.format("{_id: {$gt: \"%s\"}}", key), BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertEquals(5, actualMessage.children().size());
        int index = 5;
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            assertInstanceOf(SimpleStringRedisMessage.class, entry.getKey());
            assertInstanceOf(FullBulkStringRedisMessage.class, entry.getValue());

            SimpleStringRedisMessage resultKey = (SimpleStringRedisMessage) entry.getKey();
            assertEquals(keys[index], resultKey.content());
            assertFalse(excludedKeys.contains(resultKey.content()));

            FullBulkStringRedisMessage resultMessageValue = (FullBulkStringRedisMessage) entry.getValue();
            byte[] resultValue = ByteBufUtil.getBytes(resultMessageValue.content());
            assertArrayEquals(expectedDocument.get(resultKey.content()), resultValue);
            index++;
        }
    }

    @Test
    void shouldDoPhysicalIndexScanWithSingleOperator_DefaultIDIndex_LT() {
        Map<String, byte[]> expectedDocument = insertDocuments(makeDummyDocument(10));

        // Find the document in the middle
        String[] keys = expectedDocument.keySet().toArray(new String[0]);
        String key = keys[4];
        Set<String> excludedKeys = new HashSet<>(Arrays.asList(keys).subList(4, 9));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        // Query should retrieve the first two documents we inserted
        cmd.query(BUCKET_NAME, String.format("{_id: {$lt: \"%s\"}}", key), BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;

        assertEquals(4, actualMessage.children().size());

        int index = 0;
        // The first two documents, the last one is excluded by the query.
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            assertInstanceOf(SimpleStringRedisMessage.class, entry.getKey());
            assertInstanceOf(FullBulkStringRedisMessage.class, entry.getValue());

            SimpleStringRedisMessage resultKey = (SimpleStringRedisMessage) entry.getKey();
            assertEquals(keys[index], resultKey.content());
            assertFalse(excludedKeys.contains(resultKey.content()));

            FullBulkStringRedisMessage resultMessageValue = (FullBulkStringRedisMessage) entry.getValue();
            byte[] resultValue = ByteBufUtil.getBytes(resultMessageValue.content());
            assertArrayEquals(expectedDocument.get(resultKey.content()), resultValue);
            index++;
        }
    }

    @Test
    void shouldDoPhysicalIndexScanWithSingleOperator_DefaultIDIndex_LTE() {
        Map<String, byte[]> expectedDocument = insertDocuments(makeDummyDocument(10));

        // Find the document in the middle
        String[] keys = expectedDocument.keySet().toArray(new String[0]);
        String key = keys[4];
        Set<String> excludedKeys = new HashSet<>(Arrays.asList(keys).subList(5, 9));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        // Query should retrieve the first two documents we inserted
        cmd.query(BUCKET_NAME, String.format("{_id: {$lte: \"%s\"}}", key), BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;

        assertEquals(5, actualMessage.children().size());
        int index = 0;
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            assertInstanceOf(SimpleStringRedisMessage.class, entry.getKey());
            assertInstanceOf(FullBulkStringRedisMessage.class, entry.getValue());

            SimpleStringRedisMessage resultKey = (SimpleStringRedisMessage) entry.getKey();
            assertEquals(keys[index], resultKey.content());
            assertFalse(excludedKeys.contains(resultKey.content()));

            FullBulkStringRedisMessage resultMessageValue = (FullBulkStringRedisMessage) entry.getValue();
            byte[] resultValue = ByteBufUtil.getBytes(resultMessageValue.content());
            assertArrayEquals(expectedDocument.get(resultKey.content()), resultValue);
            index++;
        }
    }
}