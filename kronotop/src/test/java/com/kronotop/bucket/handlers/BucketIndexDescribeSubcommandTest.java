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

import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BucketIndexDescribeSubcommandTest extends BaseIndexHandlerTest {

    @Test
    void shouldReturnErrorIfBucketDoesNotExist() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexDescribe("non-existing-bucket", "not-existing-index").encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("NOSUCHBUCKET No such bucket: 'non-existing-bucket'", actualMessage.content());
    }

    @Test
    void shouldReturnErrorIfIndexDoesNotExist() {
        getBucketMetadata(TEST_BUCKET); // creates the bucket with the default id index
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexDescribe(TEST_BUCKET, "not-existing-index").encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("NOSUCHINDEX No such index: 'not-existing-index'", actualMessage.content());
    }

    @Test
    void shouldDescribeIndex() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"username\": {\"bson_type\": \"string\"}}").encode(buf);
            runCommand(channel, buf);
        }

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        String indexName = "selector:username.bsonType:STRING";

        ByteBuf buf = Unpooled.buffer();
        cmd.indexDescribe(TEST_BUCKET, indexName).encode(buf);
        Object msg = runCommand(channel, buf);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertNotNull(actualMessage);

        Map<RedisMessage, RedisMessage> fields = actualMessage.children();
        for (Map.Entry<RedisMessage, RedisMessage> entry : fields.entrySet()) {
            SimpleStringRedisMessage key = (SimpleStringRedisMessage) entry.getKey();
            switch (key.content()) {
                case "id" -> {
                    IntegerRedisMessage value = (IntegerRedisMessage) entry.getValue();
                    Index index = metadata.indexes().getIndex("username", IndexSelectionPolicy.ALL);
                    assertEquals(index.definition().id(), value.value());
                }
                case "selector" -> {
                    SimpleStringRedisMessage value = (SimpleStringRedisMessage) entry.getValue();
                    assertEquals("username", value.content());
                }
                case "bson_type" -> {
                    SimpleStringRedisMessage value = (SimpleStringRedisMessage) entry.getValue();
                    assertEquals("STRING", value.content());
                }
                case "status" -> {
                    SimpleStringRedisMessage value = (SimpleStringRedisMessage) entry.getValue();
                    assertEquals(IndexStatus.WAITING.name(), value.content());
                }
                case "statistics" -> {
                    MapRedisMessage value = (MapRedisMessage) entry.getValue();
                    for (Map.Entry<RedisMessage, RedisMessage> statsEntry : value.children().entrySet()) {
                        SimpleStringRedisMessage statsKey = (SimpleStringRedisMessage) statsEntry.getKey();
                        if (statsKey.content().equals("cardinality")) {
                            IntegerRedisMessage cardinality = (IntegerRedisMessage) statsEntry.getValue();
                            assertEquals(0, cardinality.value());
                        }
                    }
                }
                default -> fail("Unexpected key: " + key.content());
            }
        }
    }
}