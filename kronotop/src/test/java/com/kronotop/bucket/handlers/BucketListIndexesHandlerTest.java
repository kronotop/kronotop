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
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BucketListIndexesHandlerTest extends BaseBucketHandlerTest {

    @BeforeEach
    void beforeEach() {
        getBucketMetadata(BUCKET_NAME);
    }

    @Test
    void shouldReturnErrorIfBucketDoesNotExist() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.listIndexes("non-existing-bucket").encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("NOSUCHBUCKET No such bucket: 'non-existing-bucket'", actualMessage.content());
    }

    @Test
    void shouldListIndexes() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.createIndex(BUCKET_NAME, "{\"selector\": {\"bson_type\": \"int32\", \"sort_order\": \"asc\"}, \"username\": {\"bson_type\": \"string\", \"sort_order\": \"desc\"}}").encode(buf);
            runCommand(channel, buf);
        }

        List<String> expectedNames = List.of(
                "selector:_id.bsonType:BINARY.sortOrder:ASCENDING",
                "selector:selector.bsonType:INT32.sortOrder:ASCENDING",
                "selector:username.bsonType:STRING.sortOrder:DESCENDING"
        );

        ByteBuf buf = Unpooled.buffer();
        cmd.listIndexes(BUCKET_NAME).encode(buf);
        Object msg = runCommand(channel, buf);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals(3, actualMessage.children().size());

        List<String> names = new ArrayList<>();
        for (RedisMessage message : actualMessage.children()) {
            MapRedisMessage actual = (MapRedisMessage) message;
            actual.children().forEach((key1, value1) -> {
                SimpleStringRedisMessage key = (SimpleStringRedisMessage) key1;
                SimpleStringRedisMessage value = (SimpleStringRedisMessage) value1;
                if (key.content().equals("name")) {
                    names.add(value.content());
                } else {
                    fail("Unexpected key: " + key.content());
                }
            });
        }
        assertEquals(expectedNames, names);
    }
}
