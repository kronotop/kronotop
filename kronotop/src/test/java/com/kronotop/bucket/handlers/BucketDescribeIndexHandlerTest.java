// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BucketDescribeIndexHandlerTest extends BaseIndexHandlerTest {

    @Test
    void shouldReturnErrorIfBucketDoesNotExist() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.describeIndex("non-existing-bucket", "not-existing-index").encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("NOSUCHBUCKET No such bucket: 'non-existing-bucket'", actualMessage.content());
    }

    @Test
    void shouldReturnErrorIfIndexDoesNotExist() {
        getBucketMetadata(BUCKET_NAME); // creates the bucket with the default id index
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.describeIndex(BUCKET_NAME, "not-existing-index").encode(buf);
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
            cmd.createIndex(BUCKET_NAME, "{\"username\": {\"bson_type\": \"string\", \"sort_order\": \"desc\"}}").encode(buf);
            runCommand(channel, buf);
        }

        String indexName = "field:username.bsonType:STRING.sortOrder:DESCENDING";

        ByteBuf buf = Unpooled.buffer();
        cmd.describeIndex(BUCKET_NAME, indexName).encode(buf);
        Object msg = runCommand(channel, buf);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertNotNull(actualMessage);

        Map<RedisMessage, RedisMessage> fields = actualMessage.children();
        for (Map.Entry<RedisMessage, RedisMessage> entry : fields.entrySet()) {
            SimpleStringRedisMessage key = (SimpleStringRedisMessage) entry.getKey();
            switch (key.content()) {
                case "field" -> {
                    SimpleStringRedisMessage value = (SimpleStringRedisMessage) entry.getValue();
                    assertEquals("username", value.content());
                }
                case "bson_type" -> {
                    SimpleStringRedisMessage value = (SimpleStringRedisMessage) entry.getValue();
                    assertEquals("STRING", value.content());
                }
                case "sort_order" -> {
                    SimpleStringRedisMessage value = (SimpleStringRedisMessage) entry.getValue();
                    assertEquals("DESCENDING", value.content());
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