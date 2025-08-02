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
            cmd.createIndex(BUCKET_NAME, "{\"field\": {\"bson_type\": \"int32\", \"sort_order\": \"asc\"}, \"username\": {\"bson_type\": \"string\", \"sort_order\": \"desc\"}}").encode(buf);
            runCommand(channel, buf);
        }

        List<String> expectedNames = List.of(
                "field:_id.bsonType:BINARY.sortOrder:ASCENDING",
                "field:field.bsonType:INT32.sortOrder:ASCENDING",
                "field:username.bsonType:STRING.sortOrder:DESCENDING"
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
