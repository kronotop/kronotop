// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class BucketCreateIndexHandlerTest extends BaseIndexHandlerTest {

    @Test
    void shouldReturnErrorIfBucketDoesNotExist() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.createIndex("non-existing-bucket", "{\"username\": {\"bson_type\": \"string\", \"sort_order\": \"desc\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("NOSUCHBUCKET No such bucket: 'non-existing-bucket'", actualMessage.content());
    }

    @Test
    void shouldCreateIndexWithMultipleFields() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.createIndex(BUCKET_NAME, "{\"field\": {\"bson_type\": \"int32\", \"sort_order\": \"asc\"}, \"username\": {\"bson_type\": \"string\", \"sort_order\": \"desc\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    void invalidTypeShouldReturnError() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.createIndex(BUCKET_NAME, "{\"field\": {\"bson_type\": \"int322\", \"sort_order\": \"asc\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("ERR Unknown BSON type: int322", actualMessage.content());
    }

    @Test
    void invalidSortOrderShouldReturnError() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.createIndex(BUCKET_NAME, "{\"field\": {\"bson_type\": \"int32\", \"sort_order\": \"bsc\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("ERR Unknown SortOrder: bsc", actualMessage.content());
    }

    @Test
    void shouldCreateIndexForValidTypes() {
        String template = "{\"field\": {\"bson_type\": \"%s\", \"sort_order\": \"asc\"}}";
        List<String> validTypes = List.of("int32", "string", "double", "binary", "boolean", "datetime", "timestamp", "int64", "decimal128");
        for (String validType : validTypes) {
            BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            ByteBuf buf = Unpooled.buffer();
            String directive = String.format(template, validType);
            cmd.createIndex(BUCKET_NAME, directive).encode(buf);
            Object msg = runCommand(channel, buf);
            if (msg instanceof ErrorRedisMessage errorRedisMessage) {
                fail("For '" + directive + "', should not return error: " + errorRedisMessage.content());
            }
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals(Response.OK, actualMessage.content());
        }
    }

    @Test
    void shouldCreateIndexForValidSortOrder() {
        String template = "{\"field\": {\"name\": \"%s\", \"bson_type\": \"int32\", \"sort_order\": \"%s\"}}";
        List<String> validSortOrderKinds = List.of("asc", "desc", "ascending", "descending");
        for (String validSortOrderKind : validSortOrderKinds) {
            BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            ByteBuf buf = Unpooled.buffer();
            String directive = String.format(template, UUID.randomUUID(), validSortOrderKind);
            cmd.createIndex(BUCKET_NAME, directive).encode(buf);
            Object msg = runCommand(channel, buf);
            if (msg instanceof ErrorRedisMessage errorRedisMessage) {
                fail("For '" + directive + "', should not return error: " + errorRedisMessage.content());
            }
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals(Response.OK, actualMessage.content());
        }
    }

    @Test
    void shouldThrowAnErrorWhenIndexAlreadyExists() {
        String definition = "{\"field\": {\"bson_type\": \"int32\", \"sort_order\": \"asc\"}, \"username\": {\"bson_type\": \"string\", \"sort_order\": \"desc\"}}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.createIndex(BUCKET_NAME, definition).encode(buf);
            Object msg = runCommand(channel, buf);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals(Response.OK, actualMessage.content());
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.createIndex(BUCKET_NAME, definition).encode(buf);
            Object msg = runCommand(channel, buf);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals("ERR 'field:field.bsonType:INT32.sortOrder:ASCENDING' has already exist", actualMessage.content());
        }
    }

    @Test
    void invalidIndexDefinition() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.createIndex(BUCKET_NAME, "{\"some\": \"key\"}").encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("ERR Invalid index definition", actualMessage.content());
    }
}
