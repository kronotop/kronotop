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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.index.maintenance.IndexTaskUtil;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BucketCreateIndexHandlerTest extends BaseIndexHandlerTest {

    @Test
    void shouldCreateBucketIfItDoesNotExist() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.createIndex("non-existing-bucket", "{\"username\": {\"bson_type\": \"string\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    void shouldCreateIndexWithMultipleFields() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.createIndex(TEST_BUCKET, "{\"selector-one\": {\"bson_type\": \"int32\"}, \"selector-two\": {\"bson_type\": \"string\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    void invalidTypeShouldReturnError() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.createIndex(TEST_BUCKET, "{\"selector\": {\"bson_type\": \"int322\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("ERR Unknown BSON type: int322", actualMessage.content());
    }


    @Test
    void shouldCreateIndexForValidTypes() {
        String template = "{\"selector-%s\": {\"bson_type\": \"%s\"}}";
        List<String> validTypes = List.of("int32", "string", "double", "binary", "boolean", "datetime", "timestamp", "int64");
        // TODO: Enable this when we implement decimal128 indexes - "decimal128");
        for (String validType : validTypes) {
            BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            ByteBuf buf = Unpooled.buffer();
            String directive = String.format(template, validType, validType);
            cmd.createIndex(TEST_BUCKET, directive).encode(buf);
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
        String definition = "{\"selector\": {\"bson_type\": \"int32\"}, \"username\": {\"bson_type\": \"string\"}}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.createIndex(TEST_BUCKET, definition).encode(buf);
            Object msg = runCommand(channel, buf);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals(Response.OK, actualMessage.content());
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.createIndex(TEST_BUCKET, definition).encode(buf);
            Object msg = runCommand(channel, buf);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals("ERR 'selector:selector.bsonType:INT32' has already exist", actualMessage.content());
        }
    }

    @Test
    void invalidIndexDefinition() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.createIndex(TEST_BUCKET, "{\"some\": \"key\"}").encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("ERR Invalid index schema", actualMessage.content());
    }

    @Test
    void shouldCreateTaskForSecondaryIndex() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.createIndex(TEST_BUCKET, "{\"username\": {\"name\": \"test\", \"bson_type\": \"string\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals(Response.OK, actualMessage.content());

        // Verify task was created
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            List<Versionstamp> taskIds = IndexTaskUtil.listTasks(tx, TEST_NAMESPACE, TEST_BUCKET, "test");
            assertEquals(1, taskIds.size(), "Expected exactly one task to be created");
        }
    }
}
