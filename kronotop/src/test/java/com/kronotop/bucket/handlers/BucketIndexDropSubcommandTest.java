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
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.maintenance.IndexTaskUtil;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class BucketIndexDropSubcommandTest extends BaseIndexHandlerTest {

    @Test
    void shouldReturnErrorIfBucketDoesNotExist() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexDrop("non-existing-bucket", "not-existing-index").encode(buf);
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
        cmd.indexDrop(TEST_BUCKET, "not-existing-index").encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("NOSUCHINDEX No such index: 'not-existing-index'", actualMessage.content());
    }

    @Test
    @Disabled("Flaky test")
    void shouldDropIndex() {
        // TODO: This test logic is error prone. Use awaitility to check the conditions continuously
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"username\": {\"name\": \"test-index\", \"bson_type\": \"string\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexDrop(TEST_BUCKET, "test-index").encode(buf);
            Object msg = runCommand(channel, buf);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            // Verify task was created
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                List<Versionstamp> taskIds = IndexTaskUtil.listTasks(tx, TEST_NAMESPACE, TEST_BUCKET, "test-index");
                // create-index + drop-index tasks will co-exists
                assertEquals(2, taskIds.size());
            }
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexDescribe(TEST_BUCKET, "test-index").encode(buf);
            Object msg = runCommand(channel, buf);
            MapRedisMessage actualMessage = (MapRedisMessage) msg;
            assertNotNull(actualMessage);
            boolean found = false;
            for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
                SimpleStringRedisMessage key = (SimpleStringRedisMessage) entry.getKey();
                if (key.content().equals("status")) {
                    found = true;
                    SimpleStringRedisMessage value = (SimpleStringRedisMessage) entry.getValue();
                    assertEquals(IndexStatus.DROPPED.name(), value.content());
                }
            }
            assertTrue(found, "No 'status' found in the result map");
        }

        await().atMost(Duration.ofSeconds(15)).until(() -> {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexDescribe(TEST_BUCKET, "test-index").encode(buf);
            Object msg = runCommand(channel, buf);
            assertNotNull(msg);
            if (!(msg instanceof ErrorRedisMessage actualMessage)) {
                return false;
            }
            return actualMessage.content().equals("NOSUCHINDEX No such index: 'test-index'");
        });
    }

    @Test
    void shouldNotDropDefaultIdIndex() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexDrop(TEST_BUCKET, DefaultIndexDefinition.ID.name()).encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("ERR Cannot drop the primary index", actualMessage.content());
    }
}