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

import com.apple.foundationdb.Transaction;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.PrimaryIndex;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.time.Duration;

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
    void shouldDropIndex() {
        // Behavior: INDEX DROP returns OK and the index eventually disappears (NOSUCHINDEX).
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"username\": {\"name\": \"test-index\", \"bson_type\": \"string\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals(Response.OK, actualMessage.content());
        }

        // INDEX DROP rejects while the build task is still active, so poll until it accepts.
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexDrop(TEST_BUCKET, "test-index").encode(buf);
            Object msg = runCommand(channel, buf);
            return msg instanceof SimpleStringRedisMessage;
        });

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
        cmd.indexDrop(TEST_BUCKET, PrimaryIndex.NAME).encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("ERR Cannot drop the primary index", actualMessage.content());
    }

    @Test
    void shouldDropCompoundIndex() {
        // Behavior: INDEX DROP returns OK for a compound index and the index eventually disappears (NOSUCHINDEX).
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"$compound\": [{\"name\": \"test-compound-index\", \"fields\": [{\"selector\": \"age\", \"bson_type\": \"int32\"}, {\"selector\": \"name\", \"bson_type\": \"string\"}]}]}").encode(buf);
            Object msg = runCommand(channel, buf);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals(Response.OK, actualMessage.content());
        }

        // INDEX DROP rejects while the build task is still active, so poll until it accepts.
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexDrop(TEST_BUCKET, "test-compound-index").encode(buf);
            Object msg = runCommand(channel, buf);
            return msg instanceof SimpleStringRedisMessage;
        });

        await().atMost(Duration.ofSeconds(15)).until(() -> {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexDescribe(TEST_BUCKET, "test-compound-index").encode(buf);
            Object msg = runCommand(channel, buf);
            assertNotNull(msg);
            if (!(msg instanceof ErrorRedisMessage actualMessage)) {
                return false;
            }
            return actualMessage.content().equals("NOSUCHINDEX No such index: 'test-compound-index'");
        });
    }

    @Test
    void shouldThrowBucketBeingRemovedExceptionWhenDroppingIndexOnRemovedBucket() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        // First, create the bucket by creating an index
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"username\": {\"name\": \"test-index\", \"bson_type\": \"string\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals(Response.OK, actualMessage.content());
        }

        // Get the bucket metadata and mark it as removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            TransactionalContext tx = new TransactionalContext(context, tr);
            BucketMetadataUtil.setRemoved(tx, metadata);
            tr.commit().join();
        }

        // Flush the bucket metadata cache so open reads the dropped status
        Runnable cleanup = context.getBucketMetadataCache().createEvictionWorker(context::now, 0);
        cleanup.run();

        // Wait until the cache entry is actually evicted
        await().atMost(Duration.ofSeconds(5)).until(() ->
                context.getBucketMetadataCache().get(TEST_NAMESPACE, TEST_BUCKET) == null
        );

        // Try to drop the index on the dropped bucket
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexDrop(TEST_BUCKET, "test-index").encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
            assertEquals("BUCKETBEINGREMOVED Bucket 'test-bucket' is being removed", errorMessage.content());
        }
    }
}