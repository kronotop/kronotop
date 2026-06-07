/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.index.statistics.HistogramBucket;
import com.kronotop.bucket.index.statistics.HistogramCodec;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.transaction.TransactionUtil;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;


class BucketIndexAnalyzeSubcommandTest extends BaseIndexHandlerTest {

    @Test
    void shouldReturnNoSuchBucketErrorIfItDoesNotExist() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexAnalyze("non-existing-bucket", "non-existing-index").encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("NOSUCHBUCKET No such bucket: 'non-existing-bucket'", actualMessage.content());
    }

    @Test
    void shouldReturnNoSuchIndexErrorIfItDoesNotExist() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"username\": {\"bson_type\": \"string\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexAnalyze(TEST_BUCKET, "non-existing-index").encode(buf);
            Object msg = runCommand(channel, buf);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals("NOSUCHINDEX No such index: 'non-existing-index'", actualMessage.content());
        }
    }

    @Test
    void shouldBuildEmptyHistogram() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"username\": {\"name\": \"test\", \"bson_type\": \"string\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            // Analyze only works with the indexes in READY status.
            BucketMetadata metadata = TransactionUtil.execute(context,
                    tr -> BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET)
            );
            Index index = metadata.indexes().getIndex("username", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(index.subspace());
        }

        {
            // Retry on transient FDB transaction conflicts.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                ByteBuf buf = Unpooled.buffer();
                cmd.indexAnalyze(TEST_BUCKET, "test").encode(buf);
                Object msg = runCommand(channel, buf);
                // The background IndexBoundaryRoutine may have already scheduled an analyze task.
                if (msg instanceof ErrorRedisMessage errorMessage) {
                    assertFalse(errorMessage.content().startsWith("NOT_COMMITTED"),
                            "Transient FDB conflict, retrying");
                    assertEquals("An analyze task has already exist", errorMessage.content());
                } else {
                    SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
                    assertNotNull(actualMessage);
                    assertEquals(Response.OK, actualMessage.content());
                }
            });
        }

        await().atMost(Duration.ofSeconds(15)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
                Index index = metadata.indexes().getIndex("username", IndexSelectionPolicy.ALL);
                SingleFieldIndexDefinition definition = SingleFieldIndexUtil.loadIndexDefinition(tr, index.subspace());
                byte[] key = IndexUtil.histogramKey(metadata.subspace(), definition.id());
                byte[] value = tr.get(key).join();
                if (value == null) {
                    return false;
                }
                List<HistogramBucket> histogram = HistogramCodec.decode(value);
                return histogram.isEmpty();
            }
        });
    }

    @Test
    void shouldThrowBucketBeingRemovedExceptionWhenAnalyzingIndexOnRemovedBucket() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        // First, create the bucket by creating an index
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"username\": {\"name\": \"test\", \"bson_type\": \"string\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals(Response.OK, actualMessage.content());
        }

        // Wait for the index to be ready before testing bucket removal
        {
            BucketMetadata metadata = TransactionUtil.execute(context,
                    tr -> BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET)
            );
            Index index = metadata.indexes().getIndex("username", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(index.subspace());
        }

        // Get the bucket metadata and mark it as removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            TransactionalContext tx = new TransactionalContext(context, tr);
            BucketMetadataUtil.setRemoved(tx, metadata);
            tr.commit().join();
        }

        // Invalidate the bucket metadata cache so open reads the dropped status
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, TEST_BUCKET);

        // Try to analyze the index on the dropped bucket.
        // A background routine holding a pre-removal snapshot may re-cache non-removed metadata
        // after the invalidate above, so re-invalidate and retry until the removal guard is seen.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, TEST_BUCKET);
            ByteBuf buf = Unpooled.buffer();
            cmd.indexAnalyze(TEST_BUCKET, "test").encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
            assertFalse(errorMessage.content().startsWith("NOT_COMMITTED"),
                    "Transient FDB conflict, retrying");
            assertEquals("BUCKETBEINGREMOVED Bucket 'test-bucket' is being removed", errorMessage.content());
        });
    }
}
