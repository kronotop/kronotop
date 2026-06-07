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

package com.kronotop.bucket.vector;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.bucket.index.NoSuchIndexException;
import com.kronotop.bucket.index.VectorIndex;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class VectorIndexDropCleanupTest extends BaseBucketHandlerTest {

    private void createBucketWithVectorIndex() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.indexes(
                "{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}"
        )).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
    }

    private BucketService getBucketService() {
        return context.getService(BucketService.NAME);
    }

    @Test
    void shouldDropVectorIndex() {
        // Behavior: BUCKET.INDEX DROP on a vector index marks it as DROPPED, and the index
        // eventually disappears from FDB. The in-memory registry entry is also removed.
        createBucketWithVectorIndex();

        BucketMetadata metadata;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
        }
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
        assertNotNull(vectorIndex);
        long indexId = vectorIndex.definition().id();
        String indexName = vectorIndex.definition().name();

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexDrop(TEST_BUCKET, indexName).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);

        // Wait for the index to be cleared from FDB
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                try {
                    IndexUtil.open(tr, metadata.subspace(), indexName);
                    return false;
                } catch (NoSuchIndexException exp) {
                    return true;
                }
            }
        });

        // Verify the in-memory registry entry is removed
        VectorGraphIndexGroup group = getBucketService().getVectorGraphRegistry().get(
                TEST_NAMESPACE, TEST_BUCKET, indexId
        );
        assertNull(group);
    }

    @Test
    void shouldCleanupDiskFilesOnVectorIndexDrop() {
        // Behavior: After dropping a vector index, the disk directory for the index is deleted.
        createBucketWithVectorIndex();

        BucketMetadata metadata;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
        }
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
        assertNotNull(vectorIndex);
        long indexId = vectorIndex.definition().id();
        String indexName = vectorIndex.definition().name();

        // Create the disk directory to simulate the flushed state
        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(
                        getBucketService().getBucketDataDir())
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(indexId));
        try {
            Files.createDirectories(indexDir);
            Files.writeString(indexDir.resolve("test.index"), "dummy");
        } catch (Exception e) {
            fail("Failed to create test directory: " + e.getMessage());
        }
        assertTrue(Files.exists(indexDir));

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexDrop(TEST_BUCKET, indexName).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);

        // Wait for the index directory to be deleted
        await().atMost(Duration.ofSeconds(15)).until(() -> !Files.exists(indexDir));
    }

    @Test
    void shouldHandleDropWhenNoRegistryEntry() {
        // Behavior: Dropping a vector index never loaded into the in-memory registry
        // does not crash. The FDB entries are still cleaned up.
        createBucketWithVectorIndex();

        BucketMetadata metadata;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
        }
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
        assertNotNull(vectorIndex);
        String indexName = vectorIndex.definition().name();

        // Ensure no registry entry exists
        VectorGraphIndexGroup group = getBucketService().getVectorGraphRegistry().get(
                TEST_NAMESPACE, TEST_BUCKET, vectorIndex.definition().id()
        );
        assertNull(group, "No documents inserted, so no registry entry should exist");

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexDrop(TEST_BUCKET, indexName).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);

        // Wait for the index to be cleared from FDB
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                try {
                    IndexUtil.open(tr, metadata.subspace(), indexName);
                    return false;
                } catch (NoSuchIndexException exp) {
                    return true;
                }
            }
        });
    }

    @Test
    void shouldHandleDropWhenNoDiskFiles() {
        // Behavior: Dropping a vector index never flushed to disk does not crash.
        // The FDB entries are still cleaned up normally.
        createBucketWithVectorIndex();

        BucketMetadata metadata;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
        }
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
        assertNotNull(vectorIndex);
        long indexId = vectorIndex.definition().id();
        String indexName = vectorIndex.definition().name();

        // Verify no disk directory exists
        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(
                        getBucketService().getBucketDataDir())
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(indexId));
        assertFalse(Files.exists(indexDir));

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexDrop(TEST_BUCKET, indexName).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);

        // Wait for the index to be cleared from FDB
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                try {
                    IndexUtil.open(tr, metadata.subspace(), indexName);
                    return false;
                } catch (NoSuchIndexException exp) {
                    return true;
                }
            }
        });
    }

    @Test
    void shouldReturnErrorWhenDroppingNonExistentIndexOnBucketWithVectorIndex() {
        // Behavior: BUCKET.INDEX DROP returns NOSUCHINDEX when the named index doesn't exist,
        // even when the bucket has a vector index.
        createBucketWithVectorIndex();

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexDrop(TEST_BUCKET, "nonexistent-index").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage error = (ErrorRedisMessage) msg;
        assertTrue(error.content().contains("NOSUCHINDEX"));
    }
}
