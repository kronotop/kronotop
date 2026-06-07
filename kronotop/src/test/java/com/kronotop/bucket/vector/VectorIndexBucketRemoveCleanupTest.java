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
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.VectorIndex;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class VectorIndexBucketRemoveCleanupTest extends BaseVectorHookIntegrationTest {

    private BucketMetadata getMetadata() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
        }
    }

    private void removeBucket() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.remove(TEST_BUCKET).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
    }

    @Test
    void shouldCleanupVectorRegistryOnBucketRemove() throws IOException {
        // Behavior: When a bucket with a vector index is removed, the in-memory
        // VectorGraphIndexRegistry entries for that bucket are cleaned up.
        createBucketWithVectorIndex();
        insertDocumentWithVector(new float[]{1.0f, 0.0f, 0.0f}, "test");
        try (OnHeapVectorGraphIndex ignored = awaitVectorGraph(TEST_BUCKET, "embedding", 1)) {

            // Capture index ID before remove
            long indexId;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
                VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
                assertNotNull(vectorIndex);
                indexId = vectorIndex.definition().id();
            }

            removeBucket();

            VectorGraphIndexRegistry registry = getBucketService().getVectorGraphRegistry();
            await().atMost(Duration.ofSeconds(15)).until(() ->
                    registry.get(TEST_NAMESPACE, TEST_BUCKET, indexId) == null
            );
        }
    }

    @Test
    void shouldCleanupVectorDiskFilesOnBucketRemove() {
        // Behavior: When a bucket with a vector index is removed, the disk directory
        // containing vector index files for that bucket is deleted.
        createBucketWithVectorIndex();
        BucketMetadata metadata = getMetadata();

        // Create the disk directory to simulate the flushed state
        Path bucketVectorDir = VectorGraphIndexGroup.resolveVectorDir(
                        getBucketService().getBucketDataDir())
                .resolve(metadata.uuid().toString());
        try {
            Files.createDirectories(bucketVectorDir);
            Files.writeString(bucketVectorDir.resolve("test.index"), "dummy");
        } catch (Exception e) {
            fail("Failed to create test directory: " + e.getMessage());
        }
        assertTrue(Files.exists(bucketVectorDir));

        removeBucket();

        await().atMost(Duration.ofSeconds(15)).until(() -> !Files.exists(bucketVectorDir));
    }
}
