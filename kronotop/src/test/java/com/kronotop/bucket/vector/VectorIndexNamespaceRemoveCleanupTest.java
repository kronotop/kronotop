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
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.VectorIndex;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class VectorIndexNamespaceRemoveCleanupTest extends BaseBucketHandlerTest {

    private void createNamespace(String ns) {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceCreate(ns).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
    }

    private void useNamespace(String ns) {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceUse(ns).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
    }

    private void removeNamespace(String ns) {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceRemove(ns).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
    }

    private void createBucketWithVectorIndexInNamespace(String ns, String bucket) {
        useNamespace(ns);
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.create(bucket, BucketCreateArgs.Builder.indexes(
                "{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}"
        )).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
    }

    private void insertDocumentWithVectorInNamespace(String ns, String bucket, float[] vector) {
        useNamespace(ns);
        BsonDocument doc = new BsonDocument();
        doc.put("label", new BsonString("test"));
        BsonArray embedding = new BsonArray();
        for (float v : vector) {
            embedding.add(new BsonDouble(v));
        }
        doc.put("embedding", embedding);

        byte[] docBytes = BSONUtil.toBytes(doc);
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(bucket, docBytes).encode(buf);
        runCommand(channel, buf);
    }

    private BucketService getBucketService() {
        return context.getService(BucketService.NAME);
    }

    @Test
    void shouldCleanupChildNamespaceVectorRegistryOnParentRemove() {
        // Behavior: Removing parent namespace "a" cleans up in-memory vector index
        // registry entries for child namespace "a.b".
        String parent = UUID.randomUUID().toString();
        String child = parent + ".sub";

        createNamespace(child);
        createBucketWithVectorIndexInNamespace(child, TEST_BUCKET);
        insertDocumentWithVectorInNamespace(child, TEST_BUCKET, new float[]{1.0f, 0.0f, 0.0f});

        // Capture the index ID
        long indexId;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, child, TEST_BUCKET);
            VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
            assertNotNull(vectorIndex);
            indexId = vectorIndex.definition().id();
        }

        // Wait for the vector graph to appear in the registry
        VectorGraphIndexRegistry registry = getBucketService().getVectorGraphRegistry();
        await().atMost(Duration.ofSeconds(5)).until(() ->
                registry.get(child, TEST_BUCKET, indexId) != null
        );

        removeNamespace(parent);

        await().atMost(Duration.ofSeconds(15)).until(() ->
                registry.get(child, TEST_BUCKET, indexId) == null
        );
    }

    @Test
    void shouldCleanupChildNamespaceDiskFilesOnParentRemove() {
        // Behavior: Removing parent namespace "a" deletes vector index disk files
        // for child namespace "a.b" by iterating bucket UUIDs from the registry.
        String parent = UUID.randomUUID().toString();
        String child = parent + ".sub";

        createNamespace(child);
        createBucketWithVectorIndexInNamespace(child, TEST_BUCKET);
        insertDocumentWithVectorInNamespace(child, TEST_BUCKET, new float[]{1.0f, 0.0f, 0.0f});

        BucketMetadata metadata;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            metadata = BucketMetadataUtil.reload(context, tr, child, TEST_BUCKET);
        }

        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
        assertNotNull(vectorIndex);
        long indexId = vectorIndex.definition().id();

        // Wait for the vector graph to appear in the registry
        VectorGraphIndexRegistry registry = getBucketService().getVectorGraphRegistry();
        await().atMost(Duration.ofSeconds(5)).until(() ->
                registry.get(child, TEST_BUCKET, indexId) != null
        );

        // Create disk directory to simulate flushed state
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

        removeNamespace(parent);

        await().atMost(Duration.ofSeconds(15)).until(() -> !Files.exists(bucketVectorDir));
    }
}
