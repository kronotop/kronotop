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
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.bson.types.ObjectId;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

abstract class BaseVectorHookIntegrationTest extends BaseBucketHandlerTest {

    protected void createBucketWithVectorIndex() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.indexes(
                "{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}"
        )).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
    }

    protected BucketService getBucketService() {
        return context.getService(BucketService.NAME);
    }

    protected OnHeapVectorGraphIndex getVectorGraph(String bucket, String selector) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, bucket);
            VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector(selector, IndexSelectionPolicy.ALL);
            assertNotNull(vectorIndex, "Vector index should exist for selector: " + selector);
            VectorGraphIndexGroup group = getBucketService().getVectorGraphRegistry().get(
                    TEST_NAMESPACE, bucket, vectorIndex.definition().id()
            );
            if (group == null || group.getOnHeapIndexes().isEmpty()) {
                return null;
            }
            return group.getOnHeapIndexes().getFirst();
        }
    }

    protected OnHeapVectorGraphIndex awaitVectorGraph(String bucket, String selector, int expectedSize) {
        AtomicReference<OnHeapVectorGraphIndex> ref = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            OnHeapVectorGraphIndex graph = getVectorGraph(bucket, selector);
            assertNotNull(graph, "Vector graph should exist");
            assertTrue(graph.size() >= expectedSize, "Expected size >= " + expectedSize + " but was " + graph.size());
            ref.set(graph);
        });
        return ref.get();
    }

    protected ObjectId insertDocumentWithVector(float[] vector, String label) {
        BsonDocument doc = new BsonDocument();
        doc.put("label", new BsonString(label));
        BsonArray embedding = new BsonArray();
        for (float v : vector) {
            embedding.add(new BsonDouble(v));
        }
        doc.put("embedding", embedding);

        byte[] docBytes = BSONUtil.toBytes(doc);
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, docBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage array = (ArrayRedisMessage) msg;
        assertEquals(1, array.children().size());

        return com.kronotop.TestUtil.bulkStringToObjectId(
                (com.kronotop.server.resp3.FullBulkStringRedisMessage) array.children().getFirst()
        );
    }
}
