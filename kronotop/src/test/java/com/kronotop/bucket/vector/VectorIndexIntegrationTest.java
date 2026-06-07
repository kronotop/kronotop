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
import com.kronotop.TestUtil;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.DistanceFunction;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.VectorIndex;
import com.kronotop.bucket.pipeline.DocumentLocation;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.commands.BucketVectorArgs;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import io.github.jbellis.jvector.graph.SearchResult;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.*;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class VectorIndexIntegrationTest extends BaseBucketHandlerTest {

    private void insertDocumentsWithLabelsAndVectors(String[] labels, float[][] vectors) {
        for (int i = 0; i < vectors.length; i++) {
            BsonDocument doc = new BsonDocument();
            doc.put("label", new BsonString(labels[i]));
            BsonArray embedding = new BsonArray();
            for (float v : vectors[i]) {
                embedding.add(new BsonDouble(v));
            }
            doc.put("embedding", embedding);

            byte[] docBytes = BSONUtil.toBytes(doc);
            BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, docBytes).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
        }
    }

    private void insertDocumentsWithVectors(float[][] vectors) {
        for (float[] vec : vectors) {
            BsonDocument doc = new BsonDocument();
            BsonArray embedding = new BsonArray();
            for (float v : vec) {
                embedding.add(new BsonDouble(v));
            }
            doc.put("embedding", embedding);

            byte[] docBytes = BSONUtil.toBytes(doc);
            BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, docBytes).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
        }
    }

    private RedisMessage findInMapMessage(MapRedisMessage mapRedisMessage, String key) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : mapRedisMessage.children().entrySet()) {
            FullBulkStringRedisMessage keyMessage = (FullBulkStringRedisMessage) entry.getKey();
            if (keyMessage.content().toString(StandardCharsets.UTF_8).equals(key)) {
                return entry.getValue();
            }
        }
        return null;
    }

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

    @Test
    void shouldInsertDocumentWithVectorField() {
        // Behavior: Inserting a document with a BsonArray of BsonDouble vector field into a bucket
        // that has a vector index succeeds, and the document can be queried back with the vector data intact.
        createBucketWithVectorIndex();

        BsonDocument doc = new BsonDocument();
        doc.put("label", new BsonString("test"));
        BsonArray embedding = new BsonArray();
        embedding.add(new BsonDouble(0.2));
        embedding.add(new BsonDouble(0.3));
        embedding.add(new BsonDouble(0.4));
        doc.put("embedding", embedding);

        byte[] docBytes = BSONUtil.toBytes(doc);

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, docBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(1, actualMessage.children().size());

        FullBulkStringRedisMessage message = (FullBulkStringRedisMessage) actualMessage.children().getFirst();
        ObjectId objectId = TestUtil.bulkStringToObjectId(message);
        assertNotNull(objectId);

        // Query back the document via RESP3
        switchProtocol(cmd, RESPVersion.RESP3);
        buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(buf);
        Object queryResponse = runCommand(channel, buf);

        List<BsonDocument> entries = extractEntries(queryResponse);
        assertEquals(1, entries.size());

        BsonDocument retrieved = entries.getFirst();
        assertEquals("test", retrieved.getString("label").getValue());
        assertEquals(BsonType.ARRAY, retrieved.get("embedding").getBsonType());

        // Verify vector data round-trips correctly
        BsonArray retrievedEmbedding = retrieved.getArray("embedding");
        assertEquals(3, retrievedEmbedding.size());
        assertEquals(0.2, retrievedEmbedding.get(0).asDouble().getValue(), 1e-6);
        assertEquals(0.3, retrievedEmbedding.get(1).asDouble().getValue(), 1e-6);
        assertEquals(0.4, retrievedEmbedding.get(2).asDouble().getValue(), 1e-6);

        // Verify vector index metadata exists in FDB
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            VectorIndex vectorIdx = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
            assertNotNull(vectorIdx, "Vector index should exist for 'embedding' field");
            assertEquals(3, vectorIdx.definition().dimensions());
            assertEquals(DistanceFunction.COSINE, vectorIdx.definition().distance());
        }
    }

    @Test
    void shouldRejectInsertWhenVectorFieldIsNotArray() {
        // Behavior: Inserting a document where the vector-indexed field is not an array is rejected with an error.
        createBucketWithVectorIndex();

        BsonDocument doc = new BsonDocument();
        doc.put("label", new BsonString("test"));
        doc.put("embedding", new BsonString("not-an-array"));

        byte[] docBytes = BSONUtil.toBytes(doc);

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, docBytes).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage error = (ErrorRedisMessage) response;
        assertTrue(error.content().contains("must be an array"));
    }

    @Test
    void shouldRejectInsertWhenVectorDimensionsMismatch() {
        // Behavior: Inserting a document where the vector array has the wrong number of dimensions is rejected.
        createBucketWithVectorIndex();

        BsonDocument doc = new BsonDocument();
        doc.put("label", new BsonString("test"));
        BsonArray embedding = new BsonArray();
        embedding.add(new BsonDouble(0.2));
        embedding.add(new BsonDouble(0.3));
        doc.put("embedding", embedding);

        byte[] docBytes = BSONUtil.toBytes(doc);

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, docBytes).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage error = (ErrorRedisMessage) response;
        assertTrue(error.content().contains("requires 3 dimensions but got 2"));
    }

    @Test
    void shouldRejectInsertWhenVectorElementsNotDouble() {
        // Behavior: Inserting a document where the vector array contains non-double elements is rejected.
        createBucketWithVectorIndex();

        BsonDocument doc = new BsonDocument();
        doc.put("label", new BsonString("test"));
        BsonArray embedding = new BsonArray();
        embedding.add(new BsonInt32(1));
        embedding.add(new BsonInt32(2));
        embedding.add(new BsonString("three"));
        doc.put("embedding", embedding);

        byte[] docBytes = BSONUtil.toBytes(doc);

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, docBytes).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage error = (ErrorRedisMessage) response;
        assertTrue(error.content().contains("must contain only double elements"));
    }

    private void insertDocumentWithVector() {
        BsonDocument doc = new BsonDocument();
        doc.put("label", new BsonString("test"));
        BsonArray embedding = new BsonArray();
        embedding.add(new BsonDouble(0.2));
        embedding.add(new BsonDouble(0.3));
        embedding.add(new BsonDouble(0.4));
        doc.put("embedding", embedding);

        byte[] docBytes = BSONUtil.toBytes(doc);

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, docBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
    }

    @Test
    void shouldRejectUpdateWhenVectorFieldSetToNonArray() {
        // Behavior: Updating the vector-indexed field to a non-array value via $set is rejected.
        createBucketWithVectorIndex();
        insertDocumentWithVector();

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"embedding\": \"not-an-array\"}}").encode(buf);
        Object response = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage error = (ErrorRedisMessage) response;
        assertTrue(error.content().contains("must be an array"));
    }

    @Test
    void shouldRejectUpdateWhenVectorDimensionsMismatch() {
        // Behavior: Updating the vector field to an array with the wrong dimension count is rejected.
        createBucketWithVectorIndex();
        insertDocumentWithVector();

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"embedding\": [0.1, 0.2]}}").encode(buf);
        Object response = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage error = (ErrorRedisMessage) response;
        assertTrue(error.content().contains("requires 3 dimensions but got 2"));
    }

    @Test
    void shouldRejectUpdateWhenVectorElementsNotDouble() {
        // Behavior: Updating the vector field to an array with non-double elements is rejected.
        createBucketWithVectorIndex();
        insertDocumentWithVector();

        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setOps = new BsonDocument();
        BsonArray badEmbedding = new BsonArray();
        badEmbedding.add(new BsonInt32(1));
        badEmbedding.add(new BsonInt32(2));
        badEmbedding.add(new BsonString("three"));
        setOps.put("embedding", badEmbedding);
        updateDoc.put("$set", setOps);
        byte[] updateBytes = BSONUtil.toBytes(updateDoc);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{}", updateBytes).encode(buf);
        Object response = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage error = (ErrorRedisMessage) response;
        assertTrue(error.content().contains("must contain only double elements"));
    }

    @Test
    void shouldRejectUpdateWhenVectorComponentOverflowsFloat() {
        // Behavior: Updating a vector field with a component that overflows the float range is rejected.
        createBucketWithVectorIndex();
        insertDocumentWithVector();

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setOps = new BsonDocument();
        BsonArray badEmbedding = new BsonArray();
        badEmbedding.add(new BsonDouble(1.0));
        badEmbedding.add(new BsonDouble(Double.MAX_VALUE));
        badEmbedding.add(new BsonDouble(0.5));
        setOps.put("embedding", badEmbedding);
        updateDoc.put("$set", setOps);
        byte[] updateBytes = BSONUtil.toBytes(updateDoc);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{}", updateBytes).encode(buf);
        Object response = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage error = (ErrorRedisMessage) response;
        assertTrue(error.content().contains("overflows float range"));
    }

    @Test
    void shouldUpdateDocumentWithValidVectorField() {
        // Behavior: Updating the vector field to a valid array of correct dimensions succeeds.
        createBucketWithVectorIndex();
        insertDocumentWithVector();

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"embedding\": [0.5, 0.6, 0.7]}}").encode(buf);
        Object response = runCommand(channel, buf);

        List<ObjectId> objectIds = extractObjectIds(response);
        assertEquals(1, objectIds.size());
    }

    @Test
    void shouldInsertDocumentWithMissingVectorField() {
        // Behavior: Inserting a document without the vector-indexed field succeeds (vector field is optional).
        createBucketWithVectorIndex();

        BsonDocument doc = new BsonDocument();
        doc.put("label", new BsonString("no-vector"));

        byte[] docBytes = BSONUtil.toBytes(doc);

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, docBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(1, actualMessage.children().size());
    }

    private BucketService getBucketService() {
        return context.getService(BucketService.NAME);
    }

    private OnHeapVectorGraphIndex getVectorGraph(String bucket, String selector) {
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

    @Test
    void shouldPopulateVectorGraphOnInsert() {
        // Behavior: After inserting a document with a vector field, the in-memory VectorGraph contains
        // the vector and it can be found via GraphSearcher.
        createBucketWithVectorIndex();

        BsonDocument doc = new BsonDocument();
        doc.put("label", new BsonString("test"));
        BsonArray embedding = new BsonArray();
        embedding.add(new BsonDouble(0.2));
        embedding.add(new BsonDouble(0.3));
        embedding.add(new BsonDouble(0.4));
        doc.put("embedding", embedding);

        byte[] docBytes = BSONUtil.toBytes(doc);
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, docBytes).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        OnHeapVectorGraphIndex graph = getVectorGraph(TEST_BUCKET, "embedding");
        assertNotNull(graph);
        assertEquals(1, graph.size());

        // Search the graph with the same vector
        SearchResult result = graph.search(new float[]{0.2f, 0.3f, 0.4f}, 1);

        assertEquals(1, result.getNodes().length);
        int ordinal = result.getNodes()[0].node;
        assertNotNull(graph.getMetadata().findDocumentLocation(ordinal));
    }

    @Test
    void shouldPopulateVectorGraphWithMultipleInserts() {
        // Behavior: After inserting multiple documents with vector fields, the VectorGraph contains
        // all vectors and a search returns the closest match.
        createBucketWithVectorIndex();

        float[][] vectors = {
                {0.1f, 0.2f, 0.3f},
                {0.4f, 0.5f, 0.6f},
                {0.7f, 0.8f, 0.9f}
        };

        for (float[] vec : vectors) {
            BsonDocument doc = new BsonDocument();
            BsonArray embedding = new BsonArray();
            for (float v : vec) {
                embedding.add(new BsonDouble(v));
            }
            doc.put("embedding", embedding);

            byte[] docBytes = BSONUtil.toBytes(doc);
            BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, docBytes).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
        }

        OnHeapVectorGraphIndex graph = getVectorGraph(TEST_BUCKET, "embedding");
        assertNotNull(graph);
        assertEquals(3, graph.size());

        // Search with the second vector, it should be found as top-1
        SearchResult result = graph.search(new float[]{0.4f, 0.5f, 0.6f}, 1);

        assertEquals(1, result.getNodes().length);
        assertTrue(result.getNodes()[0].score > 0);
    }

    @Test
    void shouldNotPopulateVectorGraphWhenVectorFieldMissing() {
        // Behavior: Inserting a document without the vector-indexed field does not create or populate
        // a VectorGraph in the registry.
        createBucketWithVectorIndex();

        BsonDocument doc = new BsonDocument();
        doc.put("label", new BsonString("no-vector"));

        byte[] docBytes = BSONUtil.toBytes(doc);
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, docBytes).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        OnHeapVectorGraphIndex graph = getVectorGraph(TEST_BUCKET, "embedding");
        assertTrue(graph == null || graph.size() == 0);
    }

    @Test
    void shouldSearchVectorsAndReturnResults() {
        // Behavior: BUCKET.VECTOR returns an array of {score, document} maps for matching vectors,
        // ordered by similarity score.
        createBucketWithVectorIndex();

        float[][] vectors = {
                {0.1f, 0.2f, 0.3f},
                {0.4f, 0.5f, 0.6f},
                {0.7f, 0.8f, 0.9f}
        };
        insertDocumentsWithVectors(vectors);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.4f, 0.5f, 0.6f}).encode(buf);
        Object response = runCommand(channel, buf);

        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(3, array.children().size());

        // Each child should be a map with score and document
        for (RedisMessage child : array.children()) {
            assertInstanceOf(MapRedisMessage.class, child);
            MapRedisMessage map = (MapRedisMessage) child;
            RedisMessage score = findInMapMessage(map, "score");
            assertNotNull(score);
            assertInstanceOf(DoubleRedisMessage.class, score);
            assertTrue(((DoubleRedisMessage) score).value() > 0);

            RedisMessage document = findInMapMessage(map, "entry");
            assertNotNull(document);
            assertInstanceOf(FullBulkStringRedisMessage.class, document);
        }
    }

    @Test
    void shouldSearchVectorsWithTop() {
        // Behavior: BUCKET.VECTOR with TOP restricts the number of returned results.
        createBucketWithVectorIndex();

        float[][] vectors = {
                {0.1f, 0.2f, 0.3f},
                {0.4f, 0.5f, 0.6f},
                {0.7f, 0.8f, 0.9f}
        };
        insertDocumentsWithVectors(vectors);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.4f, 0.5f, 0.6f}, BucketVectorArgs.Builder.top(1)).encode(buf);
        Object response = runCommand(channel, buf);

        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(1, array.children().size());
    }

    @Test
    void shouldReturnEmptyArrayWhenNoVectorsInserted() {
        // Behavior: BUCKET.VECTOR returns an empty array when the vector graph has no entries.
        createBucketWithVectorIndex();

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.1f, 0.2f, 0.3f}).encode(buf);
        Object response = runCommand(channel, buf);

        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertTrue(array.children().isEmpty());
    }

    @Test
    void shouldRejectVectorSearchWithWrongDimensions() {
        // Behavior: BUCKET.VECTOR rejects a query vector with dimensions that don't match the index definition.
        createBucketWithVectorIndex();
        insertDocumentWithVector();

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.1f, 0.2f}).encode(buf);
        Object response = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage error = (ErrorRedisMessage) response;
        assertTrue(error.content().contains("requires 3 dimensions"));
    }

    @Test
    void shouldRejectVectorSearchWithInvalidSelector() {
        // Behavior: BUCKET.VECTOR rejects a search when no vector index exists for the given selector.
        createBucketWithVectorIndex();

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "nonexistent", new float[]{0.1f, 0.2f, 0.3f}).encode(buf);
        Object response = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage error = (ErrorRedisMessage) response;
        assertTrue(error.content().contains("No vector index found"));
    }

    @Test
    void shouldRemoveOldVectorFromGraphAfterUpdate() {
        // Behavior: After updating a document's vector field, the old vector's ordinal is removed
        // from graph metadata and searching for the new vector returns a valid result.
        createBucketWithVectorIndex();
        insertDocumentsWithLabelsAndVectors(
                new String[]{"target", "other1", "other2"},
                new float[][]{{1.0f, 0.0f, 0.0f}, {0.0f, 1.0f, 0.0f}, {0.5f, 0.5f, 0.0f}}
        );

        OnHeapVectorGraphIndex graph = getVectorGraph(TEST_BUCKET, "embedding");
        assertNotNull(graph);
        assertEquals(3, graph.size());

        SearchResult preUpdate = graph.search(new float[]{1.0f, 0.0f, 0.0f}, 1);
        assertEquals(1, preUpdate.getNodes().length);
        int oldOrdinal = preUpdate.getNodes()[0].node;
        assertNotNull(graph.getMetadata().findDocumentLocation(oldOrdinal));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"label\": \"target\"}", "{\"$set\": {\"embedding\": [0.0, 0.0, 1.0]}}").encode(buf);
        List<ObjectId> updatedIds = extractObjectIds(runCommand(channel, buf));
        assertEquals(1, updatedIds.size());

        assertNull(graph.getMetadata().findDocumentLocation(oldOrdinal));

        SearchResult postUpdate = graph.search(new float[]{0.0f, 0.0f, 1.0f}, 1);
        assertEquals(1, postUpdate.getNodes().length);
        int newOrdinal = postUpdate.getNodes()[0].node;
        assertNotNull(graph.getMetadata().findDocumentLocation(newOrdinal));
    }

    @Test
    void shouldFindUpdatedVectorWhenBucketHasSingleDocument() {
        // Behavior: Updating the vector field of the only document in a bucket replaces the node
        // in the graph, and searching for the new vector returns that document.
        createBucketWithVectorIndex();
        insertDocumentsWithLabelsAndVectors(
                new String[]{"target"},
                new float[][]{{1.0f, 0.0f, 0.0f}}
        );

        OnHeapVectorGraphIndex graph = getVectorGraph(TEST_BUCKET, "embedding");
        assertNotNull(graph);
        assertEquals(1, graph.size());

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"label\": \"target\"}", "{\"$set\": {\"embedding\": [0.0, 0.0, 1.0]}}").encode(buf);
        List<ObjectId> updatedIds = extractObjectIds(runCommand(channel, buf));
        assertEquals(1, updatedIds.size());
        ObjectId targetObjectId = updatedIds.getFirst();

        SearchResult postUpdate = graph.search(new float[]{0.0f, 0.0f, 1.0f}, 1);
        assertEquals(1, postUpdate.getNodes().length);
        DocumentLocation location = graph.getMetadata().findDocumentLocation(postUpdate.getNodes()[0].node);
        assertNotNull(location);
        assertEquals(targetObjectId, location.objectId());
    }

    @Test
    void shouldRefreshVectorIndexWhenParentPathSet() {
        // Behavior: Replacing the parent document of a nested vector field via $set refreshes
        // the vector index. The old vector is removed from the graph and the new vector becomes
        // searchable for the same document.
        BucketCommandBuilder<byte[], byte[]> createCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf createBuf = Unpooled.buffer();
        createCmd.create(TEST_BUCKET, BucketCreateArgs.Builder.indexes(
                "{\"$vector\": {\"field\": \"profile.embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}"
        )).encode(createBuf);
        Object createResponse = runCommand(channel, createBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, createResponse);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) createResponse).content());

        // Insert documents with nested vector fields
        String[] labels = {"target", "other1", "other2"};
        float[][] vectors = {{1.0f, 0.0f, 0.0f}, {0.0f, 1.0f, 0.0f}, {0.5f, 0.5f, 0.0f}};
        ObjectId targetObjectId = null;
        for (int i = 0; i < labels.length; i++) {
            BsonDocument doc = new BsonDocument();
            doc.put("label", new BsonString(labels[i]));
            BsonArray embedding = new BsonArray();
            for (float v : vectors[i]) {
                embedding.add(new BsonDouble(v));
            }
            doc.put("profile", new BsonDocument().append("embedding", embedding));

            BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            ByteBuf buf = Unpooled.buffer();
            insertCmd.insert(TEST_BUCKET, BSONUtil.toBytes(doc)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
            ObjectId objectId = TestUtil.bulkStringToObjectId(
                    (FullBulkStringRedisMessage) ((ArrayRedisMessage) msg).children().getFirst());
            if (labels[i].equals("target")) {
                targetObjectId = objectId;
            }
        }
        assertNotNull(targetObjectId);

        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            OnHeapVectorGraphIndex g = getVectorGraph(TEST_BUCKET, "profile.embedding");
            assertNotNull(g, "Vector graph should exist");
            assertEquals(3, g.size(), "Graph should contain the inserted vectors");
        });
        OnHeapVectorGraphIndex graph = getVectorGraph(TEST_BUCKET, "profile.embedding");

        assertNotNull(graph);
        SearchResult preUpdate = graph.search(new float[]{1.0f, 0.0f, 0.0f}, 1);
        assertEquals(1, preUpdate.getNodes().length);
        int oldOrdinal = preUpdate.getNodes()[0].node;
        assertNotNull(graph.getMetadata().findDocumentLocation(oldOrdinal));

        // Replace the whole parent document of the vector field
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"label\": \"target\"}",
                "{\"$set\": {\"profile\": {\"embedding\": [0.0, 0.0, 1.0]}}}").encode(buf);
        List<ObjectId> updatedIds = extractObjectIds(runCommand(channel, buf));
        assertEquals(1, updatedIds.size());

        // The graph hooks run after commit; await the old vector's removal and the new one's arrival
        ObjectId finalTargetObjectId = targetObjectId;
        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            assertNull(graph.getMetadata().findDocumentLocation(oldOrdinal));

            SearchResult postUpdate = graph.search(new float[]{0.0f, 0.0f, 1.0f}, 1);
            assertEquals(1, postUpdate.getNodes().length);
            int newOrdinal = postUpdate.getNodes()[0].node;
            var location = graph.getMetadata().findDocumentLocation(newOrdinal);
            assertNotNull(location);
            assertEquals(finalTargetObjectId, location.objectId(),
                    "Nearest node to the new vector should be the updated document");
        });
    }

    @Test
    void shouldSearchVectorsWithResp2() {
        // Behavior: BUCKET.VECTOR returns an array of [score, document] pairs in RESP2 mode.
        createBucketWithVectorIndex();

        float[][] vectors = {
                {0.1f, 0.2f, 0.3f},
                {0.4f, 0.5f, 0.6f},
        };
        insertDocumentsWithVectors(vectors);

        // Use RESP2 (default protocol)
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.4f, 0.5f, 0.6f}).encode(buf);
        Object response = runCommand(channel, buf);

        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(2, array.children().size());

        // Each child should be a [score, document] pair
        for (RedisMessage child : array.children()) {
            assertInstanceOf(ArrayRedisMessage.class, child);
            ArrayRedisMessage pair = (ArrayRedisMessage) child;
            assertEquals(2, pair.children().size());
            assertInstanceOf(FullBulkStringRedisMessage.class, pair.children().get(0)); // score as string
            assertInstanceOf(FullBulkStringRedisMessage.class, pair.children().get(1)); // document
        }
    }

    @Test
    void shouldSearchVectorsWithCollationAwareFilter() {
        // Behavior: BUCKET.VECTOR with a BQL filter respects bucket-level collation.
        // Turkish PRIMARY (strength=1) treats 'i' and 'İ' as the same base character,
        // so filtering with $eq "İstanbul" matches both "istanbul" and "İstanbul".

        BucketCommandBuilder<byte[], byte[]> createCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        createCmd.create(TEST_BUCKET, BucketCreateArgs.Builder.indexes(
                "{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}"
        ).collation("{\"locale\": \"tr\", \"strength\": 1}")).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());

        String[] labels = {"istanbul", "\u0130stanbul", "ankara"};
        float[][] vectors = {
                {0.1f, 0.2f, 0.3f},
                {0.4f, 0.5f, 0.6f},
                {0.7f, 0.8f, 0.9f}
        };
        insertDocumentsWithLabelsAndVectors(labels, vectors);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.3f, 0.4f, 0.5f},
                BucketVectorArgs.Builder.filter("{\"label\": {\"$eq\": \"\u0130stanbul\"}}")).encode(buf);
        Object vectorResponse = runCommand(channel, buf);

        assertInstanceOf(ArrayRedisMessage.class, vectorResponse);
        ArrayRedisMessage array = (ArrayRedisMessage) vectorResponse;
        assertEquals(2, array.children().size(),
                "Turkish PRIMARY collation should match both 'istanbul' and '\u0130stanbul'");

        Set<String> returnedLabels = new HashSet<>();
        for (RedisMessage child : array.children()) {
            assertInstanceOf(MapRedisMessage.class, child);
            MapRedisMessage map = (MapRedisMessage) child;

            RedisMessage score = findInMapMessage(map, "score");
            assertNotNull(score);
            assertInstanceOf(DoubleRedisMessage.class, score);

            RedisMessage entry = findInMapMessage(map, "entry");
            assertNotNull(entry);
            assertInstanceOf(FullBulkStringRedisMessage.class, entry);
            byte[] docBytes = io.netty.buffer.ByteBufUtil.getBytes(((FullBulkStringRedisMessage) entry).content());
            BsonDocument doc = BSONUtil.toBsonDocument(docBytes);
            returnedLabels.add(doc.getString("label").getValue());
        }
        assertEquals(Set.of("istanbul", "\u0130stanbul"), returnedLabels);
    }
}
