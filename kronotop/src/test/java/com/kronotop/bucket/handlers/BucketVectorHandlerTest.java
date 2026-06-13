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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.index.SingleFieldIndexUtil;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.commands.BucketVectorArgs;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BucketVectorHandlerTest extends BaseBucketHandlerTest {

    /**
     * Creates a bucket with a vector index on "embedding" and a single-field index on "label".
     * The field index is created separately and waited on for readiness.
     */
    private void createBucketWithVectorAndFieldIndex() {
        // Step 1: Create a bucket with a vector index
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.indexes(
                "{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}"
        )).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());

        // Step 2: Create the "label" field index and wait for readiness
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        SingleFieldIndexDefinition labelIndex = SingleFieldIndexDefinition.create(
                "label_string_idx", "label", BsonType.STRING, false, IndexStatus.WAITING
        );

        DirectorySubspace subspace;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            subspace = SingleFieldIndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, labelIndex);
            tr.commit().join();
        }
        waitForIndexReadiness(subspace);
        waitUntilUpdated(metadata);
    }

    private void createBucketWithVectorIndexOnly() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.indexes(
                "{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}"
        )).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
    }

    private void insertDocument(String label, float[] vec) {
        insertDocument(label, null, vec);
    }

    private void insertDocument(String label, String category, float[] vec) {
        BsonDocument doc = new BsonDocument();
        doc.put("label", new BsonString(label));
        if (category != null) {
            doc.put("category", new BsonString(category));
        }
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

    /**
     * Collects the "label" field of every document returned by a BUCKET.VECTOR response.
     */
    private List<String> vectorResultLabels(Object response) {
        if (response instanceof ErrorRedisMessage err) {
            fail("Unexpected error: " + err.content());
        }
        assertInstanceOf(ArrayRedisMessage.class, response);
        List<String> labels = new ArrayList<>();
        for (RedisMessage child : ((ArrayRedisMessage) response).children()) {
            MapRedisMessage map = (MapRedisMessage) child;
            RedisMessage entryMsg = findInMapMessage(map, "entry");
            assertNotNull(entryMsg);
            byte[] docBytes = io.netty.buffer.ByteBufUtil.getBytes(((FullBulkStringRedisMessage) entryMsg).content());
            BsonDocument doc = BSONUtil.toBsonDocument(docBytes);
            labels.add(doc.getString("label").getValue());
        }
        return labels;
    }

    @Test
    void shouldSearchWithoutFilter() {
        // Behavior: BUCKET.VECTOR without FILTER returns all matching vectors ordered by similarity.
        createBucketWithVectorAndFieldIndex();

        insertDocument("alpha", new float[]{0.1f, 0.2f, 0.3f});
        insertDocument("beta", new float[]{0.4f, 0.5f, 0.6f});
        insertDocument("gamma", new float[]{0.7f, 0.8f, 0.9f});

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.4f, 0.5f, 0.6f}).encode(buf);
        Object response = runCommand(channel, buf);

        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(3, array.children().size());
    }

    @Test
    void shouldFilterByIndexedField() {
        // Behavior: BUCKET.VECTOR with FILTER on an indexed field returns only documents matching both
        // the vector similarity and the filter condition.
        createBucketWithVectorAndFieldIndex();

        insertDocument("alpha", new float[]{0.1f, 0.2f, 0.3f});
        insertDocument("beta", new float[]{0.4f, 0.5f, 0.6f});
        insertDocument("gamma", new float[]{0.7f, 0.8f, 0.9f});

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.4f, 0.5f, 0.6f}, BucketVectorArgs.Builder.filter("{\"label\": \"beta\"}")).encode(buf);
        Object response = runCommand(channel, buf);

        if (response instanceof ErrorRedisMessage err) {
            fail("Unexpected error: " + err.content());
        }
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(1, array.children().size());

        MapRedisMessage map = (MapRedisMessage) array.children().getFirst();
        RedisMessage entryMsg = findInMapMessage(map, "entry");
        assertNotNull(entryMsg);
        assertInstanceOf(FullBulkStringRedisMessage.class, entryMsg);
        byte[] docBytes = io.netty.buffer.ByteBufUtil.getBytes(((FullBulkStringRedisMessage) entryMsg).content());
        BsonDocument doc = BSONUtil.toBsonDocument(docBytes);
        assertEquals("beta", doc.getString("label").getValue());
    }

    @Test
    void shouldFilterOnNonIndexedField() {
        // Behavior: BUCKET.VECTOR with FILTER on a non-indexed field uses post-filtering via
        // full-scan residual predicate, returning only documents that match the filter.
        createBucketWithVectorIndexOnly();

        insertDocument("alpha", new float[]{0.1f, 0.2f, 0.3f});
        insertDocument("beta", new float[]{0.4f, 0.5f, 0.6f});

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.1f, 0.2f, 0.3f}, BucketVectorArgs.Builder.filter("{\"label\": \"alpha\"}")).encode(buf);
        Object response = runCommand(channel, buf);

        if (response instanceof ErrorRedisMessage err) {
            fail("Unexpected error: " + err.content());
        }
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(1, array.children().size());

        MapRedisMessage map = (MapRedisMessage) array.children().getFirst();
        RedisMessage entryMsg = findInMapMessage(map, "entry");
        assertNotNull(entryMsg);
        byte[] docBytes = io.netty.buffer.ByteBufUtil.getBytes(((FullBulkStringRedisMessage) entryMsg).content());
        BsonDocument doc = BSONUtil.toBsonDocument(docBytes);
        assertEquals("alpha", doc.getString("label").getValue());
    }

    @Test
    void shouldReturnEmptyWhenFilterMatchesNothing() {
        // Behavior: BUCKET.VECTOR with FILTER that matches no documents returns an empty array.
        createBucketWithVectorAndFieldIndex();

        insertDocument("alpha", new float[]{0.1f, 0.2f, 0.3f});
        insertDocument("beta", new float[]{0.4f, 0.5f, 0.6f});

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.4f, 0.5f, 0.6f}, BucketVectorArgs.Builder.filter("{\"label\": \"nonexistent\"}")).encode(buf);
        Object response = runCommand(channel, buf);

        if (response instanceof ErrorRedisMessage err) {
            fail("Unexpected error: " + err.content());
        }
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertTrue(array.children().isEmpty());
    }

    @Test
    void shouldFilterWithTopLimit() {
        // Behavior: BUCKET.VECTOR with FILTER and TOP returns at most TOP results that match the filter.
        createBucketWithVectorAndFieldIndex();

        insertDocument("alpha", new float[]{0.1f, 0.2f, 0.3f});
        insertDocument("alpha", new float[]{0.4f, 0.5f, 0.6f});
        insertDocument("alpha", new float[]{0.7f, 0.8f, 0.9f});
        insertDocument("beta", new float[]{0.2f, 0.3f, 0.4f});

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.5f, 0.5f, 0.5f}, BucketVectorArgs.Builder.top(2).filter("{\"label\": \"alpha\"}")).encode(buf);
        Object response = runCommand(channel, buf);

        if (response instanceof ErrorRedisMessage err) {
            fail("Unexpected error: " + err.content());
        }
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(2, array.children().size());

        for (RedisMessage child : array.children()) {
            MapRedisMessage map = (MapRedisMessage) child;
            RedisMessage entryMsg = findInMapMessage(map, "entry");
            byte[] docBytes = io.netty.buffer.ByteBufUtil.getBytes(((FullBulkStringRedisMessage) entryMsg).content());
            BsonDocument doc = BSONUtil.toBsonDocument(docBytes);
            assertEquals("alpha", doc.getString("label").getValue());
        }
    }

    @Test
    void shouldOvershootWhenFilterIsHighlySelective() {
        // Behavior: When FILTER is highly selective (few matches), the overshoot loop increases
        // fetchSize to find enough matching results. With TOP 1, even if the closest vector doesn't
        // match the filter, the handler finds a matching result by overshooting.
        createBucketWithVectorAndFieldIndex();

        // Insert many non-matching documents and one matching
        for (int i = 0; i < 10; i++) {
            insertDocument("noise", new float[]{
                    0.1f + i * 0.01f,
                    0.2f + i * 0.01f,
                    0.3f + i * 0.01f
            });
        }
        // The target document is further away but matches the filter
        insertDocument("target", new float[]{0.9f, 0.9f, 0.9f});

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.15f, 0.25f, 0.35f}, BucketVectorArgs.Builder.top(1).filter("{\"label\": \"target\"}")).encode(buf);
        Object response = runCommand(channel, buf);

        if (response instanceof ErrorRedisMessage err) {
            fail("Unexpected error: " + err.content());
        }
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(1, array.children().size());

        MapRedisMessage map = (MapRedisMessage) array.children().getFirst();
        RedisMessage entryMsg = findInMapMessage(map, "entry");
        byte[] docBytes = io.netty.buffer.ByteBufUtil.getBytes(((FullBulkStringRedisMessage) entryMsg).content());
        BsonDocument doc = BSONUtil.toBsonDocument(docBytes);
        assertEquals("target", doc.getString("label").getValue());
    }

    @Test
    void shouldResumeFilterAcrossMultipleBatches() {
        // Behavior: When the filter matches more documents than the initial batch size
        // (batchSize = ceil(limit * 1.5)), the handler resumes the filter pipeline to
        // fetch additional batches while growing the vector search K, until enough
        // filtered results are found.
        createBucketWithVectorAndFieldIndex();

        // 20 noise documents close to the query vector — these dominate the top-K
        for (int i = 0; i < 20; i++) {
            insertDocument("noise", new float[]{
                    0.1f + i * 0.005f,
                    0.2f + i * 0.005f,
                    0.3f + i * 0.005f
            });
        }

        // 10 target documents far from the query — require deep vector search
        for (int i = 0; i < 10; i++) {
            insertDocument("target", new float[]{
                    0.9f - i * 0.03f,
                    0.1f + i * 0.02f,
                    0.1f + i * 0.02f
            });
        }

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // TOP 1 → batchSize = ceil(1 * 1.5) = 2
        // Filter matches 10 docs → needs 5 batches of 2 to exhaust
        // Vector must grow past 20 noise docs to reach any target
        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.1f, 0.2f, 0.3f},
                BucketVectorArgs.Builder.top(1).filter("{\"label\": \"target\"}")).encode(buf);
        Object response = runCommand(channel, buf);

        if (response instanceof ErrorRedisMessage err) {
            fail("Unexpected error: " + err.content());
        }
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(1, array.children().size());

        MapRedisMessage map = (MapRedisMessage) array.children().getFirst();
        RedisMessage entryMsg = findInMapMessage(map, "entry");
        byte[] docBytes = io.netty.buffer.ByteBufUtil.getBytes(
                ((FullBulkStringRedisMessage) entryMsg).content());
        BsonDocument doc = BSONUtil.toBsonDocument(docBytes);
        assertEquals("target", doc.getString("label").getValue());
    }

    @Test
    void shouldFilterByThreshold() {
        // Behavior: THRESHOLD excludes results with score below the threshold; only high-similarity results returned.
        createBucketWithVectorIndexOnly();

        insertDocument("close", new float[]{0.4f, 0.5f, 0.6f});
        insertDocument("far", new float[]{-0.9f, -0.8f, -0.7f});

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.4f, 0.5f, 0.6f},
                BucketVectorArgs.Builder.threshold(0.99f)).encode(buf);
        Object response = runCommand(channel, buf);

        if (response instanceof ErrorRedisMessage err) {
            fail("Unexpected error: " + err.content());
        }
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        // The exact match should have score ~1.0 (cosine), the far one should be filtered out
        assertEquals(1, array.children().size());

        MapRedisMessage map = (MapRedisMessage) array.children().getFirst();
        RedisMessage entryMsg = findInMapMessage(map, "entry");
        byte[] docBytes = io.netty.buffer.ByteBufUtil.getBytes(((FullBulkStringRedisMessage) entryMsg).content());
        BsonDocument doc = BSONUtil.toBsonDocument(docBytes);
        assertEquals("close", doc.getString("label").getValue());
    }

    @Test
    void shouldReturnEmptyWhenThresholdExcludesAll() {
        // Behavior: Very high THRESHOLD (e.g. 2.0 for cosine) returns empty array when no results meet the threshold.
        createBucketWithVectorIndexOnly();

        insertDocument("alpha", new float[]{0.1f, 0.2f, 0.3f});
        insertDocument("beta", new float[]{0.4f, 0.5f, 0.6f});

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.4f, 0.5f, 0.6f},
                BucketVectorArgs.Builder.threshold(2.0f)).encode(buf);
        Object response = runCommand(channel, buf);

        if (response instanceof ErrorRedisMessage err) {
            fail("Unexpected error: " + err.content());
        }
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertTrue(array.children().isEmpty());
    }

    @Test
    void shouldRespectMaxScanCandidatesOverride() {
        // Behavior: MAX-SCAN-CANDIDATES limits the total candidates examined during filtered search.
        // With a very low cap, the search stops early and returns fewer results than available.
        createBucketWithVectorAndFieldIndex();

        // Insert many noise documents and a few targets far from the query vector
        for (int i = 0; i < 20; i++) {
            insertDocument("noise", new float[]{
                    0.1f + i * 0.005f,
                    0.2f + i * 0.005f,
                    0.3f + i * 0.005f
            });
        }
        for (int i = 0; i < 5; i++) {
            insertDocument("target", new float[]{
                    0.9f - i * 0.02f,
                    0.1f + i * 0.01f,
                    0.1f + i * 0.01f
            });
        }

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // MAX-SCAN-CANDIDATES=3 means the supplier examines at most 3 candidates before stopping.
        // With 20 noise docs closer to the query, the cap will be hit before any target is found.
        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.1f, 0.2f, 0.3f},
                BucketVectorArgs.Builder.filter("{\"label\": \"target\"}").maxScanCandidates(3)).encode(buf);
        Object response = runCommand(channel, buf);

        if (response instanceof ErrorRedisMessage err) {
            fail("Unexpected error: " + err.content());
        }
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        // With only 3 candidates examined, the search can't reach the target docs past 20 noise docs
        assertTrue(array.children().size() < 5);
    }

    @Test
    void shouldAcceptOverqueryParameter() {
        // Behavior: BUCKET.VECTOR with OVERQUERY >= 1.0 is accepted and returns results normally.
        createBucketWithVectorIndexOnly();

        insertDocument("alpha", new float[]{0.1f, 0.2f, 0.3f});
        insertDocument("beta", new float[]{0.4f, 0.5f, 0.6f});

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.4f, 0.5f, 0.6f},
                BucketVectorArgs.Builder.overquery(2.0f)).encode(buf);
        Object response = runCommand(channel, buf);

        if (response instanceof ErrorRedisMessage err) {
            fail("Unexpected error: " + err.content());
        }
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertFalse(array.children().isEmpty());
    }

    @Test
    void shouldRejectOverqueryBelowOne() {
        // Behavior: BUCKET.VECTOR with OVERQUERY < 1.0 returns an error.
        createBucketWithVectorIndexOnly();

        insertDocument("alpha", new float[]{0.1f, 0.2f, 0.3f});

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.1f, 0.2f, 0.3f},
                BucketVectorArgs.Builder.overquery(0.5f)).encode(buf);
        Object response = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, response);
    }

    @Test
    void shouldCombineThresholdWithFilter() {
        // Behavior: FILTER + THRESHOLD work together: filter narrows candidates, threshold removes low-score survivors.
        createBucketWithVectorAndFieldIndex();

        insertDocument("alpha", new float[]{0.4f, 0.5f, 0.6f});
        insertDocument("alpha", new float[]{-0.9f, -0.8f, -0.7f});
        insertDocument("beta", new float[]{0.4f, 0.5f, 0.6f});

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.4f, 0.5f, 0.6f},
                BucketVectorArgs.Builder.filter("{\"label\": \"alpha\"}").threshold(0.99f)).encode(buf);
        Object response = runCommand(channel, buf);

        if (response instanceof ErrorRedisMessage err) {
            fail("Unexpected error: " + err.content());
        }
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        // Filter keeps only "alpha" docs (2), a threshold keeps only the high-similarity one (1)
        assertEquals(1, array.children().size());

        MapRedisMessage map = (MapRedisMessage) array.children().getFirst();
        RedisMessage entryMsg = findInMapMessage(map, "entry");
        byte[] docBytes = io.netty.buffer.ByteBufUtil.getBytes(((FullBulkStringRedisMessage) entryMsg).content());
        BsonDocument doc = BSONUtil.toBsonDocument(docBytes);
        assertEquals("alpha", doc.getString("label").getValue());
    }

    @Test
    void shouldFilterNumericWideningInt32StoredDoublePredicateViaVectorSearch() {
        // Behavior: BUCKET.VECTOR with FILTER containing a DOUBLE predicate correctly matches
        // documents storing INT32 values via numeric widening in PredicateEvaluator.
        createBucketWithVectorIndexOnly();

        // Document with price=50 (INT32), should match $lt 100.0
        BsonDocument doc1 = new BsonDocument();
        doc1.put("price", new BsonInt32(50));
        BsonArray emb1 = new BsonArray();
        for (float v : new float[]{0.1f, 0.2f, 0.3f}) {
            emb1.add(new BsonDouble(v));
        }
        doc1.put("embedding", emb1);

        BucketCommandBuilder<byte[], byte[]> insertCmd1 = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf insertBuf1 = Unpooled.buffer();
        insertCmd1.insert(TEST_BUCKET, BSONUtil.toBytes(doc1)).encode(insertBuf1);
        assertInstanceOf(ArrayRedisMessage.class, runCommand(channel, insertBuf1));

        // Document with price=150 (INT32), should NOT match $lt 100.0
        BsonDocument doc2 = new BsonDocument();
        doc2.put("price", new BsonInt32(150));
        BsonArray emb2 = new BsonArray();
        for (float v : new float[]{0.4f, 0.5f, 0.6f}) {
            emb2.add(new BsonDouble(v));
        }
        doc2.put("embedding", emb2);

        BucketCommandBuilder<byte[], byte[]> insertCmd2 = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf insertBuf2 = Unpooled.buffer();
        insertCmd2.insert(TEST_BUCKET, BSONUtil.toBytes(doc2)).encode(insertBuf2);
        assertInstanceOf(ArrayRedisMessage.class, runCommand(channel, insertBuf2));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.1f, 0.2f, 0.3f},
                BucketVectorArgs.Builder.filter("{\"price\": {\"$lt\": 100.0}}")).encode(buf);
        Object response = runCommand(channel, buf);

        if (response instanceof ErrorRedisMessage err) {
            fail("Unexpected error: " + err.content());
        }
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(1, array.children().size());

        MapRedisMessage map = (MapRedisMessage) array.children().getFirst();
        RedisMessage entryMsg = findInMapMessage(map, "entry");
        assertNotNull(entryMsg);
        byte[] docBytes = io.netty.buffer.ByteBufUtil.getBytes(((FullBulkStringRedisMessage) entryMsg).content());
        BsonDocument result = BSONUtil.toBsonDocument(docBytes);
        assertEquals(50, result.getInt32("price").getValue());
    }

    @Test
    void shouldFilterNumericWideningDoubleStoredInt32PredicateViaVectorSearch() {
        // Behavior: BUCKET.VECTOR with FILTER containing an INT32 predicate correctly matches
        // documents storing DOUBLE values via numeric widening in PredicateEvaluator.
        createBucketWithVectorIndexOnly();

        // Document with price=19.99 (DOUBLE), should match $gt 10
        BsonDocument doc1 = new BsonDocument();
        doc1.put("price", new BsonDouble(19.99));
        BsonArray emb1 = new BsonArray();
        for (float v : new float[]{0.1f, 0.2f, 0.3f}) {
            emb1.add(new BsonDouble(v));
        }
        doc1.put("embedding", emb1);

        BucketCommandBuilder<byte[], byte[]> insertCmd1 = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf insertBuf1 = Unpooled.buffer();
        insertCmd1.insert(TEST_BUCKET, BSONUtil.toBytes(doc1)).encode(insertBuf1);
        assertInstanceOf(ArrayRedisMessage.class, runCommand(channel, insertBuf1));

        // Document with price=5.0 (DOUBLE), should NOT match $gt 10
        BsonDocument doc2 = new BsonDocument();
        doc2.put("price", new BsonDouble(5.0));
        BsonArray emb2 = new BsonArray();
        for (float v : new float[]{0.4f, 0.5f, 0.6f}) {
            emb2.add(new BsonDouble(v));
        }
        doc2.put("embedding", emb2);

        BucketCommandBuilder<byte[], byte[]> insertCmd2 = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf insertBuf2 = Unpooled.buffer();
        insertCmd2.insert(TEST_BUCKET, BSONUtil.toBytes(doc2)).encode(insertBuf2);
        assertInstanceOf(ArrayRedisMessage.class, runCommand(channel, insertBuf2));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.1f, 0.2f, 0.3f},
                BucketVectorArgs.Builder.filter("{\"price\": {\"$gt\": 10}}")).encode(buf);
        Object response = runCommand(channel, buf);

        if (response instanceof ErrorRedisMessage err) {
            fail("Unexpected error: " + err.content());
        }
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(1, array.children().size());

        MapRedisMessage map = (MapRedisMessage) array.children().getFirst();
        RedisMessage entryMsg = findInMapMessage(map, "entry");
        assertNotNull(entryMsg);
        byte[] docBytes = io.netty.buffer.ByteBufUtil.getBytes(((FullBulkStringRedisMessage) entryMsg).content());
        BsonDocument result = BSONUtil.toBsonDocument(docBytes);
        assertEquals(19.99, result.getDouble("price").getValue());
    }

    @Test
    void shouldProjectIncludedFieldsOnVectorSearch() {
        // Behavior: BUCKET.VECTOR with PROJECTION {"label": 1} returns only _id and label fields,
        // excluding category and embedding from the response.
        createBucketWithVectorIndexOnly();

        insertDocument("alpha", "electronics", new float[]{0.1f, 0.2f, 0.3f});
        insertDocument("beta", "books", new float[]{0.4f, 0.5f, 0.6f});

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.4f, 0.5f, 0.6f},
                new BucketVectorArgs().projection("{\"label\": 1}")).encode(buf);
        Object response = runCommand(channel, buf);

        if (response instanceof ErrorRedisMessage err) {
            fail("Unexpected error: " + err.content());
        }
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(2, array.children().size());

        for (RedisMessage child : array.children()) {
            MapRedisMessage map = (MapRedisMessage) child;
            RedisMessage scoreMsg = findInMapMessage(map, "score");
            assertNotNull(scoreMsg);

            RedisMessage entryMsg = findInMapMessage(map, "entry");
            assertNotNull(entryMsg);
            byte[] docBytes = io.netty.buffer.ByteBufUtil.getBytes(((FullBulkStringRedisMessage) entryMsg).content());
            BsonDocument doc = BSONUtil.toBsonDocument(docBytes);

            assertTrue(doc.containsKey("_id"));
            assertTrue(doc.containsKey("label"));
            assertFalse(doc.containsKey("category"));
            assertFalse(doc.containsKey("embedding"));
        }
    }

    @Test
    void shouldProjectExcludedFieldsOnVectorSearch() {
        // Behavior: BUCKET.VECTOR with PROJECTION {"embedding": 0} returns all fields except embedding.
        createBucketWithVectorIndexOnly();

        insertDocument("alpha", "electronics", new float[]{0.1f, 0.2f, 0.3f});

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.1f, 0.2f, 0.3f},
                new BucketVectorArgs().projection("{\"embedding\": 0}")).encode(buf);
        Object response = runCommand(channel, buf);

        if (response instanceof ErrorRedisMessage err) {
            fail("Unexpected error: " + err.content());
        }
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(1, array.children().size());

        MapRedisMessage map = (MapRedisMessage) array.children().getFirst();
        RedisMessage entryMsg = findInMapMessage(map, "entry");
        assertNotNull(entryMsg);
        byte[] docBytes = io.netty.buffer.ByteBufUtil.getBytes(((FullBulkStringRedisMessage) entryMsg).content());
        BsonDocument doc = BSONUtil.toBsonDocument(docBytes);

        assertTrue(doc.containsKey("_id"));
        assertTrue(doc.containsKey("label"));
        assertTrue(doc.containsKey("category"));
        assertFalse(doc.containsKey("embedding"));
    }

    @Test
    void shouldProjectWithFilterOnVectorSearch() {
        // Behavior: BUCKET.VECTOR with both FILTER and PROJECTION applies the filter first,
        // then projects the matching results to include only the specified fields.
        createBucketWithVectorAndFieldIndex();

        insertDocument("alpha", "electronics", new float[]{0.1f, 0.2f, 0.3f});
        insertDocument("beta", "books", new float[]{0.4f, 0.5f, 0.6f});

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.4f, 0.5f, 0.6f},
                new BucketVectorArgs().filter("{\"label\": \"beta\"}").projection("{\"label\": 1}")).encode(buf);
        Object response = runCommand(channel, buf);

        if (response instanceof ErrorRedisMessage err) {
            fail("Unexpected error: " + err.content());
        }
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(1, array.children().size());

        MapRedisMessage map = (MapRedisMessage) array.children().getFirst();
        RedisMessage entryMsg = findInMapMessage(map, "entry");
        assertNotNull(entryMsg);
        byte[] docBytes = io.netty.buffer.ByteBufUtil.getBytes(((FullBulkStringRedisMessage) entryMsg).content());
        BsonDocument doc = BSONUtil.toBsonDocument(docBytes);

        assertTrue(doc.containsKey("_id"));
        assertTrue(doc.containsKey("label"));
        assertEquals("beta", doc.getString("label").getValue());
        assertFalse(doc.containsKey("category"));
        assertFalse(doc.containsKey("embedding"));
    }

    @Test
    void shouldFilterWithRegexInVectorSearch() {
        // Behavior: BUCKET.VECTOR with a $regex FILTER post-filters candidates, returning only
        // documents whose string field matches the pattern.
        createBucketWithVectorIndexOnly();

        insertDocument("alpha", new float[]{0.1f, 0.2f, 0.3f});
        insertDocument("alpine", new float[]{0.15f, 0.25f, 0.35f});
        insertDocument("beta", new float[]{0.4f, 0.5f, 0.6f});

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.1f, 0.2f, 0.3f},
                BucketVectorArgs.Builder.filter("{\"label\": {\"$regex\": \"^al\"}}")).encode(buf);
        Object response = runCommand(channel, buf);

        List<String> labels = vectorResultLabels(response);
        assertEquals(2, labels.size(), "Only labels starting with 'al' should match");
        assertTrue(labels.contains("alpha"));
        assertTrue(labels.contains("alpine"));
    }

    @Test
    void shouldFilterWithCaseInsensitiveRegexInVectorSearch() {
        // Behavior: the i option makes the $regex FILTER in BUCKET.VECTOR case-insensitive.
        createBucketWithVectorIndexOnly();

        insertDocument("Alpha", new float[]{0.1f, 0.2f, 0.3f});
        insertDocument("ALPHA", new float[]{0.15f, 0.25f, 0.35f});
        insertDocument("beta", new float[]{0.4f, 0.5f, 0.6f});

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.1f, 0.2f, 0.3f},
                BucketVectorArgs.Builder.filter("{\"label\": {\"$regex\": \"^alpha$\", \"$options\": \"i\"}}")).encode(buf);
        Object response = runCommand(channel, buf);

        List<String> labels = vectorResultLabels(response);
        assertEquals(2, labels.size(), "Case-insensitive match should find both 'Alpha' and 'ALPHA'");
        assertTrue(labels.contains("Alpha"));
        assertTrue(labels.contains("ALPHA"));
    }

    @Test
    void shouldReturnNoResultsWhenRegexFilterMatchesNothing() {
        // Behavior: a $regex FILTER that matches no candidate returns an empty result set.
        createBucketWithVectorIndexOnly();

        insertDocument("alpha", new float[]{0.1f, 0.2f, 0.3f});
        insertDocument("beta", new float[]{0.4f, 0.5f, 0.6f});

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.vector(TEST_BUCKET, "embedding", new float[]{0.1f, 0.2f, 0.3f},
                BucketVectorArgs.Builder.filter("{\"label\": {\"$regex\": \"^zzz\"}}")).encode(buf);
        Object response = runCommand(channel, buf);

        List<String> labels = vectorResultLabels(response);
        assertEquals(0, labels.size(), "No label matches the pattern");
    }
}
