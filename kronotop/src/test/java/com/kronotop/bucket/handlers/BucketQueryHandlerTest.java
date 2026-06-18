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
import com.kronotop.bucket.*;
import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ParameterExtractor;
import com.kronotop.bucket.bql.QueryShape;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.pipeline.PipelineNode;
import com.kronotop.commands.*;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class BucketQueryHandlerTest extends BaseBucketHandlerTest {

    private static final String COLLATION_BUCKET = "collation-bucket";

    @BeforeEach
    void setUp() {
        createBucket(TEST_BUCKET);
    }

    private void createBucketWithCollation(String bucketName, String collationJson) {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.create(bucketName, BucketCreateArgs.Builder
                .collation(collationJson)
                .shards(List.of(TEST_SHARD_ID))
                .ifNotExists()).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
    }

    private void insertDocumentsIntoBucket(String bucketName, List<byte[]> documents) {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(documents);
        cmd.insert(bucketName, docs).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
    }

    @Test
    void shouldReturnSameCachedPlanForSubsequentQueries() {
        // Insert a document to create the bucket
        insertDocumentsAndGetObjectIds(List.of(BSONUtil.jsonToDocumentThenBytes("{\"age\": 30}")));

        BucketService bucketService = context.getService(BucketService.NAME);
        BucketMetadata metadata;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
        }

        byte[] query = "{\"age\": {\"$eq\": 30}}".getBytes();
        long maxTtl = context.getConfig().getInt("bucket.plan_cache.max_ttl");

        BqlExpr expr = BqlParser.parse(query);
        List<BqlValue> parameters = expr != null
                ? ParameterExtractor.extract(expr)
                : List.of();

        // Plan the same query twice with caching enabled
        PipelineNode plan1 = bucketService.getPlanner().plan(context, metadata, expr, parameters, true, maxTtl);
        PipelineNode plan2 = bucketService.getPlanner().plan(context, metadata, expr, parameters, true, maxTtl);

        // Both plans should be the same object (cached) and structurally equal
        assertSame(plan1, plan2, "Subsequent queries should return the same cached plan");
        assertThat(plan1).usingRecursiveComparison().isEqualTo(plan2);
    }

    @Test
    void shouldShareCachedPlanAcrossRegexQueriesWithDifferentPatterns() {
        // Behavior: regex queries on the same field share one cached plan because the pattern is a
        // parameter, not part of the plan shape. Each query still matches with its own pattern.
        insertDocumentsAndGetObjectIds(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alana\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}")
        ));

        BucketService bucketService = context.getService(BucketService.NAME);
        BucketMetadata metadata;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
        }
        long maxTtl = context.getConfig().getInt("bucket.plan_cache.max_ttl");

        BqlExpr exprAl = BqlParser.parse("{\"name\": {\"$regex\": \"^Al\"}}".getBytes());
        BqlExpr exprBo = BqlParser.parse("{\"name\": {\"$regex\": \"^Bo\"}}".getBytes());

        // Same shape across different patterns
        assertEquals(QueryShape.compute(exprAl), QueryShape.compute(exprBo),
                "Regex queries on the same field share the same shape");

        // The differing patterns are extracted as differing parameters
        List<BqlValue> paramsAl = ParameterExtractor.extract(exprAl);
        List<BqlValue> paramsBo = ParameterExtractor.extract(exprBo);
        assertNotEquals(paramsAl, paramsBo, "Different patterns must produce different parameters");

        PipelineNode plan1 = bucketService.getPlanner().plan(context, metadata, exprAl, paramsAl, true, maxTtl);
        PipelineNode plan2 = bucketService.getPlanner().plan(context, metadata, exprBo, paramsBo, true, maxTtl);
        assertSame(plan1, plan2, "Same-shape regex queries should reuse the cached plan");

        // End-to-end: the cached plan applies each query's own pattern, not a stale one
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf1 = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": {\"$regex\": \"^Al\"}}").encode(buf1);
        List<BsonDocument> al = extractEntries(runCommand(channel, buf1));
        assertEquals(2, al.size(), "'^Al' should match Alice and Alana");

        ByteBuf buf2 = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": {\"$regex\": \"^Bo\"}}").encode(buf2);
        List<BsonDocument> bo = extractEntries(runCommand(channel, buf2));
        assertEquals(1, bo.size(), "'^Bo' should match only Bob");
        assertEquals("Bob", BsonHelper.getString(bo.getFirst(), "name"));
    }

    @Test
    void shouldInvalidatePlanCacheOnBucketMetadataUpdatedEvent() {
        // Insert a document to create the bucket
        insertDocumentsAndGetObjectIds(List.of(BSONUtil.jsonToDocumentThenBytes("{\"age\": 30}")));

        BucketService bucketService = context.getService(BucketService.NAME);
        PlanCache planCache = bucketService.getPlanCache();

        BucketMetadata metadata;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
        }

        byte[] query = "{\"age\": {\"$eq\": 30}}".getBytes();
        long maxTtl = context.getConfig().getInt("bucket.plan_cache.max_ttl");

        BqlExpr expr = BqlParser.parse(query);
        List<BqlValue> parameters = expr != null
                ? ParameterExtractor.extract(expr)
                : List.of();

        // Plan a query to populate the cache
        bucketService.getPlanner().plan(context, metadata, expr, parameters, true, maxTtl);

        // Verify plan is cached
        BqlExpr bqlExpr = BqlParser.parse(query);
        long shapeHash = QueryShape.compute(bqlExpr);
        assertNotNull(planCache.get(metadata.namespace(), metadata.uuid(), shapeHash),
                "Plan should be cached");

        // Publish BUCKET_METADATA_UPDATED_EVENT
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);
            tr.commit().join();
        }

        // Wait for async event processing to invalidate the cache
        final BucketMetadata finalMetadata = metadata;
        await().atMost(Duration.ofSeconds(15)).until(() ->
                planCache.get(finalMetadata.namespace(), finalMetadata.uuid(), shapeHash) == null
        );
    }

    @Test
    void shouldInvalidatePlanCacheOnBucketRemovedEvent() {
        // Insert a document to create the bucket
        insertDocumentsAndGetObjectIds(List.of(BSONUtil.jsonToDocumentThenBytes("{\"age\": 30}")));

        BucketService bucketService = context.getService(BucketService.NAME);
        PlanCache planCache = bucketService.getPlanCache();

        BucketMetadata metadata;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
        }

        byte[] query = "{\"age\": {\"$eq\": 30}}".getBytes();
        long maxTtl = context.getConfig().getInt("bucket.plan_cache.max_ttl");

        BqlExpr expr = BqlParser.parse(query);
        List<BqlValue> parameters = expr != null
                ? ParameterExtractor.extract(expr)
                : List.of();

        // Plan a query to populate the cache
        bucketService.getPlanner().plan(context, metadata, expr, parameters, true, maxTtl);

        // Verify the plan is cached
        BqlExpr bqlExpr = BqlParser.parse(query);
        long shapeHash = QueryShape.compute(bqlExpr);
        assertNotNull(planCache.get(metadata.namespace(), metadata.uuid(), shapeHash),
                "Plan should be cached");

        // Set the bucket as removed (this publishes BUCKET_REMOVED_EVENT)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            BucketMetadataUtil.setRemoved(tx, metadata);
            tr.commit().join();
        }

        // Wait for async event processing to invalidate the cache
        final BucketMetadata finalMetadata = metadata;
        await().atMost(Duration.ofSeconds(15)).until(() ->
                planCache.get(finalMetadata.namespace(), finalMetadata.uuid(), shapeHash) == null
        );
    }

    @Test
    void shouldDoPhysicalFullScanWithoutOperator() {
        Map<ObjectId, byte[]> expectedDocument = insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(expectedDocument.size(), entries.size());
        for (BsonDocument doc : entries) {
            ObjectId id = doc.getObjectId("_id").getValue();
            assertNotNull(expectedDocument.get(id));

            // Compare without _id since original bytes don't have _id
            BsonDocument expected = BSONUtil.toBsonDocument(expectedDocument.get(id));
            BsonDocument actual = doc.clone();
            actual.remove("_id");
            assertEquals(expected, actual);
        }
    }

    @Test
    void shouldDoPhysicalFullScanWithoutOperator_RESP2() {
        Map<ObjectId, byte[]> expectedDocument = insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP2);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        ArrayRedisMessage actualMessage = (ArrayRedisMessage) ((ArrayRedisMessage) msg).children().get(1);
        for (RedisMessage entry : actualMessage.children()) {
            FullBulkStringRedisMessage bulk = (FullBulkStringRedisMessage) entry;
            byte[] docBytes = ByteBufUtil.getBytes(bulk.content());
            BsonDocument doc = BSONUtil.toBsonDocument(docBytes);
            ObjectId id = doc.getObjectId("_id").getValue();
            assertNotNull(expectedDocument.get(id));

            BsonDocument expected = BSONUtil.toBsonDocument(expectedDocument.get(id));
            BsonDocument actual = doc.clone();
            actual.remove("_id");
            assertEquals(expected, actual);
        }
    }

    @Test
    void shouldDoPhysicalIndexScanWithSingleOperator_DefaultIDIndex_EQ() {
        Map<ObjectId, byte[]> expectedDocument = insertDocumentsAndGetObjectIds(makeDummyDocument(3));

        // Find the document in the middle
        ObjectId[] keys = expectedDocument.keySet().toArray(new ObjectId[0]);
        ObjectId expectedKey = keys[1];

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, String.format("{_id: {$eq: \"%s\"}}", expectedKey.toHexString())).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size());

        BsonDocument doc = entries.get(0);
        ObjectId resultKey = doc.getObjectId("_id").getValue();
        assertEquals(expectedKey, resultKey);

        BsonDocument expected = BSONUtil.toBsonDocument(expectedDocument.get(expectedKey));
        BsonDocument actual = doc.clone();
        actual.remove("_id");
        assertEquals(expected, actual);
    }

    @Test
    void shouldDoPhysicalIndexScanWithSingleOperator_DefaultIDIndex_GTE() {
        Map<ObjectId, byte[]> expectedDocument = insertDocumentsAndGetObjectIds(makeDummyDocument(10));

        // Find the document in the middle
        ObjectId[] keys = expectedDocument.keySet().toArray(new ObjectId[0]);
        ObjectId key = keys[4];
        List<ObjectId> excludedKeys = Arrays.asList(keys).subList(0, 4);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, String.format("{_id: {$gte: \"%s\"}}", key.toHexString())).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(6, entries.size());
        int index = 4;
        for (BsonDocument doc : entries) {
            ObjectId resultKey = doc.getObjectId("_id").getValue();
            assertEquals(keys[index], resultKey);
            assertFalse(excludedKeys.contains(resultKey));

            BsonDocument expected = BSONUtil.toBsonDocument(expectedDocument.get(resultKey));
            BsonDocument actual = doc.clone();
            actual.remove("_id");
            assertEquals(expected, actual);
            index++;
        }
    }

    @Test
    void shouldDoPhysicalIndexScanWithSingleOperator_DefaultIDIndex_GT() {
        Map<ObjectId, byte[]> expectedDocument = insertDocumentsAndGetObjectIds(makeDummyDocument(10));

        ObjectId[] keys = expectedDocument.keySet().toArray(new ObjectId[0]);
        ObjectId key = keys[4];
        List<ObjectId> excludedKeys = Arrays.asList(keys).subList(0, 5);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, String.format("{_id: {$gt: \"%s\"}}", key.toHexString())).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(5, entries.size());
        int index = 5;
        for (BsonDocument doc : entries) {
            ObjectId resultKey = doc.getObjectId("_id").getValue();
            assertEquals(keys[index], resultKey);
            assertFalse(excludedKeys.contains(resultKey));

            BsonDocument expected = BSONUtil.toBsonDocument(expectedDocument.get(resultKey));
            BsonDocument actual = doc.clone();
            actual.remove("_id");
            assertEquals(expected, actual);
            index++;
        }
    }

    @Test
    void shouldDoPhysicalIndexScanWithSingleOperator_DefaultIDIndex_LT() {
        Map<ObjectId, byte[]> expectedDocument = insertDocumentsAndGetObjectIds(makeDummyDocument(10));

        // Find the document in the middle
        ObjectId[] keys = expectedDocument.keySet().toArray(new ObjectId[0]);
        ObjectId key = keys[4];
        List<ObjectId> excludedKeys = Arrays.asList(keys).subList(4, 9);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        // Query should retrieve the first two documents we inserted
        cmd.query(TEST_BUCKET, String.format("{_id: {$lt: \"%s\"}}", key.toHexString())).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(4, entries.size());

        int index = 0;
        // The first two documents, the last one is excluded by the query.
        for (BsonDocument doc : entries) {
            ObjectId resultKey = doc.getObjectId("_id").getValue();
            assertEquals(keys[index], resultKey);
            assertFalse(excludedKeys.contains(resultKey));

            BsonDocument expected = BSONUtil.toBsonDocument(expectedDocument.get(resultKey));
            BsonDocument actual = doc.clone();
            actual.remove("_id");
            assertEquals(expected, actual);
            index++;
        }
    }

    @Test
    void shouldDoPhysicalIndexScanWithSingleOperator_DefaultIDIndex_LTE() {
        Map<ObjectId, byte[]> expectedDocument = insertDocumentsAndGetObjectIds(makeDummyDocument(10));

        // Find the document in the middle
        ObjectId[] keys = expectedDocument.keySet().toArray(new ObjectId[0]);
        ObjectId key = keys[4];
        List<ObjectId> excludedKeys = Arrays.asList(keys).subList(5, 9);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        // Query should retrieve the first two documents we inserted
        cmd.query(TEST_BUCKET, String.format("{_id: {$lte: \"%s\"}}", key.toHexString())).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(5, entries.size());
        int index = 0;
        for (BsonDocument doc : entries) {
            ObjectId resultKey = doc.getObjectId("_id").getValue();
            assertEquals(keys[index], resultKey);
            assertFalse(excludedKeys.contains(resultKey));

            BsonDocument expected = BSONUtil.toBsonDocument(expectedDocument.get(resultKey));
            BsonDocument actual = doc.clone();
            actual.remove("_id");
            assertEquals(expected, actual);
            index++;
        }
    }

    // ========================================================================
    // FULL SCAN INTEGRATION TESTS
    // Tests for non-indexed field queries that require full bucket scans
    // ========================================================================

    @Test
    void shouldDoFullScanWithStringFieldFilter_EQ() {
        // Insert documents with varying string field values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 35}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 40}") // Duplicate name
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": {\"$eq\": \"Alice\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(2, entries.size(), "Should find 2 documents with name 'Alice'");

        // Verify that all returned documents have name = "Alice"
        for (BsonDocument doc : entries) {
            assertEquals("Alice", BsonHelper.getString(doc, "name"), "Document should have name = 'Alice'");
        }
    }

    @Test
    void shouldDoFullScanWithNumericFieldFilter_GTE() {
        // Insert documents with varying numeric field values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25, \"score\": 85.5}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30, \"score\": 92.0}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 35, \"score\": 78.5}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 28, \"score\": 95.5}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 32, \"score\": 88.0}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"age\": {\"$gte\": 30}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(3, entries.size(), "Should find 3 documents with age >= 30");

        // Verify all returned documents have age >= 30
        for (BsonDocument doc : entries) {
            int age = BsonHelper.getInteger(doc, "age");
            assertTrue(age >= 30, "Document should have age >= 30, but was " + age);
        }
    }

    @Test
    void shouldDoFullScanWithSelectiveFilter_InternalScanningEdgeCase() {
        // This tests the critical edge case we fixed: selective filters that match few documents
        // Insert many documents where only the last one matches
        List<byte[]> documents = new ArrayList<>();

        // Add 15 documents that don't match the filter
        for (int i = 0; i < 15; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes(
                    String.format("{\"category\": \"NORMAL\", \"index\": %d, \"data\": \"content-%d\"}", i, i)));
        }

        // Add one final document that matches
        documents.add(BSONUtil.jsonToDocumentThenBytes(
                "{\"category\": \"SPECIAL\", \"index\": 15, \"data\": \"final-content\"}"));

        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"category\": \"SPECIAL\"}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size(), "Should find exactly 1 document with category 'SPECIAL'");

        // Verify the returned document is the special one
        BsonDocument doc = entries.get(0);
        assertEquals("SPECIAL", BsonHelper.getString(doc, "category"), "Should be the SPECIAL category document");
        assertEquals("final-content", BsonHelper.getString(doc, "data"), "Should be the final document");
    }

    @Test
    void shouldDoFullScanWithBooleanFieldFilter() {
        // Insert documents with boolean fields
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"active\": true, \"premium\": false}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"active\": false, \"premium\": true}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"active\": true, \"premium\": true}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"active\": false, \"premium\": false}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"active\": true}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(2, entries.size(), "Should find 2 documents with active = true");

        // Verify returned documents have active = true (Alice and Charlie)
        for (BsonDocument doc : entries) {
            String name = BsonHelper.getString(doc, "name");
            assertTrue("Alice".equals(name) || "Charlie".equals(name),
                    "Should be Alice or Charlie (active=true), but was " + name);
            assertTrue(BsonHelper.getBoolean(doc, "active"), "Document should have active=true");
        }
    }

    @Test
    void shouldDoFullScanWithRangeQuery_AND_Optimization() {
        // Test range queries that should be optimized to PhysicalRangeScan
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"score\": 75}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"score\": 85}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"score\": 95}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"score\": 65}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"score\": 90}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Frank\", \"score\": 55}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        // Range query: 70 <= score <= 90
        cmd.query(TEST_BUCKET, "{\"$and\": [{\"score\": {\"$gte\": 70}}, {\"score\": {\"$lte\": 90}}]}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(3, entries.size(), "Should find 3 documents with score 70-90");

        // Verify returned documents are Alice (75), Bob (85), Eve (90)
        for (BsonDocument doc : entries) {
            String name = BsonHelper.getString(doc, "name");
            int score = BsonHelper.getInteger(doc, "score");
            assertTrue(("Alice".equals(name) && score == 75) ||
                            ("Bob".equals(name) && score == 85) ||
                            ("Eve".equals(name) && score == 90),
                    "Should be Alice (75), Bob (85), or Eve (90), but was " + name + " (" + score + ")");
            assertTrue(score >= 70 && score <= 90, "Score should be in range 70-90");
        }
    }

    @Test
    void shouldDoFullScanWithNoMatchingDocuments() {
        // Test the case where filter matches no documents - should return empty without infinite loops
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"category\": \"TYPE_A\", \"value\": 100}"),
                BSONUtil.jsonToDocumentThenBytes("{\"category\": \"TYPE_B\", \"value\": 200}"),
                BSONUtil.jsonToDocumentThenBytes("{\"category\": \"TYPE_C\", \"value\": 300}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"category\": \"NONEXISTENT_TYPE\"}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(0, entries.size(), "Should find no documents for nonexistent category");
    }

    @Test
    void shouldDoFullScanWithComplexDocument_NestedFields() {
        // Test full scan with more complex document structures
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"user\": {\"name\": \"Alice\", \"age\": 25}, \"status\": \"active\", \"tags\": [\"vip\", \"premium\"]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"user\": {\"name\": \"Bob\", \"age\": 30}, \"status\": \"inactive\", \"tags\": [\"standard\"]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"user\": {\"name\": \"Charlie\", \"age\": 35}, \"status\": \"active\", \"tags\": [\"vip\"]}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"status\": \"active\"}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(2, entries.size(), "Should find 2 documents with status 'active'");

        // Verify returned documents have status = "active" (Alice and Charlie)
        for (BsonDocument doc : entries) {
            BsonDocument user = BsonHelper.getDocument(doc, "user");
            assertNotNull(user);
            String name = BsonHelper.getString(user, "name");
            String status = BsonHelper.getString(doc, "status");
            assertTrue("Alice".equals(name) || "Charlie".equals(name),
                    "Should be Alice or Charlie (status=active), but was " + name);
            assertEquals("active", status, "Should contain status active");
        }
    }

    @Test
    void shouldDoFullScanWithMixedDataTypes() {
        // Test full scan with various BSON data types
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"string\", \"value\": \"text\", \"priority\": 1}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"number\", \"value\": 42, \"priority\": 2}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"boolean\", \"value\": true, \"priority\": 1}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"string\", \"value\": \"another\", \"priority\": 3}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"priority\": 1}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(2, entries.size(), "Should find 2 documents with priority = 1");

        // Verify returned documents have priority = 1
        for (BsonDocument doc : entries) {
            int priority = BsonHelper.getInteger(doc, "priority");
            assertEquals(1, priority, "Document should have priority = 1");
        }
    }

    @Test
    void shouldHandleFullScanWithLargeResultSet() {
        // Test full scan performance with larger dataset
        List<byte[]> documents = new ArrayList<>();
        int totalDocs = 50;
        int matchingDocs = 0;

        for (int i = 0; i < totalDocs; i++) {
            String tier = (i % 5 == 0) ? "GOLD" : "STANDARD"; // Every 5th document is GOLD
            if (tier.equals("GOLD")) matchingDocs++;

            documents.add(BSONUtil.jsonToDocumentThenBytes(
                    String.format("{\"id\": %d, \"tier\": \"%s\", \"data\": \"content-%d\"}", i, tier, i)));
        }

        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"tier\": \"GOLD\"}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(matchingDocs, entries.size(), "Should find all GOLD tier documents");

        // Verify all returned documents are GOLD tier
        for (BsonDocument doc : entries) {
            assertEquals("GOLD", BsonHelper.getString(doc, "tier"), "All returned documents should be GOLD tier");
        }
    }

    @Test
    void shouldThrowBucketBeingRemovedExceptionWhenQueryingRemovedBucket() {
        // Insert a document to create the bucket
        insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));

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

        // Try to query the dropped bucket
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertEquals("BUCKETBEINGREMOVED Bucket 'test-bucket' is being removed", errorMessage.content());
    }

    @Test
    void shouldReturnEmptyResultWhenQueryingIdGreaterThanMax() {
        List<byte[]> documents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes("{\"status\": \"ALIVE\"}"));
        }

        Map<ObjectId, byte[]> insertedDocuments = insertDocumentsAndGetObjectIds(documents);
        ObjectId[] keys = insertedDocuments.keySet().toArray(new ObjectId[0]);
        ObjectId greatestId = keys[keys.length - 1];

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, String.format("{\"_id\": {\"$gt\": \"%s\"}}", greatestId.toHexString())).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(0, entries.size());
    }

    @Test
    void shouldReturnAllDocumentsWhenQueryingIdGteFirstDocument() {
        // Behavior: $gte on _id with the first document's _id should return all documents
        // since all subsequent _ids are greater than or equal to the first.

        List<byte[]> documents = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes("{\"status\": \"ALIVE\"}"));
        }

        Map<ObjectId, byte[]> insertedDocuments = insertDocumentsAndGetObjectIds(documents);
        ObjectId[] keys = insertedDocuments.keySet().toArray(new ObjectId[0]);
        ObjectId firstId = keys[0];

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, String.format("{\"_id\": {\"$gte\": \"%s\"}}", firstId.toHexString())).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(5, entries.size(), "Should return all 5 documents");
    }

    @Test
    void shouldReadInsertedDocumentWithinSameTransaction() {
        // Behavior: Documents inserted within a BEGIN/COMMIT transaction are immediately readable
        // by QUERY within the same transaction (read-your-writes).

        createBucket(TEST_BUCKET);

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        BucketCommandBuilder<String, String> bucketCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(bucketCmd, RESPVersion.RESP3);

        // Step 1: BEGIN transaction
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }

        // Step 2: INSERT document within transaction
        {
            byte[] doc = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"TestDoc\", \"value\": 42}");
            ByteBuf buf = Unpooled.buffer();
            insertCmd.insert(TEST_BUCKET, doc).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
        }

        // Step 3: QUERY the document within the same transaction - should succeed (read-your-writes)
        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.query(TEST_BUCKET, "{\"name\": \"TestDoc\"}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size());

            BsonDocument doc = entries.getFirst();
            assertTrue(doc.containsKey("_id"), "Document should have server-injected _id field");
            assertEquals("TestDoc", doc.getString("name").getValue());
            assertEquals(42, doc.getInt32("value").getValue());
        }

        // Step 4: COMMIT transaction
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.commit().encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }
    }

    @Test
    void shouldQueryCommittedAndUncommittedDocumentsTogether() {
        // Behavior: Querying within a transaction returns both previously committed documents
        // and documents inserted in the current uncommitted transaction.

        // Step 1: Insert a document that gets auto-committed
        byte[] committedDoc = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"CommittedDoc\", \"value\": 1}");
        insertDocumentsAndGetObjectIds(List.of(committedDoc));

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        BucketCommandBuilder<String, String> bucketCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(bucketCmd, RESPVersion.RESP3);

        // Step 2: BEGIN transaction
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }

        // Step 3: INSERT a second document within the transaction
        {
            byte[] doc = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"UncommittedDoc\", \"value\": 2}");
            ByteBuf buf = Unpooled.buffer();
            insertCmd.insert(TEST_BUCKET, doc).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
        }

        // Step 4: QUERY all documents — should return both committed and uncommitted
        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(2, entries.size());

            Map<String, BsonDocument> byName = new HashMap<>();
            for (BsonDocument doc : entries) {
                assertTrue(doc.containsKey("_id"), "Document should have server-injected _id field");
                byName.put(doc.getString("name").getValue(), doc);
            }

            BsonDocument committed = byName.get("CommittedDoc");
            assertNotNull(committed, "CommittedDoc should be present");
            assertEquals(1, committed.getInt32("value").getValue());

            BsonDocument uncommitted = byName.get("UncommittedDoc");
            assertNotNull(uncommitted, "UncommittedDoc should be present");
            assertEquals(2, uncommitted.getInt32("value").getValue());
        }

        // Step 5: COMMIT transaction
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.commit().encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }
    }

    @Test
    void shouldDiscardUncommittedDocumentsAfterRollback() {
        // Behavior: Documents inserted within a transaction are visible via RYW(read-your-writes) before rollback,
        // but after rollback they disappear and only previously committed documents remain.

        // Step 1: Insert a document that gets auto-committed
        byte[] committedDoc = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"CommittedDoc\", \"value\": 1}");
        insertDocumentsAndGetObjectIds(List.of(committedDoc));

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        BucketCommandBuilder<String, String> bucketCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(bucketCmd, RESPVersion.RESP3);

        // Step 2: BEGIN transaction
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }

        // Step 3: INSERT a second document within the transaction
        {
            byte[] doc = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"UncommittedDoc\", \"value\": 2}");
            ByteBuf buf = Unpooled.buffer();
            insertCmd.insert(TEST_BUCKET, doc).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
        }

        // Step 4: QUERY all documents — should return both committed and uncommitted (read-your-writes)
        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(2, entries.size());
        }

        // Step 5: ROLLBACK transaction
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.rollback().encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }

        // Step 6: BEGIN a fresh transaction and QUERY — only the committed document should remain
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size());

            BsonDocument doc = entries.getFirst();
            assertTrue(doc.containsKey("_id"), "Document should have server-injected _id field");
            assertEquals("CommittedDoc", doc.getString("name").getValue());
            assertEquals(1, doc.getInt32("value").getValue());
        }
    }

    @Test
    void shouldQueryCommittedDataWithSnapshotRead() {
        // Behavior: Enabling snapshot read via SNAPSHOTREAD ON within a transaction
        // allows reading previously committed data without conflicting with concurrent writes.

        // Step 1: Insert a document that gets auto-committed
        byte[] committedDoc = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"SnapshotDoc\", \"value\": 99}");
        insertDocumentsAndGetObjectIds(List.of(committedDoc));

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        BucketCommandBuilder<String, String> bucketCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(bucketCmd, RESPVersion.RESP3);

        // Step 2: BEGIN transaction
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }

        // Step 3: Enable snapshot read
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.snapshotRead(SnapshotReadArgs.Builder.on()).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }

        // Step 4: QUERY the committed document via snapshot read
        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size());

            BsonDocument doc = entries.getFirst();
            assertTrue(doc.containsKey("_id"), "Document should have server-injected _id field");
            assertEquals("SnapshotDoc", doc.getString("name").getValue());
            assertEquals(99, doc.getInt32("value").getValue());
        }
    }

    @Test
    void shouldPreserveBucketDataAfterMovingNamespace() {
        // Behavior: Verifies that bucket data (documents) remains accessible after moving a namespace
        // using NAMESPACE MOVE. The move delegates to FoundationDB's DirectoryLayer.move(), which
        // atomically relocates the entire directory tree.

        String oldNamespace = "a.b.c";
        String newNamespace = "a.b.d";
        namespace = "a.b";

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // Create namespace a.b.c
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(oldNamespace).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Create bucket in a.b.c
        createBucket(channel, oldNamespace, TEST_BUCKET);

        // Insert documents
        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        byte[] doc1 = BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30}");
        byte[] doc2 = BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25}");
        byte[] doc3 = BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 35}");
        {
            ByteBuf buf = Unpooled.buffer();
            insertCmd.insert(TEST_BUCKET, doc1, doc2, doc3).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
            assertEquals(3, ((ArrayRedisMessage) response).children().size());
        }

        // Query before move to verify documents exist
        BucketCommandBuilder<String, String> queryCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(queryCmd, RESPVersion.RESP3);
        {
            ByteBuf buf = Unpooled.buffer();
            queryCmd.query(TEST_BUCKET, "{}").encode(buf);
            Object response = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(response);
            assertEquals(3, entries.size(), "Should have 3 documents before move");
        }

        // Move namespace a.b.c -> a.b.d
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceMove(oldNamespace, newNamespace).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Verify the old namespace is gone
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceExists(oldNamespace).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            assertEquals(0, ((IntegerRedisMessage) response).value());
        }

        // Switch to the new namespace
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse(newNamespace).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Query bucket under the new namespace — data should be preserved
        {
            ByteBuf buf = Unpooled.buffer();
            queryCmd.query(TEST_BUCKET, "{}").encode(buf);
            Object response = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(response);
            assertEquals(3, entries.size(), "Should still have 3 documents after move");
        }
    }

    // --- Projection tests ---

    @Test
    void shouldProjectIncludedFieldsOnly() {
        // Behavior: PROJECTION with inclusion mode returns only specified fields plus _id.
        List<byte[]> docs = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30, \"email\": \"alice@test.com\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 25, \"email\": \"bob@test.com\"}")
        );
        insertDocumentsAndGetObjectIds(docs);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.projection("{\"name\": 1}")).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(2, entries.size());
        for (BsonDocument doc : entries) {
            assertTrue(doc.containsKey("_id"), "Projected document should include _id by default");
            assertTrue(doc.containsKey("name"), "Projected document should include 'name'");
            assertFalse(doc.containsKey("age"), "Projected document should not include 'age'");
            assertFalse(doc.containsKey("email"), "Projected document should not include 'email'");
        }
    }

    @Test
    void shouldProjectExcludedFields() {
        // Behavior: PROJECTION with exclusion mode returns all fields except the specified ones.
        List<byte[]> docs = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30, \"email\": \"alice@test.com\"}")
        );
        insertDocumentsAndGetObjectIds(docs);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.projection("{\"email\": 0}")).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size());
        BsonDocument doc = entries.getFirst();
        assertTrue(doc.containsKey("_id"));
        assertTrue(doc.containsKey("name"));
        assertTrue(doc.containsKey("age"));
        assertFalse(doc.containsKey("email"), "Excluded field 'email' should not be present");
    }

    @Test
    void shouldExcludeIdFieldWithProjection() {
        // Behavior: PROJECTION with {"_id": 0, "name": 1} excludes _id from results.
        List<byte[]> docs = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}")
        );
        insertDocumentsAndGetObjectIds(docs);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.projection("{\"_id\": 0, \"name\": 1}")).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size());
        BsonDocument doc = entries.getFirst();
        assertFalse(doc.containsKey("_id"), "_id should be excluded");
        assertTrue(doc.containsKey("name"));
        assertFalse(doc.containsKey("age"));
    }

    @Test
    void shouldProjectNestedFields() {
        // Behavior: PROJECTION with dot notation includes only the specified nested field.
        List<byte[]> docs = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"address\": {\"city\": \"Istanbul\", \"zip\": \"34000\"}}")
        );
        insertDocumentsAndGetObjectIds(docs);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.projection("{\"address.city\": 1}")).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size());
        BsonDocument doc = entries.getFirst();
        assertTrue(doc.containsKey("_id"));
        assertFalse(doc.containsKey("name"), "'name' should not be present");
        assertTrue(doc.containsKey("address"));

        BsonDocument address = doc.getDocument("address");
        assertTrue(address.containsKey("city"));
        assertFalse(address.containsKey("zip"), "'zip' should not be present in projected nested doc");
        assertEquals("Istanbul", address.getString("city").getValue());
    }

    @Test
    void shouldProjectWithLimit() {
        // Behavior: PROJECTION combined with LIMIT returns projected documents up to the limit.
        List<byte[]> docs = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 35}")
        );
        insertDocumentsAndGetObjectIds(docs);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}", new BucketQueryArgs().projection("{\"name\": 1}").limit(2)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(2, entries.size());
        for (BsonDocument doc : entries) {
            assertTrue(doc.containsKey("_id"));
            assertTrue(doc.containsKey("name"));
            assertFalse(doc.containsKey("age"), "Projected document should not include 'age'");
        }
    }

    @Test
    void shouldProjectWithResultSort() {
        // Behavior: PROJECTION combined with RESULTSORT returns projected documents in sorted order.
        insertDocumentsAndGetObjectIds(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 35}")
        ));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}", new BucketQueryArgs().projection("{\"name\": 1, \"age\": 1}").resultSort("age", "ASC")).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(3, entries.size());

        // Verify sorted order and projection
        assertEquals(25, entries.get(0).getInt32("age").getValue());
        assertEquals(30, entries.get(1).getInt32("age").getValue());
        assertEquals(35, entries.get(2).getInt32("age").getValue());
        for (BsonDocument doc : entries) {
            assertTrue(doc.containsKey("name"));
            assertTrue(doc.containsKey("age"));
            assertTrue(doc.containsKey("_id"));
            assertEquals(3, doc.size(), "Document should only have _id, name, and age");
        }
    }

    @Test
    void shouldReturnAllFieldsWithoutProjection() {
        // Behavior: When no PROJECTION is specified, all fields are returned (existing behavior preserved).
        List<byte[]> docs = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30, \"email\": \"alice@test.com\"}")
        );
        insertDocumentsAndGetObjectIds(docs);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size());
        BsonDocument doc = entries.getFirst();
        assertTrue(doc.containsKey("_id"));
        assertTrue(doc.containsKey("name"));
        assertTrue(doc.containsKey("age"));
        assertTrue(doc.containsKey("email"));
    }

    @Test
    void shouldProjectWithAdvance() {
        // Behavior: Projection applies consistently across cursor pages via BUCKET.ADVANCE.
        List<byte[]> docs = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 35}")
        );
        insertDocumentsAndGetObjectIds(docs);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // First page: LIMIT 2 with projection
        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}", new BucketQueryArgs().projection("{\"name\": 1}").limit(2)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> firstPage = extractEntries(msg);
        assertEquals(2, firstPage.size());
        for (BsonDocument doc : firstPage) {
            assertTrue(doc.containsKey("_id"));
            assertTrue(doc.containsKey("name"));
            assertFalse(doc.containsKey("age"), "First page should not include 'age'");
        }

        // Advance cursor for second page
        int cursorId = extractCursorId(msg);
        ByteBuf advanceBuf = Unpooled.buffer();
        cmd.advanceQuery(cursorId).encode(advanceBuf);
        Object advanceMsg = runCommand(channel, advanceBuf);
        assertInstanceOf(MapRedisMessage.class, advanceMsg);

        List<BsonDocument> secondPage = extractEntries(advanceMsg);
        assertEquals(1, secondPage.size());
        BsonDocument doc = secondPage.getFirst();
        assertTrue(doc.containsKey("_id"));
        assertTrue(doc.containsKey("name"));
        assertFalse(doc.containsKey("age"), "Second page (via ADVANCE) should also not include 'age'");
    }

    // --- Collation tests ---

    @Test
    void shouldQueryWithTurkishCollation() {
        // Behavior: QUERY with Turkish PRIMARY collation matches i and İ as equivalent.
        insertDocumentsAndGetObjectIds(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"istanbul\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"\u0130stanbul\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"ankara\"}")
        ));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": {\"$eq\": \"\u0130stanbul\"}}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"tr\", \"strength\": 1}")).encode(buf);
        Object msg = runCommand(channel, buf);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(2, entries.size());
    }

    @Test
    void shouldQueryWithFrenchSecondaryCollation() {
        // Behavior: QUERY with French SECONDARY collation is case-insensitive but accent-sensitive.
        insertDocumentsAndGetObjectIds(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"cafe\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Cafe\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"caf\u00e9\"}")
        ));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": {\"$eq\": \"cafe\"}}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"fr\", \"strength\": 2}")).encode(buf);
        Object msg = runCommand(channel, buf);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(2, entries.size());
    }

    @Test
    void shouldQueryWithCollationForcingFullScan() {
        // Behavior: QUERY with collation mismatching index forces full scan but returns correct results.
        insertDocumentsAndGetObjectIds(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"istanbul\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"\u0130stanbul\"}")
        ));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);

        ByteBuf indexBuf = Unpooled.buffer();
        cmd.indexCreate(TEST_BUCKET, "{\"name\": {\"bson_type\": \"string\", \"collation\": {\"locale\": \"en\", \"strength\": 1}}}").encode(indexBuf);
        Object indexResult = runCommand(channel, indexBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, indexResult);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Index nameIndex = metadata.indexes().getIndex("name", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(nameIndex.subspace());
        }
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, TEST_BUCKET);

        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": {\"$eq\": \"\u0130stanbul\"}}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"tr\", \"strength\": 1}")).encode(buf);
        Object msg = runCommand(channel, buf);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(2, entries.size());
    }

    @Test
    void shouldQueryWithNoCollationPreservingBehavior() {
        // Behavior: QUERY without COLLATION uses binary comparison (exact match only).
        insertDocumentsAndGetObjectIds(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"istanbul\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"\u0130stanbul\"}")
        ));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": {\"$eq\": \"\u0130stanbul\"}}").encode(buf);
        Object msg = runCommand(channel, buf);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size());
    }

    @Test
    void shouldQueryWithCollationOverridingBucket() {
        // Behavior: Query-level collation overrides bucket-level collation.
        createBucketWithCollation(COLLATION_BUCKET, "{\"locale\": \"en\", \"strength\": 1}");

        insertDocumentsIntoBucket(COLLATION_BUCKET, List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"cafe\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Cafe\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"caf\u00e9\"}")
        ));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(COLLATION_BUCKET, "{\"name\": {\"$eq\": \"cafe\"}}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"fr\", \"strength\": 3}")).encode(buf);
        Object msg = runCommand(channel, buf);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size());
        assertEquals("cafe", BsonHelper.getString(entries.getFirst(), "name"));
    }

    @Test
    void shouldQueryWithCollationAndLimit() {
        // Behavior: QUERY with collation and LIMIT returns at most LIMIT entries.
        insertDocumentsAndGetObjectIds(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"istanbul\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"\u0130stanbul\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"ankara\"}")
        ));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": {\"$eq\": \"\u0130stanbul\"}}",
                new BucketQueryArgs().collation("{\"locale\": \"tr\", \"strength\": 1}").limit(1)).encode(buf);
        Object msg = runCommand(channel, buf);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size());
    }

    @Test
    void shouldQueryWithCollationAndSortBy() {
        // Behavior: QUERY with collation matching index collation supports SORTBY on indexed field.
        createBucketWithCollation(COLLATION_BUCKET, "{\"locale\": \"tr\", \"strength\": 3}");

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);

        ByteBuf indexBuf = Unpooled.buffer();
        cmd.indexCreate(COLLATION_BUCKET, "{\"name\": {\"bson_type\": \"string\", \"collation\": {\"locale\": \"tr\", \"strength\": 3}}}").encode(indexBuf);
        Object indexResult = runCommand(channel, indexBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, indexResult);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, COLLATION_BUCKET);
            Index nameIndex = metadata.indexes().getIndex("name", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(nameIndex.subspace());
        }
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, COLLATION_BUCKET);

        insertDocumentsIntoBucket(COLLATION_BUCKET, List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"zebra\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"ankara\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"istanbul\"}")
        ));

        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(COLLATION_BUCKET, "{\"name\": {\"$gte\": \"\"}}",
                new BucketQueryArgs().collation("{\"locale\": \"tr\", \"strength\": 3}").sortBy("name", "ASC")).encode(buf);
        Object msg = runCommand(channel, buf);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(3, entries.size());
        assertEquals("ankara", BsonHelper.getString(entries.get(0), "name"));
        assertEquals("istanbul", BsonHelper.getString(entries.get(1), "name"));
        assertEquals("zebra", BsonHelper.getString(entries.get(2), "name"));
    }

    @Test
    void shouldRejectInvalidCollationLocale() {
        // Behavior: QUERY with invalid collation locale returns an error.
        insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": {\"$eq\": \"test\"}}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"xyz_invalid\"}")).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
    }

    @Test
    void shouldRejectInvalidCollationStrength() {
        // Behavior: QUERY with invalid collation strength returns an error.
        insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": {\"$eq\": \"test\"}}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"en\", \"strength\": 99}")).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
    }

    @Test
    void shouldRejectMalformedCollationJson() {
        // Behavior: QUERY with malformed collation JSON returns an error.
        insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": {\"$eq\": \"test\"}}",
                BucketQueryArgs.Builder.collation("not-json")).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
    }

    @Test
    void shouldRejectQueryWithSortByCollationMismatchOnIndex() {
        // Behavior: QUERY with SORTBY on a field whose index has mismatched collation
        // returns an error — the index cannot provide correct sort ordering.
        createBucketWithCollation(COLLATION_BUCKET, "{\"locale\": \"tr\", \"strength\": 3}");

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);

        ByteBuf indexBuf = Unpooled.buffer();
        cmd.indexCreate(COLLATION_BUCKET, "{\"name\": {\"bson_type\": \"string\", \"collation\": {\"locale\": \"en\", \"strength\": 3}}}").encode(indexBuf);
        Object indexResult = runCommand(channel, indexBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, indexResult);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, COLLATION_BUCKET);
            Index nameIndex = metadata.indexes().getIndex("name", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(nameIndex.subspace());
        }
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, COLLATION_BUCKET);

        insertDocumentsIntoBucket(COLLATION_BUCKET, List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"zebra\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"ankara\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"istanbul\"}")
        ));

        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(COLLATION_BUCKET, "{\"name\": {\"$gte\": \"\"}}",
                new BucketQueryArgs().collation("{\"locale\": \"tr\", \"strength\": 3}").sortBy("name", "ASC")).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("collation does not match the query collation"));
    }

    @Test
    void shouldThrowErrorWhenSortByAndResultSortUseTheSameField() {
        // Behavior: SORTBY and RESULTSORT cannot both target the same field — it is redundant and semantically invalid.
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}", new BucketQueryArgs().sortBy("age", "ASC").resultSort("age", "ASC")).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("SORTBY and RESULTSORT cannot use the same field"));
    }

    @Test
    void shouldMatchStringFieldWithRegex() {
        // Behavior: $regex selects documents whose string field matches the pattern, via a full-scan residual filter.
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alana\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": {\"$regex\": \"^Al\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(2, entries.size(), "Should match the two names starting with 'Al'");
        for (BsonDocument doc : entries) {
            assertThat(BsonHelper.getString(doc, "name")).startsWith("Al");
        }
    }

    @Test
    void shouldMatchRegexCaseInsensitivelyWithOption() {
        // Behavior: the i option makes $regex matching case-insensitive.
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"ALICE\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": {\"$regex\": \"^alice$\", \"$options\": \"i\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(2, entries.size(), "Case-insensitive match should find both 'Alice' and 'ALICE'");
    }

    @Test
    void shouldNotMatchRegexAgainstNonStringField() {
        // Behavior: $regex never matches a non-string field value, only the string-typed field.
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"code\": \"123\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"code\": 123}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"code\": {\"$regex\": \"12\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size(), "Only the string-typed 'code' should match");
        assertEquals("123", BsonHelper.getString(entries.getFirst(), "code"));
    }

    @Test
    void shouldMatchRegexAgainstArrayElement() {
        // Behavior: $regex matches if any string element of an array field matches the pattern.
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"tags\": [\"red\", \"green\"]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"tags\": [\"blue\"]}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"tags\": {\"$regex\": \"een$\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size(), "Only the document with a matching array element should be returned");
    }

    @Test
    void shouldNegateRegexWithNot() {
        // Behavior: $not over $regex returns documents whose string field does not match the pattern.
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alana\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": {\"$not\": {\"$regex\": \"^Al\"}}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size(), "Only 'Bob' should remain after negating the prefix match");
        assertEquals("Bob", BsonHelper.getString(entries.getFirst(), "name"));
    }

    @Test
    void shouldReturnNonStringAndMissingFieldsWhenNegatingRegex() {
        // Behavior: $not over $regex returns documents whose field is missing, null, or non-string,
        // since none of them match the pattern.
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": 1988}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": null}"),
                BSONUtil.jsonToDocumentThenBytes("{\"city\": \"X\"}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": {\"$not\": {\"$regex\": \"^Al\"}}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(4, entries.size(),
                "Only 'Alice' matches the pattern; every other document (string, number, null, missing) is returned");
        boolean aliceReturned = entries.stream().anyMatch(doc -> {
            BsonValue name = doc.get("name");
            return name != null && name.isString() && "Alice".equals(name.asString().getValue());
        });
        assertFalse(aliceReturned, "'Alice' matches the pattern, so $not must exclude it");
    }

    @Test
    void shouldMatchWithRegexLiteralsInIn() {
        // Behavior: $in with regex literals (sent as BSON) matches a string field against any pattern.
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Carol\"}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$in", new BsonArray(List.of(
                new BsonRegularExpression("^Al", ""),
                new BsonRegularExpression("^Bo", "")))));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, BSONUtil.toBytes(query)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(2, entries.size(), "Alice and Bob match the regex alternatives");
        Set<String> names = new HashSet<>();
        for (BsonDocument doc : entries) {
            names.add(BsonHelper.getString(doc, "name"));
        }
        assertEquals(Set.of("Alice", "Bob"), names);
    }

    @Test
    void shouldMixStringAndRegexLiteralsInIn() {
        // Behavior: $in may combine an exact string with a regex literal; either alternative matches.
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Carol\"}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$in", new BsonArray(List.of(
                new BsonString("Carol"),
                new BsonRegularExpression("^Al", "")))));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, BSONUtil.toBytes(query)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(2, entries.size(), "Carol matches the literal, Alice matches the regex");
    }

    @Test
    void shouldExcludeWithRegexLiteralsInNin() {
        // Behavior: $nin with a regex literal excludes documents whose field matches the pattern.
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alana\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$nin", new BsonArray(List.of(
                new BsonRegularExpression("^Al", "")))));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, BSONUtil.toBytes(query)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size(), "Only 'Bob' survives the regex exclusion");
        assertEquals("Bob", BsonHelper.getString(entries.getFirst(), "name"));
    }

    @Test
    void shouldRequireAllRegexLiteralsInAll() {
        // Behavior: $all with regex literals requires every pattern to match some array element.
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"tags\": [\"alpha\", \"beta\"]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"tags\": [\"alpha\", \"gamma\"]}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BsonDocument query = new BsonDocument("tags", new BsonDocument("$all", new BsonArray(List.of(
                new BsonRegularExpression("^al", ""),
                new BsonRegularExpression("^be", "")))));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, BSONUtil.toBytes(query)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size(), "Only the document with both an 'al' and a 'be' tag matches");
    }

    @Test
    void shouldMatchCaseInsensitiveRegexLiteralInIn() {
        // Behavior: the i option on a regex literal inside $in makes matching case-insensitive.
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"ALICE\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$in", new BsonArray(List.of(
                new BsonRegularExpression("^alice$", "i")))));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, BSONUtil.toBytes(query)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size(), "Case-insensitive regex matches 'ALICE'");
        assertEquals("ALICE", BsonHelper.getString(entries.getFirst(), "name"));
    }

    @Test
    void shouldReturnCorrectResultsForRegexInInWhenIndexExists() {
        // Behavior: a regex element in $in cannot use the field index, so the query falls back to a full
        // scan and still returns correct results even when an index on the field is present.
        insertDocumentsAndGetObjectIds(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Carol\"}")
        ));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);

        ByteBuf indexBuf = Unpooled.buffer();
        cmd.indexCreate(TEST_BUCKET, "{\"name\": {\"bson_type\": \"string\"}}").encode(indexBuf);
        Object indexResult = runCommand(channel, indexBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, indexResult);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Index nameIndex = metadata.indexes().getIndex("name", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(nameIndex.subspace());
        }
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, TEST_BUCKET);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$in", new BsonArray(List.of(
                new BsonRegularExpression("^Al", "")))));

        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, BSONUtil.toBytes(query)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size(), "Only 'Alice' matches the regex, regardless of the index");
        assertEquals("Alice", BsonHelper.getString(entries.getFirst(), "name"));
    }
}
