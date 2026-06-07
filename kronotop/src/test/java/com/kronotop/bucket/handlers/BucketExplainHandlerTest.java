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
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.commands.BucketQueryArgs;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BucketExplainHandlerTest extends BaseBucketHandlerTest {

    @BeforeEach
    void setUp() {
        createBucket(TEST_BUCKET);
    }

    @Test
    void shouldReturnMapForRESP3Protocol() {
        insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, "{}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;
        assertFalse(mapMessage.children().isEmpty());
        assertTrue(hasKey(mapMessage, "is_cached"));
        assertTrue(hasKey(mapMessage, "plan"));
    }

    @Test
    void shouldReturnIsCachedTrueWhenPlanIsCached() {
        insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        String query = "{$and: [{status: {$eq: \"active\"}}, {age: {$gte: 18, $lt: 65}}]}";

        // First query to cache the plan
        ByteBuf queryBuf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, query).encode(queryBuf);
        runCommand(channel, queryBuf);

        // Now explain should show is_cached = true
        ByteBuf explainBuf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, query).encode(explainBuf);
        Object msg = runCommand(channel, explainBuf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;
        assertEquals(true, getBooleanValue(mapMessage, "is_cached"));
    }

    @Test
    void shouldReturnArrayForRESP2Protocol() {
        insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP2);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, "{}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage arrayMessage = (ArrayRedisMessage) msg;
        assertFalse(arrayMessage.children().isEmpty());
    }

    @Test
    void shouldIncludePlannerVersionInResponse() {
        insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, "{}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;
        MapRedisMessage plan = getPlan(mapMessage);

        // Verify planner_version is the first entry in the plan
        Map.Entry<RedisMessage, RedisMessage> firstEntry = plan.children().entrySet().iterator().next();
        assertInstanceOf(FullBulkStringRedisMessage.class, firstEntry.getKey());
        assertEquals("planner_version", ((FullBulkStringRedisMessage) firstEntry.getKey()).content().toString(StandardCharsets.UTF_8));

        assertInstanceOf(IntegerRedisMessage.class, firstEntry.getValue());
        assertEquals(1, ((IntegerRedisMessage) firstEntry.getValue()).value());
    }

    @Test
    void shouldExplainFullScan() {
        insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, "{}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;
        MapRedisMessage plan = getPlan(mapMessage);

        assertEquals("FullScan", getStringValue(plan, "nodeType"));
        assertEquals("FULL_SCAN", getStringValue(plan, "scanType"));
        assertEquals("primary-index", getStringValue(plan, "index"));
    }

    @Test
    void shouldExplainIndexScanOnPrimaryIndex() {
        Map<ObjectId, byte[]> docs = insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));
        ObjectId docId = docs.keySet().iterator().next();

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, String.format("{_id: {$eq: \"%s\"}}", docId.toHexString())).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;
        MapRedisMessage plan = getPlan(mapMessage);

        assertEquals("IndexScan", getStringValue(plan, "nodeType"));
        assertEquals("INDEX_SCAN", getStringValue(plan, "scanType"));
        assertEquals("primary-index", getStringValue(plan, "index"));
        assertEquals("_id", getStringValue(plan, "selector"));
        assertEquals("EQ", getStringValue(plan, "operator"));
    }

    @Test
    void shouldExplainFullScanWithResidualPredicate() {
        insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        // Query on non-indexed field "one" should result in FullScan with residual predicate
        cmd.explain(TEST_BUCKET, "{one: {$eq: \"two\"}}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;
        MapRedisMessage plan = getPlan(mapMessage);

        // The root node should be FullScan
        assertEquals("FullScan", getStringValue(plan, "nodeType"));
        assertEquals("FULL_SCAN", getStringValue(plan, "scanType"));

        // Verify predicate is present
        MapRedisMessage predicate = getMapValue(plan, "predicate");
        assertNotNull(predicate);
        assertEquals("PREDICATE", getStringValue(predicate, "type"));
        assertEquals("one", getStringValue(predicate, "selector"));
        assertEquals("EQ", getStringValue(predicate, "operator"));
    }

    @Test
    void shouldIncludeNodeTypeAndId() {
        insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, "{}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;
        MapRedisMessage plan = getPlan(mapMessage);

        // Verify nodeType is present
        String nodeType = getStringValue(plan, "nodeType");
        assertNotNull(nodeType);
        assertFalse(nodeType.isEmpty());

        // Verify id is present
        Long id = getIntegerValue(plan, "id");
        assertNotNull(id);
    }

    @Test
    void shouldIncludeScanTypeForScanNodes() {
        Map<ObjectId, byte[]> docs = insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));
        ObjectId docId = docs.keySet().iterator().next();

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Test INDEX_SCAN
        ByteBuf buf1 = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, String.format("{_id: {$eq: \"%s\"}}", docId.toHexString())).encode(buf1);
        Object msg1 = runCommand(channel, buf1);
        MapRedisMessage indexScanResponse = (MapRedisMessage) msg1;
        MapRedisMessage indexScanPlan = getPlan(indexScanResponse);
        assertNotNull(indexScanPlan);
        assertEquals("INDEX_SCAN", getStringValue(indexScanPlan, "scanType"));

        // Test FULL_SCAN
        ByteBuf buf2 = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, "{}").encode(buf2);
        Object msg2 = runCommand(channel, buf2);
        MapRedisMessage fullScanResponse = (MapRedisMessage) msg2;
        MapRedisMessage fullScanPlan = getPlan(fullScanResponse);
        assertNotNull(fullScanPlan);
        assertEquals("FULL_SCAN", getStringValue(fullScanPlan, "scanType"));

        // Test RANGE_SCAN (using $gt and $lt on _id)
        ByteBuf buf3 = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, String.format("{_id: {$gte: \"%s\", $lte: \"%s\"}}", docId.toHexString(), docId.toHexString())).encode(buf3);
        Object msg3 = runCommand(channel, buf3);
        MapRedisMessage rangeScanResponse = (MapRedisMessage) msg3;
        MapRedisMessage rangeScanPlan = getPlan(rangeScanResponse);
        assertNotNull(rangeScanPlan);
        assertEquals("RANGE_SCAN", getStringValue(rangeScanPlan, "scanType"));
    }

    @Test
    void shouldExplainEQOperator() {
        Map<ObjectId, byte[]> docs = insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));
        ObjectId docId = docs.keySet().iterator().next();

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, String.format("{_id: {$eq: \"%s\"}}", docId.toHexString())).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;
        MapRedisMessage plan = getPlan(mapMessage);

        assertEquals("IndexScan", getStringValue(plan, "nodeType"));
        assertEquals("EQ", getStringValue(plan, "operator"));
        assertEquals("_id", getStringValue(plan, "selector"));
        // operand is present (value is a VersionstampVal for binary _id)
        assertTrue(hasKey(plan, "operand"));
    }

    @Test
    void shouldExplainLTOperator() {
        Map<ObjectId, byte[]> docs = insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));
        ObjectId docId = docs.keySet().iterator().next();

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, String.format("{_id: {$lt: \"%s\"}}", docId.toHexString())).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;
        MapRedisMessage plan = getPlan(mapMessage);

        assertEquals("IndexScan", getStringValue(plan, "nodeType"));
        assertEquals("LT", getStringValue(plan, "operator"));
        assertEquals("_id", getStringValue(plan, "selector"));
        assertTrue(hasKey(plan, "operand"));
    }

    @Test
    void shouldExplainRangeScan() {
        Map<ObjectId, byte[]> docs = insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));
        ObjectId docId = docs.keySet().iterator().next();

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, String.format("{_id: {$gte: \"%s\", $lte: \"%s\"}}", docId.toHexString(), docId.toHexString())).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;
        MapRedisMessage plan = getPlan(mapMessage);

        assertEquals("RangeScan", getStringValue(plan, "nodeType"));
        assertEquals("RANGE_SCAN", getStringValue(plan, "scanType"));
        assertEquals("_id", getStringValue(plan, "selector"));
        assertTrue(hasKey(plan, "lowerBound"));
        assertTrue(hasKey(plan, "upperBound"));
        assertEquals(true, getBooleanValue(plan, "includeLower"));
        assertEquals(true, getBooleanValue(plan, "includeUpper"));
    }

    @Test
    void shouldExplainAndQueryWithTwoIndexes() {
        // Insert the initial document to create a bucket
        insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));

        // Create two secondary indexes
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);

        ByteBuf indexBuf1 = Unpooled.buffer();
        cmd.indexCreate(TEST_BUCKET, "{\"age\": {\"bson_type\": \"int32\"}}").encode(indexBuf1);
        Object indexResult1 = runCommand(channel, indexBuf1);
        assertInstanceOf(SimpleStringRedisMessage.class, indexResult1);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) indexResult1).content());

        ByteBuf indexBuf2 = Unpooled.buffer();
        cmd.indexCreate(TEST_BUCKET, "{\"name\": {\"bson_type\": \"string\"}}").encode(indexBuf2);
        Object indexResult2 = runCommand(channel, indexBuf2);
        assertInstanceOf(SimpleStringRedisMessage.class, indexResult2);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) indexResult2).content());

        // Wait for indexes to be ready
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Index ageIndex = metadata.indexes().getIndex("age", IndexSelectionPolicy.ALL);
            Index nameIndex = metadata.indexes().getIndex("name", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(ageIndex.subspace());
            waitForIndexReadiness(nameIndex.subspace());
        }

        // Invalidate metadata cache to ensure EXPLAIN sees the new indexes
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, TEST_BUCKET);

        // Insert documents with both fields
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}")
        );
        insertDocumentsAndGetObjectIds(documents);

        switchProtocol(cmd, RESPVersion.RESP3);

        // Query with $and on two indexed fields
        // MIXED_SCAN strategy: picks the most selective index, converts others to residual predicates
        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, "{$and: [{age: {$eq: 25}}, {name: {$eq: \"Alice\"}}]}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;
        MapRedisMessage plan = getPlan(mapMessage);

        // Planner selects one index and chains residual predicate for the other
        assertEquals("IndexScan", getStringValue(plan, "nodeType"));
        assertEquals("INDEX_SCAN", getStringValue(plan, "scanType"));
        assertTrue(hasKey(plan, "next")); // Residual predicate filter
    }

    @Test
    void shouldExplainTransformWithResidualPredicate() {
        // Insert the initial document to create a bucket
        insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));

        // Create one secondary index
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);

        ByteBuf indexBuf = Unpooled.buffer();
        cmd.indexCreate(TEST_BUCKET, "{\"age\": {\"bson_type\": \"int32\"}}").encode(indexBuf);
        Object indexResult = runCommand(channel, indexBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, indexResult);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) indexResult).content());

        // Wait for the index to be ready
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Index ageIndex = metadata.indexes().getIndex("age", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(ageIndex.subspace());
        }

        // Invalidate the metadata cache to ensure EXPLAIN sees the new index
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, TEST_BUCKET);

        // Insert documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}")
        );
        insertDocumentsAndGetObjectIds(documents);

        switchProtocol(cmd, RESPVersion.RESP3);

        // Query with $and on indexed field (age) and non-indexed field (name)
        // This should produce an IndexScan with a TransformWithResidualPredicate for the non-indexed field
        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, "{$and: [{age: {$eq: 25}}, {name: {$eq: \"Alice\"}}]}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;
        MapRedisMessage plan = getPlan(mapMessage);

        // Root should be IndexScan on the indexed field
        assertEquals("IndexScan", getStringValue(plan, "nodeType"));
        assertEquals("age", getStringValue(plan, "selector"));

        // The next node should be TransformWithResidualPredicate for the non-indexed field
        MapRedisMessage nextNode = getMapValue(plan, "next");
        assertNotNull(nextNode);
        assertEquals("TransformWithResidualPredicate", getStringValue(nextNode, "nodeType"));
        assertEquals("FILTER", getStringValue(nextNode, "operation"));

        // Verify predicate is present
        MapRedisMessage predicate = getMapValue(nextNode, "predicate");
        assertNotNull(predicate);
        // The predicate contains the non-indexed field filter
        assertTrue(hasKey(predicate, "type"));
    }

    private String getStringValue(MapRedisMessage map, String key) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : map.children().entrySet()) {
            if (entry.getKey() instanceof FullBulkStringRedisMessage keyMsg && keyMsg.content().toString(StandardCharsets.UTF_8).equals(key)) {
                if (entry.getValue() instanceof FullBulkStringRedisMessage valueMsg) {
                    return valueMsg.content().toString(StandardCharsets.UTF_8);
                }
            }
        }
        return null;
    }

    private MapRedisMessage getMapValue(MapRedisMessage map, String key) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : map.children().entrySet()) {
            if (entry.getKey() instanceof FullBulkStringRedisMessage keyMsg && keyMsg.content().toString(StandardCharsets.UTF_8).equals(key)) {
                if (entry.getValue() instanceof MapRedisMessage mapValue) {
                    return mapValue;
                }
            }
        }
        return null;
    }

    private Long getIntegerValue(MapRedisMessage map, String key) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : map.children().entrySet()) {
            if (entry.getKey() instanceof FullBulkStringRedisMessage keyMsg && keyMsg.content().toString(StandardCharsets.UTF_8).equals(key)) {
                if (entry.getValue() instanceof IntegerRedisMessage intValue) {
                    return intValue.value();
                }
            }
        }
        return null;
    }

    private boolean hasKey(MapRedisMessage map, String key) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : map.children().entrySet()) {
            if (entry.getKey() instanceof FullBulkStringRedisMessage keyMsg && keyMsg.content().toString(StandardCharsets.UTF_8).equals(key)) {
                return true;
            }
        }
        return false;
    }

    private Boolean getBooleanValue(MapRedisMessage map, String key) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : map.children().entrySet()) {
            if (entry.getKey() instanceof FullBulkStringRedisMessage keyMsg && keyMsg.content().toString(StandardCharsets.UTF_8).equals(key)) {
                if (entry.getValue() instanceof BooleanRedisMessage boolValue) {
                    return boolValue.value();
                }
            }
        }
        return null;
    }

    private MapRedisMessage getPlan(MapRedisMessage response) {
        return getMapValue(response, "plan");
    }

    // --- Collation tests ---

    private void createBucketInline(String bucketName) {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.create(bucketName, BucketCreateArgs.Builder
                .shards(List.of(TEST_SHARD_ID))
                .ifNotExists()).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
    }

    private void insertDocumentIntoBucket(String bucketName) {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(bucketName, TEST_DOCUMENT).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, response);
    }

    @Test
    void shouldExplainWithCollationForcingFullScan() {
        // Behavior: EXPLAIN with collation mismatching index shows FullScan in the plan.
        String bucket = "explain-coll-en";
        createBucketInline(bucket);
        insertDocumentIntoBucket(bucket);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);

        ByteBuf indexBuf = Unpooled.buffer();
        cmd.indexCreate(bucket, "{\"name\": {\"bson_type\": \"string\", \"collation\": {\"locale\": \"en\", \"strength\": 1}}}").encode(indexBuf);
        Object indexResult = runCommand(channel, indexBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, indexResult);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, bucket);
            Index nameIndex = metadata.indexes().getIndex("name", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(nameIndex.subspace());
        }
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, bucket);

        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(bucket, "{\"name\": {\"$eq\": \"test\"}}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"tr\", \"strength\": 1}")).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage plan = getPlan((MapRedisMessage) msg);
        assertEquals("FullScan", getStringValue(plan, "nodeType"));
    }

    @Test
    void shouldExplainWithCollationMismatchShowingDetails() {
        // Behavior: EXPLAIN with collation mismatch shows collation_mismatch=true and rejected_index in the plan.
        String bucket = "explain-coll-mismatch-details";
        createBucketInline(bucket);
        insertDocumentIntoBucket(bucket);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);

        ByteBuf indexBuf = Unpooled.buffer();
        cmd.indexCreate(bucket, "{\"name\": {\"bson_type\": \"string\", \"collation\": {\"locale\": \"en\", \"strength\": 1}}}").encode(indexBuf);
        Object indexResult = runCommand(channel, indexBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, indexResult);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, bucket);
            Index nameIndex = metadata.indexes().getIndex("name", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(nameIndex.subspace());
        }
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, bucket);

        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(bucket, "{\"name\": {\"$eq\": \"test\"}}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"tr\", \"strength\": 1}")).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage plan = getPlan((MapRedisMessage) msg);
        assertEquals("FullScan", getStringValue(plan, "nodeType"));
        assertEquals(true, getBooleanValue(plan, "collation_mismatch"));
        assertNotNull(getStringValue(plan, "rejected_index"));
    }

    @Test
    void shouldExplainWithQueryCollationInOutput() {
        // Behavior: EXPLAIN with COLLATION shows query_collation in the top-level response.
        String bucket = "explain-query-coll-output";
        createBucketInline(bucket);
        insertDocumentIntoBucket(bucket);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(bucket, "{}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"tr\", \"strength\": 2}")).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;
        assertTrue(hasKey(mapMessage, "query_collation"));

        MapRedisMessage queryCollation = getMapValue(mapMessage, "query_collation");
        assertNotNull(queryCollation);
        assertEquals("tr", getStringValue(queryCollation, "locale"));
        assertEquals(Long.valueOf(2), getIntegerValue(queryCollation, "strength"));
    }

    @Test
    void shouldNotIncludeCollationFieldsWhenNoCollation() {
        // Behavior: EXPLAIN without COLLATION does not include query_collation or collation_mismatch fields.
        insertDocumentsAndGetObjectIds(List.of(TEST_DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, "{}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;
        assertFalse(hasKey(mapMessage, "query_collation"));

        MapRedisMessage plan = getPlan(mapMessage);
        assertFalse(hasKey(plan, "collation_mismatch"));
        assertFalse(hasKey(plan, "rejected_index"));
    }

    @Test
    void shouldExplainIndexScanWithIndexCollation() {
        // Behavior: EXPLAIN on an indexed field with collation shows index_collation in the plan.
        String bucket = "explain-index-coll";
        createBucketInline(bucket);
        insertDocumentIntoBucket(bucket);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);

        ByteBuf indexBuf = Unpooled.buffer();
        cmd.indexCreate(bucket, "{\"name\": {\"bson_type\": \"string\", \"collation\": {\"locale\": \"en\", \"strength\": 1}}}").encode(indexBuf);
        Object indexResult = runCommand(channel, indexBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, indexResult);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, bucket);
            Index nameIndex = metadata.indexes().getIndex("name", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(nameIndex.subspace());
        }
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, bucket);

        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(bucket, "{\"name\": {\"$eq\": \"test\"}}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"en\", \"strength\": 1}")).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage plan = getPlan((MapRedisMessage) msg);
        assertEquals("IndexScan", getStringValue(plan, "nodeType"));
        assertTrue(hasKey(plan, "index_collation"));

        MapRedisMessage indexCollation = getMapValue(plan, "index_collation");
        assertNotNull(indexCollation);
        assertEquals("en", getStringValue(indexCollation, "locale"));
        assertEquals(Long.valueOf(1), getIntegerValue(indexCollation, "strength"));
    }

    @Test
    void shouldExplainWithMatchingCollationUsingIndex() {
        // Behavior: EXPLAIN with collation matching index shows IndexScan in the plan.
        String bucket = "explain-coll-tr";
        createBucketInline(bucket);
        insertDocumentIntoBucket(bucket);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);

        ByteBuf indexBuf = Unpooled.buffer();
        cmd.indexCreate(bucket, "{\"name\": {\"bson_type\": \"string\", \"collation\": {\"locale\": \"tr\", \"strength\": 1}}}").encode(indexBuf);
        Object indexResult = runCommand(channel, indexBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, indexResult);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, bucket);
            Index nameIndex = metadata.indexes().getIndex("name", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(nameIndex.subspace());
        }
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, bucket);

        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(bucket, "{\"name\": {\"$eq\": \"test\"}}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"tr\", \"strength\": 1}")).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage plan = getPlan((MapRedisMessage) msg);
        assertEquals("IndexScan", getStringValue(plan, "nodeType"));
        assertEquals("name", getStringValue(plan, "selector"));
    }

    @Test
    void shouldExplainWithNoCollationShowingNormalBehavior() {
        // Behavior: EXPLAIN without COLLATION shows normal IndexScan for an indexed field.
        String bucket = "explain-no-coll";
        createBucketInline(bucket);
        insertDocumentIntoBucket(bucket);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);

        ByteBuf indexBuf = Unpooled.buffer();
        cmd.indexCreate(bucket, "{\"name\": {\"bson_type\": \"string\"}}").encode(indexBuf);
        Object indexResult = runCommand(channel, indexBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, indexResult);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, bucket);
            Index nameIndex = metadata.indexes().getIndex("name", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(nameIndex.subspace());
        }
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, bucket);

        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(bucket, "{\"name\": {\"$eq\": \"test\"}}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage plan = getPlan((MapRedisMessage) msg);
        assertEquals("IndexScan", getStringValue(plan, "nodeType"));
        assertEquals("name", getStringValue(plan, "selector"));
    }
}