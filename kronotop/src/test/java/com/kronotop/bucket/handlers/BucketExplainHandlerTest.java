/*
 * Copyright (c) 2023-2025 Burak Sezer
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
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BucketExplainHandlerTest extends BaseBucketHandlerTest {

    @Test
    void shouldReturnMapForRESP3Protocol() {
        insertDocuments(List.of(DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, "{}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;
        assertFalse(mapMessage.children().isEmpty());
    }

    @Test
    void shouldReturnArrayForRESP2Protocol() {
        insertDocuments(List.of(DOCUMENT));

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
        insertDocuments(List.of(DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, "{}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;

        // Verify planner_version is the first entry
        Map.Entry<RedisMessage, RedisMessage> firstEntry = mapMessage.children().entrySet().iterator().next();
        assertInstanceOf(SimpleStringRedisMessage.class, firstEntry.getKey());
        assertEquals("planner_version", ((SimpleStringRedisMessage) firstEntry.getKey()).content());

        assertInstanceOf(IntegerRedisMessage.class, firstEntry.getValue());
        assertEquals(1, ((IntegerRedisMessage) firstEntry.getValue()).value());
    }

    @Test
    void shouldExplainFullScan() {
        insertDocuments(List.of(DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, "{}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;

        assertEquals("FullScan", getStringValue(mapMessage, "nodeType"));
        assertEquals("FULL_SCAN", getStringValue(mapMessage, "scanType"));
        assertEquals("primary-index", getStringValue(mapMessage, "index"));
    }

    @Test
    void shouldExplainIndexScanOnPrimaryIndex() {
        Map<String, byte[]> docs = insertDocuments(List.of(DOCUMENT));
        String docId = docs.keySet().iterator().next();

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, String.format("{_id: {$eq: \"%s\"}}", docId)).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;

        assertEquals("IndexScan", getStringValue(mapMessage, "nodeType"));
        assertEquals("INDEX_SCAN", getStringValue(mapMessage, "scanType"));
        assertEquals("primary-index", getStringValue(mapMessage, "index"));
        assertEquals("_id", getStringValue(mapMessage, "selector"));
        assertEquals("EQ", getStringValue(mapMessage, "operator"));
    }

    @Test
    void shouldExplainFullScanWithResidualPredicate() {
        insertDocuments(List.of(DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        // Query on non-indexed field "one" should result in FullScan with residual predicate
        cmd.explain(TEST_BUCKET, "{one: {$eq: \"two\"}}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;

        // The root node should be FullScan
        assertEquals("FullScan", getStringValue(mapMessage, "nodeType"));
        assertEquals("FULL_SCAN", getStringValue(mapMessage, "scanType"));

        // Verify predicate is present
        MapRedisMessage predicate = getMapValue(mapMessage, "predicate");
        assertNotNull(predicate);
        assertEquals("PREDICATE", getStringValue(predicate, "type"));
        assertEquals("one", getStringValue(predicate, "selector"));
        assertEquals("EQ", getStringValue(predicate, "operator"));
    }

    @Test
    void shouldIncludeNodeTypeAndId() {
        insertDocuments(List.of(DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, "{}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;

        // Verify nodeType is present
        String nodeType = getStringValue(mapMessage, "nodeType");
        assertNotNull(nodeType);
        assertFalse(nodeType.isEmpty());

        // Verify id is present
        Long id = getIntegerValue(mapMessage, "id");
        assertNotNull(id);
    }

    @Test
    void shouldIncludeScanTypeForScanNodes() {
        Map<String, byte[]> docs = insertDocuments(List.of(DOCUMENT));
        String docId = docs.keySet().iterator().next();

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Test INDEX_SCAN
        ByteBuf buf1 = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, String.format("{_id: {$eq: \"%s\"}}", docId)).encode(buf1);
        Object msg1 = runCommand(channel, buf1);
        MapRedisMessage indexScanPlan = (MapRedisMessage) msg1;
        assertNotNull(indexScanPlan);
        assertEquals("INDEX_SCAN", getStringValue(indexScanPlan, "scanType"));

        // Test FULL_SCAN
        ByteBuf buf2 = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, "{}").encode(buf2);
        Object msg2 = runCommand(channel, buf2);
        MapRedisMessage fullScanPlan = (MapRedisMessage) msg2;
        assertNotNull(fullScanPlan);
        assertEquals("FULL_SCAN", getStringValue(fullScanPlan, "scanType"));

        // Test RANGE_SCAN (using $gt and $lt on _id)
        ByteBuf buf3 = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, String.format("{_id: {$gte: \"%s\", $lte: \"%s\"}}", docId, docId)).encode(buf3);
        Object msg3 = runCommand(channel, buf3);
        MapRedisMessage rangeScanPlan = (MapRedisMessage) msg3;
        assertNotNull(rangeScanPlan);
        assertEquals("RANGE_SCAN", getStringValue(rangeScanPlan, "scanType"));
    }

    @Test
    void shouldExplainEQOperator() {
        Map<String, byte[]> docs = insertDocuments(List.of(DOCUMENT));
        String docId = docs.keySet().iterator().next();

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, String.format("{_id: {$eq: \"%s\"}}", docId)).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;

        assertEquals("IndexScan", getStringValue(mapMessage, "nodeType"));
        assertEquals("EQ", getStringValue(mapMessage, "operator"));
        assertEquals("_id", getStringValue(mapMessage, "selector"));
        // operand is present (value is a VersionstampVal for binary _id)
        assertTrue(hasKey(mapMessage, "operand"));
    }

    @Test
    void shouldExplainLTOperator() {
        Map<String, byte[]> docs = insertDocuments(List.of(DOCUMENT));
        String docId = docs.keySet().iterator().next();

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, String.format("{_id: {$lt: \"%s\"}}", docId)).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;

        assertEquals("IndexScan", getStringValue(mapMessage, "nodeType"));
        assertEquals("LT", getStringValue(mapMessage, "operator"));
        assertEquals("_id", getStringValue(mapMessage, "selector"));
        assertTrue(hasKey(mapMessage, "operand"));
    }

    @Test
    void shouldExplainRangeScan() {
        Map<String, byte[]> docs = insertDocuments(List.of(DOCUMENT));
        String docId = docs.keySet().iterator().next();

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, String.format("{_id: {$gte: \"%s\", $lte: \"%s\"}}", docId, docId)).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;

        assertEquals("RangeScan", getStringValue(mapMessage, "nodeType"));
        assertEquals("RANGE_SCAN", getStringValue(mapMessage, "scanType"));
        assertEquals("_id", getStringValue(mapMessage, "selector"));
        assertTrue(hasKey(mapMessage, "lowerBound"));
        assertTrue(hasKey(mapMessage, "upperBound"));
        assertEquals(true, getBooleanValue(mapMessage, "includeLower"));
        assertEquals(true, getBooleanValue(mapMessage, "includeUpper"));
    }

    @Test
    void shouldExplainAndQueryWithTwoIndexes() {
        // Insert initial document to create bucket
        insertDocuments(List.of(DOCUMENT));

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
            BucketMetadata metadata = BucketMetadataUtil.openUncached(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Index ageIndex = metadata.indexes().getIndex("age", IndexSelectionPolicy.ALL);
            Index nameIndex = metadata.indexes().getIndex("name", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(ageIndex.subspace());
            waitForIndexReadiness(nameIndex.subspace());
        }

        // Insert documents with both fields
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}")
        );
        insertDocuments(documents);

        switchProtocol(cmd, RESPVersion.RESP3);

        // Query with $and on two indexed fields
        // MIXED_SCAN strategy: picks most selective index, converts others to residual predicates
        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, "{$and: [{age: {$eq: 25}}, {name: {$eq: \"Alice\"}}]}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;

        // Planner selects one index and chains residual predicate for the other
        assertEquals("IndexScan", getStringValue(mapMessage, "nodeType"));
        assertEquals("INDEX_SCAN", getStringValue(mapMessage, "scanType"));
        assertTrue(hasKey(mapMessage, "next")); // Residual predicate filter
    }

    @Test
    void shouldExplainTransformWithResidualPredicate() {
        // Insert initial document to create bucket
        insertDocuments(List.of(DOCUMENT));

        // Create one secondary index
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);

        ByteBuf indexBuf = Unpooled.buffer();
        cmd.indexCreate(TEST_BUCKET, "{\"age\": {\"bson_type\": \"int32\"}}").encode(indexBuf);
        Object indexResult = runCommand(channel, indexBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, indexResult);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) indexResult).content());

        // Wait for index to be ready
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.openUncached(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Index ageIndex = metadata.indexes().getIndex("age", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(ageIndex.subspace());
        }

        // Insert documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}")
        );
        insertDocuments(documents);

        switchProtocol(cmd, RESPVersion.RESP3);

        // Query with $and on indexed field (age) and non-indexed field (name)
        // This should produce an IndexScan with a TransformWithResidualPredicate for the non-indexed field
        ByteBuf buf = Unpooled.buffer();
        cmd.explain(TEST_BUCKET, "{$and: [{age: {$eq: 25}}, {name: {$eq: \"Alice\"}}]}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage mapMessage = (MapRedisMessage) msg;

        // Root should be IndexScan on the indexed field
        assertEquals("IndexScan", getStringValue(mapMessage, "nodeType"));
        assertEquals("age", getStringValue(mapMessage, "selector"));

        // Next node should be TransformWithResidualPredicate for the non-indexed field
        MapRedisMessage nextNode = getMapValue(mapMessage, "next");
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
            if (entry.getKey() instanceof SimpleStringRedisMessage keyMsg && keyMsg.content().equals(key)) {
                if (entry.getValue() instanceof SimpleStringRedisMessage valueMsg) {
                    return valueMsg.content();
                }
            }
        }
        return null;
    }

    private MapRedisMessage getMapValue(MapRedisMessage map, String key) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : map.children().entrySet()) {
            if (entry.getKey() instanceof SimpleStringRedisMessage keyMsg && keyMsg.content().equals(key)) {
                if (entry.getValue() instanceof MapRedisMessage mapValue) {
                    return mapValue;
                }
            }
        }
        return null;
    }

    private Long getIntegerValue(MapRedisMessage map, String key) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : map.children().entrySet()) {
            if (entry.getKey() instanceof SimpleStringRedisMessage keyMsg && keyMsg.content().equals(key)) {
                if (entry.getValue() instanceof IntegerRedisMessage intValue) {
                    return intValue.value();
                }
            }
        }
        return null;
    }

    private boolean hasKey(MapRedisMessage map, String key) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : map.children().entrySet()) {
            if (entry.getKey() instanceof SimpleStringRedisMessage keyMsg && keyMsg.content().equals(key)) {
                return true;
            }
        }
        return false;
    }

    private Boolean getBooleanValue(MapRedisMessage map, String key) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : map.children().entrySet()) {
            if (entry.getKey() instanceof SimpleStringRedisMessage keyMsg && keyMsg.content().equals(key)) {
                if (entry.getValue() instanceof BooleanRedisMessage boolValue) {
                    return boolValue.value();
                }
            }
        }
        return null;
    }
}