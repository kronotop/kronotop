/*
 * Copyright (c) 2023-2025 Burak Sezer
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

import com.kronotop.bucket.BSONUtil;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.commandbuilder.kronotop.BucketQueryArgs;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.bson.Document;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class BucketQueryHandlerTest extends BaseBucketHandlerTest {

    @Test
    @Disabled
    void shouldDoPhysicalFullScanWithoutOperator() {
        Map<String, byte[]> expectedDocument = insertDocuments(List.of(DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(BUCKET_NAME, "{}", BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertEquals(expectedDocument.size(), actualMessage.children().size());
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            // Check key
            SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
            String id = keyMessage.content();
            assertNotNull(id);
            assertNotNull(expectedDocument.get(id));

            // Check value
            FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) entry.getValue();
            assertArrayEquals(expectedDocument.get(id), ByteBufUtil.getBytes(value.content()));
        }
    }

    @Test
    @Disabled
    void shouldDoPhysicalFullScanWithoutOperator_RESP2() {
        Map<String, byte[]> expectedDocument = insertDocuments(List.of(DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP2);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(BUCKET_NAME, "{}", BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        int index = 0;
        String latestId = "";
        for (RedisMessage entry : actualMessage.children()) {
            if (index % 2 == 0) {
                // Check key
                SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry;
                String id = keyMessage.content();
                assertNotNull(id);
                assertNotNull(expectedDocument.get(id));
                latestId = id;
            } else {
                // Check value
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) entry;
                assertArrayEquals(expectedDocument.get(latestId), ByteBufUtil.getBytes(value.content()));
            }
            index++;
        }
    }

    @Test
    @Disabled
    void shouldDoPhysicalIndexScanWithSingleOperator_DefaultIDIndex_EQ() {
        Map<String, byte[]> expectedDocument = insertDocuments(makeDummyDocument(3));

        // Find the document in the middle
        String[] keys = expectedDocument.keySet().toArray(new String[0]);
        String expectedKey = keys[1];

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(BUCKET_NAME, String.format("{_id: {$eq: \"%s\"}}", expectedKey), BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertEquals(1, actualMessage.children().size());
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            assertInstanceOf(SimpleStringRedisMessage.class, entry.getKey());
            assertInstanceOf(FullBulkStringRedisMessage.class, entry.getValue());

            SimpleStringRedisMessage resultKey = (SimpleStringRedisMessage) entry.getKey();
            assertEquals(expectedKey, resultKey.content());

            FullBulkStringRedisMessage resultMessageValue = (FullBulkStringRedisMessage) entry.getValue();
            byte[] resultValue = ByteBufUtil.getBytes(resultMessageValue.content());
            assertArrayEquals(expectedDocument.get(expectedKey), resultValue);
        }
    }

    @Test
    @Disabled
    void shouldDoPhysicalIndexScanWithSingleOperator_DefaultIDIndex_GTE() {
        Map<String, byte[]> expectedDocument = insertDocuments(makeDummyDocument(10));

        // Find the document in the middle
        String[] keys = expectedDocument.keySet().toArray(new String[0]);
        String key = keys[4];
        Set<String> excludedKeys = new HashSet<>(Arrays.asList(keys).subList(0, 4));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(BUCKET_NAME, String.format("{_id: {$gte: \"%s\"}}", key), BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertEquals(6, actualMessage.children().size());
        int index = 4;
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            assertInstanceOf(SimpleStringRedisMessage.class, entry.getKey());
            assertInstanceOf(FullBulkStringRedisMessage.class, entry.getValue());

            SimpleStringRedisMessage resultKey = (SimpleStringRedisMessage) entry.getKey();
            assertEquals(keys[index], resultKey.content());
            assertFalse(excludedKeys.contains(resultKey.content()));

            FullBulkStringRedisMessage resultMessageValue = (FullBulkStringRedisMessage) entry.getValue();
            byte[] resultValue = ByteBufUtil.getBytes(resultMessageValue.content());
            assertArrayEquals(expectedDocument.get(resultKey.content()), resultValue);
            index++;
        }
    }

    @Test
    @Disabled
    void shouldDoPhysicalIndexScanWithSingleOperator_DefaultIDIndex_GT() {
        Map<String, byte[]> expectedDocument = insertDocuments(makeDummyDocument(10));

        String[] keys = expectedDocument.keySet().toArray(new String[0]);
        String key = keys[4];
        Set<String> excludedKeys = new HashSet<>(Arrays.asList(keys).subList(0, 5));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(BUCKET_NAME, String.format("{_id: {$gt: \"%s\"}}", key), BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertEquals(5, actualMessage.children().size());
        int index = 5;
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            assertInstanceOf(SimpleStringRedisMessage.class, entry.getKey());
            assertInstanceOf(FullBulkStringRedisMessage.class, entry.getValue());

            SimpleStringRedisMessage resultKey = (SimpleStringRedisMessage) entry.getKey();
            assertEquals(keys[index], resultKey.content());
            assertFalse(excludedKeys.contains(resultKey.content()));

            FullBulkStringRedisMessage resultMessageValue = (FullBulkStringRedisMessage) entry.getValue();
            byte[] resultValue = ByteBufUtil.getBytes(resultMessageValue.content());
            assertArrayEquals(expectedDocument.get(resultKey.content()), resultValue);
            index++;
        }
    }

    @Test
    @Disabled
    void shouldDoPhysicalIndexScanWithSingleOperator_DefaultIDIndex_LT() {
        Map<String, byte[]> expectedDocument = insertDocuments(makeDummyDocument(10));

        // Find the document in the middle
        String[] keys = expectedDocument.keySet().toArray(new String[0]);
        String key = keys[4];
        Set<String> excludedKeys = new HashSet<>(Arrays.asList(keys).subList(4, 9));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        // Query should retrieve the first two documents we inserted
        cmd.query(BUCKET_NAME, String.format("{_id: {$lt: \"%s\"}}", key), BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;

        assertEquals(4, actualMessage.children().size());

        int index = 0;
        // The first two documents, the last one is excluded by the query.
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            assertInstanceOf(SimpleStringRedisMessage.class, entry.getKey());
            assertInstanceOf(FullBulkStringRedisMessage.class, entry.getValue());

            SimpleStringRedisMessage resultKey = (SimpleStringRedisMessage) entry.getKey();
            assertEquals(keys[index], resultKey.content());
            assertFalse(excludedKeys.contains(resultKey.content()));

            FullBulkStringRedisMessage resultMessageValue = (FullBulkStringRedisMessage) entry.getValue();
            byte[] resultValue = ByteBufUtil.getBytes(resultMessageValue.content());
            assertArrayEquals(expectedDocument.get(resultKey.content()), resultValue);
            index++;
        }
    }

    @Test
    @Disabled
    void shouldDoPhysicalIndexScanWithSingleOperator_DefaultIDIndex_LTE() {
        Map<String, byte[]> expectedDocument = insertDocuments(makeDummyDocument(10));

        // Find the document in the middle
        String[] keys = expectedDocument.keySet().toArray(new String[0]);
        String key = keys[4];
        Set<String> excludedKeys = new HashSet<>(Arrays.asList(keys).subList(5, 9));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        // Query should retrieve the first two documents we inserted
        cmd.query(BUCKET_NAME, String.format("{_id: {$lte: \"%s\"}}", key), BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;

        assertEquals(5, actualMessage.children().size());
        int index = 0;
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            assertInstanceOf(SimpleStringRedisMessage.class, entry.getKey());
            assertInstanceOf(FullBulkStringRedisMessage.class, entry.getValue());

            SimpleStringRedisMessage resultKey = (SimpleStringRedisMessage) entry.getKey();
            assertEquals(keys[index], resultKey.content());
            assertFalse(excludedKeys.contains(resultKey.content()));

            FullBulkStringRedisMessage resultMessageValue = (FullBulkStringRedisMessage) entry.getValue();
            byte[] resultValue = ByteBufUtil.getBytes(resultMessageValue.content());
            assertArrayEquals(expectedDocument.get(resultKey.content()), resultValue);
            index++;
        }
    }

    // ========================================================================
    // FULL SCAN INTEGRATION TESTS
    // Tests for non-indexed field queries that require full bucket scans
    // ========================================================================

    @Test
    @Disabled
    void shouldDoFullScanWithStringFieldFilter_EQ() {
        // Insert documents with varying string field values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 35}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 40}") // Duplicate name
        );
        Map<String, byte[]> expectedDocuments = insertDocuments(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(BUCKET_NAME, "{\"name\": {\"$eq\": \"Alice\"}}", BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertEquals(2, actualMessage.children().size(), "Should find 2 documents with name 'Alice'");

        // Verify that all returned documents have name = "Alice"
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
            String docId = keyMessage.content();
            byte[] expectedDoc = expectedDocuments.get(docId);
            assertNotNull(expectedDoc, "Document should exist in original data");

            // The document should contain "Alice" (we inserted 2 such documents)
            Document doc = BSONUtil.toDocument(expectedDoc);
            assertEquals("Alice", doc.getString("name"), "Document should have name = 'Alice'");
        }
    }

    @Test
    @Disabled
    void shouldDoFullScanWithNumericFieldFilter_GTE() {
        // Insert documents with varying numeric field values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25, \"score\": 85.5}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30, \"score\": 92.0}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 35, \"score\": 78.5}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 28, \"score\": 95.5}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 32, \"score\": 88.0}")
        );
        Map<String, byte[]> expectedDocuments = insertDocuments(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(BUCKET_NAME, "{\"age\": {\"$gte\": 30}}", BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertEquals(3, actualMessage.children().size(), "Should find 3 documents with age >= 30");

        // Verify all returned documents have age >= 30
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
            String docId = keyMessage.content();
            byte[] expectedDoc = expectedDocuments.get(docId);
            assertNotNull(expectedDoc, "Document should exist in original data");

            Document doc = BSONUtil.toDocument(expectedDoc);
            int age = doc.getInteger("age");
            assertTrue(age >= 30, "Document should have age >= 30, but was " + age);
        }
    }

    @Test
    @Disabled
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

        Map<String, byte[]> expectedDocuments = insertDocuments(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(BUCKET_NAME, "{\"category\": \"SPECIAL\"}", BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertEquals(1, actualMessage.children().size(), "Should find exactly 1 document with category 'SPECIAL'");

        // Verify the returned document is the special one
        Map.Entry<RedisMessage, RedisMessage> entry = actualMessage.children().entrySet().iterator().next();
        SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
        String docId = keyMessage.content();
        byte[] actualDoc = expectedDocuments.get(docId);
        assertNotNull(actualDoc, "Document should exist");

        Document doc = BSONUtil.toDocument(actualDoc);
        assertEquals("SPECIAL", doc.getString("category"), "Should be the SPECIAL category document");
        assertEquals("final-content", doc.getString("data"), "Should be the final document");
    }

    @Test
    @Disabled
    void shouldDoFullScanWithBooleanFieldFilter() {
        // Insert documents with boolean fields
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"active\": true, \"premium\": false}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"active\": false, \"premium\": true}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"active\": true, \"premium\": true}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"active\": false, \"premium\": false}")
        );
        Map<String, byte[]> expectedDocuments = insertDocuments(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(BUCKET_NAME, "{\"active\": true}", BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertEquals(2, actualMessage.children().size(), "Should find 2 documents with active = true");

        // Verify returned documents have active = true (Alice and Charlie)
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
            String docId = keyMessage.content();
            byte[] expectedDoc = expectedDocuments.get(docId);

            Document doc = BSONUtil.toDocument(expectedDoc);
            String name = doc.getString("name");
            assertTrue("Alice".equals(name) || "Charlie".equals(name),
                    "Should be Alice or Charlie (active=true), but was " + name);
            assertTrue(doc.getBoolean("active"), "Document should have active=true");
        }
    }

    @Test
    @Disabled
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
        Map<String, byte[]> expectedDocuments = insertDocuments(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        // Range query: 70 <= score <= 90
        cmd.query(BUCKET_NAME, "{\"$and\": [{\"score\": {\"$gte\": 70}}, {\"score\": {\"$lte\": 90}}]}",
                BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertEquals(3, actualMessage.children().size(), "Should find 3 documents with score 70-90");

        // Verify returned documents are Alice (75), Bob (85), Eve (90)
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
            String docId = keyMessage.content();
            byte[] expectedDoc = expectedDocuments.get(docId);

            Document doc = BSONUtil.toDocument(expectedDoc);
            String name = doc.getString("name");
            int score = doc.getInteger("score");
            assertTrue(("Alice".equals(name) && score == 75) ||
                            ("Bob".equals(name) && score == 85) ||
                            ("Eve".equals(name) && score == 90),
                    "Should be Alice (75), Bob (85), or Eve (90), but was " + name + " (" + score + ")");
            assertTrue(score >= 70 && score <= 90, "Score should be in range 70-90");
        }
    }

    @Test
    @Disabled
    void shouldDoFullScanWithNoMatchingDocuments() {
        // Test the case where filter matches no documents - should return empty without infinite loops
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"category\": \"TYPE_A\", \"value\": 100}"),
                BSONUtil.jsonToDocumentThenBytes("{\"category\": \"TYPE_B\", \"value\": 200}"),
                BSONUtil.jsonToDocumentThenBytes("{\"category\": \"TYPE_C\", \"value\": 300}")
        );
        insertDocuments(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(BUCKET_NAME, "{\"category\": \"NONEXISTENT_TYPE\"}", BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertEquals(0, actualMessage.children().size(), "Should find no documents for nonexistent category");
    }

    @Test
    @Disabled
    void shouldDoFullScanWithComplexDocument_NestedFields() {
        // Test full scan with more complex document structures
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"user\": {\"name\": \"Alice\", \"age\": 25}, \"status\": \"active\", \"tags\": [\"vip\", \"premium\"]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"user\": {\"name\": \"Bob\", \"age\": 30}, \"status\": \"inactive\", \"tags\": [\"standard\"]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"user\": {\"name\": \"Charlie\", \"age\": 35}, \"status\": \"active\", \"tags\": [\"vip\"]}")
        );
        Map<String, byte[]> expectedDocuments = insertDocuments(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(BUCKET_NAME, "{\"status\": \"active\"}", BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertEquals(2, actualMessage.children().size(), "Should find 2 documents with status 'active'");

        // Verify returned documents have status = "active" (Alice and Charlie)
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
            String docId = keyMessage.content();
            byte[] expectedDoc = expectedDocuments.get(docId);

            Document doc = BSONUtil.toDocument(expectedDoc);
            Document user = (Document) doc.get("user");
            String name = user.getString("name");
            String status = doc.getString("status");
            assertTrue("Alice".equals(name) || "Charlie".equals(name),
                    "Should be Alice or Charlie (status=active), but was " + name);
            assertEquals("active", status, "Should contain status active");
        }
    }

    @Test
    @Disabled
    void shouldDoFullScanWithMixedDataTypes() {
        // Test full scan with various BSON data types
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"string\", \"value\": \"text\", \"priority\": 1}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"number\", \"value\": 42, \"priority\": 2}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"boolean\", \"value\": true, \"priority\": 1}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"string\", \"value\": \"another\", \"priority\": 3}")
        );
        Map<String, byte[]> expectedDocuments = insertDocuments(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(BUCKET_NAME, "{\"priority\": 1}", BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertEquals(2, actualMessage.children().size(), "Should find 2 documents with priority = 1");

        // Verify returned documents have priority = 1
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
            String docId = keyMessage.content();
            byte[] expectedDoc = expectedDocuments.get(docId);

            Document doc = BSONUtil.toDocument(expectedDoc);
            int priority = doc.getInteger("priority");
            assertEquals(1, priority, "Document should have priority = 1");
        }
    }

    @Test
    @Disabled
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

        insertDocuments(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(BUCKET_NAME, "{\"tier\": \"GOLD\"}", BucketQueryArgs.Builder.shard(SHARD_ID)).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertEquals(matchingDocs, actualMessage.children().size(), "Should find all GOLD tier documents");

        // Verify all returned documents are GOLD tier
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
            FullBulkStringRedisMessage valueMessage = (FullBulkStringRedisMessage) entry.getValue();

            byte[] docBytes = ByteBufUtil.getBytes(valueMessage.content());
            Document doc = BSONUtil.toDocument(docBytes);
            assertEquals("GOLD", doc.getString("tier"), "All returned documents should be GOLD tier");
        }
    }
}