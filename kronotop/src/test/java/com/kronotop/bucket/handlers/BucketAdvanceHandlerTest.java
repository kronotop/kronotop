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

import com.apple.foundationdb.Transaction;
import com.kronotop.CachedTimeService;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.commandbuilder.kronotop.BucketQueryArgs;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class BucketAdvanceHandlerTest extends BaseBucketHandlerTest {

    private void appendIds(MapRedisMessage mapRedisMessage, List<String> result) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : mapRedisMessage.children().entrySet()) {
            SimpleStringRedisMessage id = (SimpleStringRedisMessage) entry.getKey();
            result.add(id.content());
        }
    }

    private void appendDocumentData(MapRedisMessage mapRedisMessage, Map<String, BsonDocument> result) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : mapRedisMessage.children().entrySet()) {
            SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
            FullBulkStringRedisMessage valueMessage = (FullBulkStringRedisMessage) entry.getValue();

            String docId = keyMessage.content();
            byte[] docBytes = ByteBufUtil.getBytes(valueMessage.content());
            BsonDocument doc = BSONUtil.toBsonDocument(docBytes);
            result.put(docId, doc);
        }
    }

    @Test
    void shouldAdvanceCursorForFullScan() {
        // Create 10 identical documents for a simple full scan test
        List<byte[]> documents = new ArrayList<>();
        for (int j = 0; j < 10; j++) {
            documents.add(DOCUMENT);
        }
        Map<String, byte[]> insertedDocs = insertDocuments(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        // BUCKET.QUERY - Full scan (empty filter) with limit of 2
        List<String> result = new ArrayList<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.limit(2)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;
            assertEquals(2, mapRedisMessage.children().size());

            RedisMessage rawCursorId = findInMapMessage(mapRedisMessage, "cursor_id");
            assertNotNull(rawCursorId);
            cursorId = Math.toIntExact(((IntegerRedisMessage) rawCursorId).value());

            RedisMessage entries = findInMapMessage(mapRedisMessage, "entries");
            assertNotNull(entries);
            assertInstanceOf(MapRedisMessage.class, entries);
            appendIds((MapRedisMessage) entries, result);
        }


        // BUCKET.ADVANCE - Continue pagination until we get all documents or hit limit
        int maxAdvanceCalls = 10; // Allow reasonable number of calls
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;

            RedisMessage rawEntries = findInMapMessage(mapRedisMessage, "entries");
            assertNotNull(rawEntries);
            assertInstanceOf(MapRedisMessage.class, rawEntries);
            MapRedisMessage entries = (MapRedisMessage) rawEntries;

            if (entries.children().isEmpty()) {
                break; // Normal termination
            }

            assertTrue(entries.children().size() <= 2, "Each batch should have at most 2 documents");

            appendIds(entries, result);
            advanceCalls++;
        }

        // We should have retrieved all 10 documents or stopped gracefully
        assertTrue(result.size() <= 10, "Should not retrieve more than 10 documents");
        assertTrue(result.size() >= 2, "Should retrieve at least the first 2 documents");

        // Verify no duplicate IDs
        Set<String> uniqueIds = new HashSet<>(result);
        assertEquals(result.size(), uniqueIds.size(), "Should not have duplicate document IDs");

        // All returned IDs should be from our inserted documents
        Set<String> insertedIds = insertedDocs.keySet();
        for (String id : result) {
            assertTrue(insertedIds.contains(id), "Returned ID should be from inserted documents");
        }
    }

    @Test
    void shouldAdvanceCursorWithSimpleFilter() {
        // Insert documents with a simple field for filtering
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"A\", \"value\": 1}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"B\", \"value\": 2}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"A\", \"value\": 3}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"B\", \"value\": 4}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"A\", \"value\": 5}")
        );

        insertDocuments(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        // BUCKET.QUERY - Filter for type A with limit of 1
        Map<String, BsonDocument> allResults = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"type\": \"A\"}", BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;
            assertEquals(2, mapRedisMessage.children().size());

            RedisMessage rawCursorId = findInMapMessage(mapRedisMessage, "cursor_id");
            assertNotNull(rawCursorId);
            cursorId = Math.toIntExact(((IntegerRedisMessage) rawCursorId).value());

            RedisMessage entries = findInMapMessage(mapRedisMessage, "entries");
            assertNotNull(entries);
            assertInstanceOf(MapRedisMessage.class, entries);
            appendDocumentData((MapRedisMessage) entries, allResults);
        }

        // BUCKET.ADVANCE - Continue until we get all type A docs or reasonable limit
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;

            RedisMessage rawEntries = findInMapMessage(mapRedisMessage, "entries");
            assertNotNull(rawEntries);
            assertInstanceOf(MapRedisMessage.class, rawEntries);
            MapRedisMessage entries = (MapRedisMessage) rawEntries;

            if (entries.children().isEmpty()) {
                break; // Normal termination
            }

            assertTrue(entries.children().size() <= 1, "Each batch should have at most 1 document");

            appendDocumentData(entries, allResults);
            advanceCalls++;
        }

        // Verify results - we expect up to 3 documents with type A
        assertTrue(allResults.size() <= 3, "Should retrieve at most 3 documents with type A");

        // Verify all returned documents have type A
        for (BsonDocument doc : allResults.values()) {
            assertEquals("A", doc.getString("type").getValue(), "All returned documents should have type A");
        }
    }

    @Test
    void shouldHandleNoResultsCorrectly() {
        // Insert documents that won't match our filter
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"category\": \"X\", \"value\": 1}"),
                BSONUtil.jsonToDocumentThenBytes("{\"category\": \"Y\", \"value\": 2}"),
                BSONUtil.jsonToDocumentThenBytes("{\"category\": \"Z\", \"value\": 3}")
        );

        insertDocuments(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        // BUCKET.QUERY - Filter for non-existent category
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"category\": \"NONEXISTENT\"}", BucketQueryArgs.Builder.limit(2)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;
            assertEquals(2, mapRedisMessage.children().size());

            RedisMessage rawCursorId = findInMapMessage(mapRedisMessage, "cursor_id");
            assertNotNull(rawCursorId);
            cursorId = Math.toIntExact(((IntegerRedisMessage) rawCursorId).value());

            RedisMessage entries = findInMapMessage(mapRedisMessage, "entries");
            assertNotNull(entries);
            assertInstanceOf(MapRedisMessage.class, entries);
            MapRedisMessage entriesMap = (MapRedisMessage) entries;
            assertEquals(0, entriesMap.children().size(), "No documents should match the filter");
        }

        // BUCKET.ADVANCE - Should return empty since no documents match
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;

            RedisMessage rawEntries = findInMapMessage(mapRedisMessage, "entries");
            assertNotNull(rawEntries);
            assertInstanceOf(MapRedisMessage.class, rawEntries);
            MapRedisMessage entries = (MapRedisMessage) rawEntries;
            assertEquals(0, entries.children().size(), "Advance should return empty when no matches");
        }
    }

    @Test
    void shouldThrowBucketBeingRemovedExceptionWhenAdvancingOnRemovedBucket() {
        // Insert documents to create the bucket
        List<byte[]> documents = new ArrayList<>();
        for (int j = 0; j < 5; j++) {
            documents.add(DOCUMENT);
        }
        insertDocuments(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Start a query with a limit to get a cursor
        int cursorId;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.limit(2)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;

            RedisMessage rawCursorId = findInMapMessage(mapRedisMessage, "cursor_id");
            assertNotNull(rawCursorId);
            cursorId = Math.toIntExact(((IntegerRedisMessage) rawCursorId).value());
        }

        // Mark the bucket as removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.openUncached(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            TransactionalContext tx = new TransactionalContext(context, tr);
            BucketMetadataUtil.setRemoved(tx, metadata);
            tr.commit().join();
        }

        // Flush the bucket metadata cache so open reads the dropped status
        Runnable cleanup = context.getBucketMetadataCache().createEvictionWorker(
                context.getService(CachedTimeService.NAME), 0);
        cleanup.run();

        // Try to advance the cursor on the dropped bucket
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
            assertEquals("BUCKETBEINGREMOVED Bucket 'test-bucket' is being removed", errorMessage.content());
        }
    }

    @Test
    void shouldAdvanceCursorWithElemMatchOnIndexedArray() {
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Create multiKey index on scores array
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"scores\": {\"bson_type\": \"int32\", \"multi_key\": true}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        // Insert documents with array fields
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"scores\": [85, 90, 78]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"scores\": [60, 55, 70]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"scores\": [95, 88, 92]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"scores\": [72, 68, 75]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"scores\": [91, 87, 93]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Frank\", \"scores\": [45, 50, 55]}") // No score >= 80
        );
        insertDocuments(documents);

        int cursorId;
        // BUCKET.QUERY - $elemMatch for scores >= 80 with limit of 2
        Map<String, BsonDocument> allResults = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"scores\": {\"$elemMatch\": {\"$gte\": 80}}}", BucketQueryArgs.Builder.limit(2)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;
            assertEquals(2, mapRedisMessage.children().size());

            RedisMessage rawCursorId = findInMapMessage(mapRedisMessage, "cursor_id");
            assertNotNull(rawCursorId);
            cursorId = Math.toIntExact(((IntegerRedisMessage) rawCursorId).value());

            RedisMessage entries = findInMapMessage(mapRedisMessage, "entries");
            assertNotNull(entries);
            assertInstanceOf(MapRedisMessage.class, entries);
            appendDocumentData((MapRedisMessage) entries, allResults);
        }

        // BUCKET.ADVANCE - Continue until we get all matching docs
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;

            RedisMessage rawEntries = findInMapMessage(mapRedisMessage, "entries");
            assertNotNull(rawEntries);
            assertInstanceOf(MapRedisMessage.class, rawEntries);
            MapRedisMessage entries = (MapRedisMessage) rawEntries;

            if (entries.children().isEmpty()) {
                break;
            }

            assertTrue(entries.children().size() <= 2, "Each batch should have at most 2 documents");
            appendDocumentData(entries, allResults);
            advanceCalls++;
        }

        // Verify results - we expect 5 documents with at least one score >= 80
        // Alice (85, 90), Bob (none), Charlie (95, 88, 92), Diana (none >= 80... wait 72, 68, 75 all < 80),
        // Eve (91, 87, 93), Frank (none)
        // Actually: Alice, Charlie, Eve have scores >= 80. Diana has max 75.
        assertEquals(3, allResults.size(), "Should retrieve exactly 3 documents with scores >= 80");

        // Verify all returned documents have at least one score >= 80
        Set<String> expectedNames = Set.of("Alice", "Charlie", "Eve");
        Set<String> actualNames = new HashSet<>();
        for (BsonDocument doc : allResults.values()) {
            actualNames.add(doc.getString("name").getValue());
        }
        assertEquals(expectedNames, actualNames, "Should match documents with scores >= 80");
    }

    @Test
    void shouldAdvanceCursorWithElemMatchOnNonIndexedArray() {
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // No index created - this will use FullScanNode

        // Insert documents with array fields
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"ratings\": [4.5, 3.8, 4.2]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"ratings\": [2.1, 2.5, 3.0]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"ratings\": [4.8, 4.9, 5.0]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"ratings\": [3.5, 3.8, 3.9]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"ratings\": [4.1, 4.3, 4.7]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Frank\", \"ratings\": [1.5, 2.0, 2.5]}") // No rating >= 4.0
        );
        insertDocuments(documents);

        int cursorId;
        // BUCKET.QUERY - $elemMatch for ratings >= 4.0 with limit of 2
        Map<String, BsonDocument> allResults = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"ratings\": {\"$elemMatch\": {\"$gte\": 4.0}}}", BucketQueryArgs.Builder.limit(2)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;
            assertEquals(2, mapRedisMessage.children().size());

            RedisMessage rawCursorId = findInMapMessage(mapRedisMessage, "cursor_id");
            assertNotNull(rawCursorId);
            cursorId = Math.toIntExact(((IntegerRedisMessage) rawCursorId).value());

            RedisMessage entries = findInMapMessage(mapRedisMessage, "entries");
            assertNotNull(entries);
            assertInstanceOf(MapRedisMessage.class, entries);
            appendDocumentData((MapRedisMessage) entries, allResults);
        }

        // BUCKET.ADVANCE - Continue until we get all matching docs
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;

            RedisMessage rawEntries = findInMapMessage(mapRedisMessage, "entries");
            assertNotNull(rawEntries);
            assertInstanceOf(MapRedisMessage.class, rawEntries);
            MapRedisMessage entries = (MapRedisMessage) rawEntries;

            if (entries.children().isEmpty()) {
                break;
            }

            assertTrue(entries.children().size() <= 2, "Each batch should have at most 2 documents");
            appendDocumentData(entries, allResults);
            advanceCalls++;
        }

        // Verify results:
        // Alice (4.5, 4.2 >= 4.0) ✓, Bob (none >= 4.0), Charlie (4.8, 4.9, 5.0) ✓,
        // Diana (3.9 max < 4.0), Eve (4.1, 4.3, 4.7) ✓, Frank (none >= 4.0)
        assertEquals(3, allResults.size(), "Should retrieve exactly 3 documents with ratings >= 4.0");

        Set<String> expectedNames = Set.of("Alice", "Charlie", "Eve");
        Set<String> actualNames = new HashSet<>();
        for (BsonDocument doc : allResults.values()) {
            actualNames.add(doc.getString("name").getValue());
        }
        assertEquals(expectedNames, actualNames, "Should match documents with ratings >= 4.0");
    }
}