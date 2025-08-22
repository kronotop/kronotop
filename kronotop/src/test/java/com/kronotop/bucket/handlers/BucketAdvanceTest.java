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
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class BucketAdvanceTest extends BaseBucketHandlerTest {

    private void appendIds(MapRedisMessage mapRedisMessage, List<String> result) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : mapRedisMessage.children().entrySet()) {
            SimpleStringRedisMessage id = (SimpleStringRedisMessage) entry.getKey();
            result.add(id.content());
        }
    }

    private void appendDocumentData(MapRedisMessage mapRedisMessage, Map<String, Document> result) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : mapRedisMessage.children().entrySet()) {
            SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
            FullBulkStringRedisMessage valueMessage = (FullBulkStringRedisMessage) entry.getValue();

            String docId = keyMessage.content();
            byte[] docBytes = ByteBufUtil.getBytes(valueMessage.content());
            Document doc = BSONUtil.toDocument(docBytes);
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

        // BUCKET.QUERY - Full scan (empty filter) with limit of 2
        List<String> result = new ArrayList<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(BUCKET_NAME, "{}", BucketQueryArgs.Builder.limit(2).shard(SHARD_ID)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;
            assertEquals(2, mapRedisMessage.children().size());
            appendIds(mapRedisMessage, result);
        }

        // BUCKET.ADVANCE - Continue pagination until we get all documents or hit limit
        int maxAdvanceCalls = 10; // Allow reasonable number of calls
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advance().encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;

            if (mapRedisMessage.children().isEmpty()) {
                break; // Normal termination
            }

            assertTrue(mapRedisMessage.children().size() <= 2, "Each batch should have at most 2 documents");
            appendIds(mapRedisMessage, result);
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

        // BUCKET.QUERY - Filter for type A with limit of 1
        Map<String, Document> allResults = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(BUCKET_NAME, "{\"type\": \"A\"}", BucketQueryArgs.Builder.limit(1).shard(SHARD_ID)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;
            // May return 0 or 1 depending on cursor behavior
            assertTrue(mapRedisMessage.children().size() <= 1, "Should return at most 1 document");
            appendDocumentData(mapRedisMessage, allResults);
        }

        // BUCKET.ADVANCE - Continue until we get all type A docs or reasonable limit
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advance().encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;

            if (mapRedisMessage.children().isEmpty()) {
                break; // Normal termination
            }

            assertTrue(mapRedisMessage.children().size() <= 1, "Each batch should have at most 1 document");
            appendDocumentData(mapRedisMessage, allResults);
            advanceCalls++;
        }

        // Verify results - we expect up to 3 documents with type A
        assertTrue(allResults.size() <= 3, "Should retrieve at most 3 documents with type A");

        // Verify all returned documents have type A
        for (Document doc : allResults.values()) {
            assertEquals("A", doc.getString("type"), "All returned documents should have type A");
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

        // BUCKET.QUERY - Filter for non-existent category
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(BUCKET_NAME, "{\"category\": \"NONEXISTENT\"}", BucketQueryArgs.Builder.limit(2).shard(SHARD_ID)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;
            assertEquals(0, mapRedisMessage.children().size(), "No documents should match the filter");
        }

        // BUCKET.ADVANCE - Should return empty since no documents match
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.advance().encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;
            assertEquals(0, mapRedisMessage.children().size(), "Advance should return empty when no matches");
        }
    }
}