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
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class BucketDeleteHandlerTest extends BaseBucketHandlerTest {

    @Test
    void test_bucket_delete_with_age_filter() {
        // Step 1: Insert test documents with different ages
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25, \"city\": \"New York\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35, \"city\": \"London\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 45, \"city\": \"Paris\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 28, \"city\": \"Tokyo\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 38, \"city\": \"Berlin\"}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert documents and collect versionstamps
        Map<String, byte[]> insertedDocs = insertDocuments(testDocuments);
        List<String> allInsertedVersionstamps = new ArrayList<>(insertedDocs.keySet());


        assertEquals(5, allInsertedVersionstamps.size(), "Should have inserted 5 documents");

        // Step 2: Delete documents with age > 30 using BUCKET.DELETE
        Set<String> deletedVersionstamps = new HashSet<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(BUCKET_NAME, "{\"age\": {\"$gt\": 30}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            MapRedisMessage deleteResponse = (MapRedisMessage) msg;

            // Extract versionstamps from delete response
            RedisMessage versionstampsMessage = findInMapMessage(deleteResponse, "versionstamps");
            assertNotNull(versionstampsMessage, "Delete response should contain versionstamps field");
            assertInstanceOf(ArrayRedisMessage.class, versionstampsMessage);

            ArrayRedisMessage versionstampsArray = (ArrayRedisMessage) versionstampsMessage;
            for (RedisMessage versionstampMsg : versionstampsArray.children()) {
                SimpleStringRedisMessage versionstamp = (SimpleStringRedisMessage) versionstampMsg;
                deletedVersionstamps.add(versionstamp.content());
            }
        }

        // Verify that we deleted the right number of documents (Bob: 35, Charlie: 45, Eve: 38)
        assertEquals(3, deletedVersionstamps.size(), "Should have deleted 3 documents with age > 30");

        // Verify all deleted versionstamps were from our original insert
        for (String deletedVs : deletedVersionstamps) {
            assertTrue(allInsertedVersionstamps.contains(deletedVs),
                    "Deleted versionstamp should be from original insert: " + deletedVs);
        }

        // Step 3: Calculate remaining versionstamps (should be Alice: 25, Diana: 28)
        Set<String> expectedRemainingVersionstamps = allInsertedVersionstamps.stream()
                .filter(vs -> !deletedVersionstamps.contains(vs))
                .collect(Collectors.toSet());

        assertEquals(2, expectedRemainingVersionstamps.size(), "Should have 2 remaining documents");

        // Step 4: Query all remaining documents and verify
        Map<String, Document> actualRemainingDocuments = new HashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(BUCKET_NAME, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            MapRedisMessage queryResponse = extractEntriesMap(msg);

            for (Map.Entry<RedisMessage, RedisMessage> entry : queryResponse.children().entrySet()) {
                SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
                FullBulkStringRedisMessage valueMessage = (FullBulkStringRedisMessage) entry.getValue();

                String versionstamp = keyMessage.content();
                byte[] docBytes = ByteBufUtil.getBytes(valueMessage.content());
                Document doc = BSONUtil.toDocument(docBytes);

                actualRemainingDocuments.put(versionstamp, doc);
            }
        }

        // Step 5: Verify that remaining documents match expectations
        assertEquals(expectedRemainingVersionstamps.size(), actualRemainingDocuments.size(),
                "Query should return exactly the expected remaining documents");

        // Verify that all returned versionstamps are in our expected set
        for (String actualVs : actualRemainingDocuments.keySet()) {
            assertTrue(expectedRemainingVersionstamps.contains(actualVs),
                    "Remaining versionstamp should be in expected set: " + actualVs);
        }

        // Step 6: Verify that remaining documents have age <= 30
        for (Document doc : actualRemainingDocuments.values()) {
            Integer age = doc.getInteger("age");
            assertNotNull(age, "Document should have age field");
            assertTrue(age <= 30, "Remaining document should have age <= 30, but was: " + age);
        }

        // Verify specific expected documents remain (Alice: 25, Diana: 28)
        Set<String> remainingNames = actualRemainingDocuments.values().stream()
                .map(doc -> doc.getString("name"))
                .collect(Collectors.toSet());

        assertEquals(Set.of("Alice", "Diana"), remainingNames,
                "Should have Alice and Diana remaining after delete");
    }

    @Test
    void test_bucket_delete_no_matches() {
        // Insert documents but delete with a filter that matches none
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert documents
        insertDocuments(testDocuments);

        // Try to delete documents with age > 50 (should match none)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(BUCKET_NAME, "{\"age\": {\"$gt\": 50}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            MapRedisMessage deleteResponse = (MapRedisMessage) msg;

            // Should have an empty versionstamp array
            RedisMessage versionstampsMessage = findInMapMessage(deleteResponse, "versionstamps");
            assertNotNull(versionstampsMessage);
            assertInstanceOf(ArrayRedisMessage.class, versionstampsMessage);

            ArrayRedisMessage versionstampsArray = (ArrayRedisMessage) versionstampsMessage;
            assertEquals(0, versionstampsArray.children().size(), "Should delete no documents");
        }

        // Verify all documents still exist
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(BUCKET_NAME, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            MapRedisMessage queryResponse = extractEntriesMap(msg);
            assertEquals(2, queryResponse.children().size(), "All documents should still exist");
        }
    }

    @Test
    void test_bucket_delete_all_documents() {
        // Insert documents and delete all with an empty filter
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 35}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert documents
        Map<String, byte[]> insertedDocs = insertDocuments(testDocuments);
        List<String> insertedVersionstamps = new ArrayList<>(insertedDocs.keySet());

        // Delete all documents with an empty filter
        Set<String> deletedVersionstamps = new HashSet<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(BUCKET_NAME, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            MapRedisMessage deleteResponse = (MapRedisMessage) msg;
            assertNotNull(deleteResponse);
            RedisMessage versionstampsMessage = findInMapMessage(deleteResponse, "versionstamps");
            ArrayRedisMessage versionstampsArray = (ArrayRedisMessage) versionstampsMessage;

            for (RedisMessage versionstampMsg : versionstampsArray.children()) {
                SimpleStringRedisMessage versionstamp = (SimpleStringRedisMessage) versionstampMsg;
                deletedVersionstamps.add(versionstamp.content());
            }
        }

        // Verify all documents were deleted
        assertEquals(3, deletedVersionstamps.size(), "Should delete all 3 documents");
        assertEquals(new HashSet<>(insertedVersionstamps), deletedVersionstamps,
                "All inserted versionstamps should be deleted");

        // Verify the bucket is empty
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(BUCKET_NAME, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            MapRedisMessage queryResponse = extractEntriesMap(msg);
            assertEquals(0, queryResponse.children().size(), "Bucket should be empty after delete all");
        }
    }

    @Test
    void test_bucket_delete_within_transaction() {
        // Step 1: Insert test documents with different ages (outside transaction)
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25, \"city\": \"New York\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35, \"city\": \"London\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 45, \"city\": \"Paris\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 28, \"city\": \"Tokyo\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 38, \"city\": \"Berlin\"}")
        );

        // Insert documents and collect versionstamps
        Map<String, byte[]> insertedDocs = insertDocuments(testDocuments);
        List<String> allInsertedVersionstamps = new ArrayList<>(insertedDocs.keySet());

        assertEquals(5, allInsertedVersionstamps.size(), "Should have inserted 5 documents");

        // Step 2: Delete documents with age > 30 within a transaction
        KronotopCommandBuilder<String, String> kronotopCmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        BucketCommandBuilder<String, String> bucketCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(bucketCmd, RESPVersion.RESP3);

        Set<String> deletedVersionstamps = new HashSet<>();

        // BEGIN
        {
            ByteBuf buf = Unpooled.buffer();
            // Create a new transaction
            kronotopCmd.begin().encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        // BUCKET.DELETE within transaction
        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.delete(BUCKET_NAME, "{\"age\": {\"$gt\": 30}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            MapRedisMessage deleteResponse = (MapRedisMessage) msg;

            // BUCKET.DELETE always returns actual versionstamps as strings
            RedisMessage versionstampsMessage = findInMapMessage(deleteResponse, "versionstamps");
            assertNotNull(versionstampsMessage, "Delete response should contain versionstamps field");
            assertInstanceOf(ArrayRedisMessage.class, versionstampsMessage);

            ArrayRedisMessage versionstampsArray = (ArrayRedisMessage) versionstampsMessage;
            // Should return actual versionstamps as strings
            for (RedisMessage versionstampMsg : versionstampsArray.children()) {
                assertInstanceOf(SimpleStringRedisMessage.class, versionstampMsg, "BUCKET.DELETE always returns versionstamps as strings");
                SimpleStringRedisMessage versionstamp = (SimpleStringRedisMessage) versionstampMsg;
                deletedVersionstamps.add(versionstamp.content());
            }

            // Verify that we deleted the right number of documents (Bob: 35, Charlie: 45, Eve: 38)
            assertEquals(3, deletedVersionstamps.size(), "Should have deleted 3 documents with age > 30");

            // Verify all deleted versionstamps were from our original insert
            for (String deletedVs : deletedVersionstamps) {
                assertTrue(allInsertedVersionstamps.contains(deletedVs),
                        "Deleted versionstamp should be from original insert: " + deletedVs);
            }
        }

        // COMMIT
        {
            // Commit the changes
            ByteBuf buf = Unpooled.buffer();
            kronotopCmd.commit().encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Step 3: Verify deletion by querying remaining documents
        Map<String, Document> actualRemainingDocuments = new HashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.query(BUCKET_NAME, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            MapRedisMessage queryResponse = extractEntriesMap(msg);

            for (Map.Entry<RedisMessage, RedisMessage> entry : queryResponse.children().entrySet()) {
                SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
                FullBulkStringRedisMessage valueMessage = (FullBulkStringRedisMessage) entry.getValue();

                String versionstamp = keyMessage.content();
                byte[] docBytes = ByteBufUtil.getBytes(valueMessage.content());
                Document doc = BSONUtil.toDocument(docBytes);

                actualRemainingDocuments.put(versionstamp, doc);
            }
        }

        // Step 4: Verify that documents with age > 30 were deleted
        assertEquals(2, actualRemainingDocuments.size(), "Should have 2 remaining documents after transaction delete");

        // Verify that remaining documents have age <= 30
        for (Document doc : actualRemainingDocuments.values()) {
            Integer age = doc.getInteger("age");
            assertNotNull(age, "Document should have age field");
            assertTrue(age <= 30, "Remaining document should have age <= 30, but was: " + age);
        }

        // Verify specific expected documents remain (Alice: 25, Diana: 28)
        Set<String> remainingNames = actualRemainingDocuments.values().stream()
                .map(doc -> doc.getString("name"))
                .collect(Collectors.toSet());

        assertEquals(Set.of("Alice", "Diana"), remainingNames,
                "Should have Alice and Diana remaining after transaction delete");
    }

    @Test
    void test_bucket_delete_with_limit_and_advance() {
        // Step 1: Insert test documents with different ages
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 35, \"city\": \"New York\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 40, \"city\": \"London\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 45, \"city\": \"Paris\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 25, \"city\": \"Tokyo\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 50, \"city\": \"Berlin\"}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert documents and collect versionstamps
        Map<String, byte[]> insertedDocs = insertDocuments(testDocuments);
        List<String> allInsertedVersionstamps = new ArrayList<>(insertedDocs.keySet());

        assertEquals(5, allInsertedVersionstamps.size(), "Should have inserted 5 documents");

        // Step 2: Delete documents with age > 30 using limit=1
        Set<String> allDeletedVersionstamps = new HashSet<>();
        int cursorId;

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(BUCKET_NAME, "{\"age\": {\"$gt\": 30}}", BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage deleteResponse = (MapRedisMessage) msg;

            // Extract cursor_id
            RedisMessage rawCursorId = findInMapMessage(deleteResponse, "cursor_id");
            assertNotNull(rawCursorId, "Delete response should contain cursor_id field");
            assertInstanceOf(IntegerRedisMessage.class, rawCursorId);
            cursorId = Math.toIntExact(((IntegerRedisMessage) rawCursorId).value());

            // Extract versionstamps from initial delete response
            RedisMessage versionstampsMessage = findInMapMessage(deleteResponse, "versionstamps");
            assertNotNull(versionstampsMessage, "Delete response should contain versionstamps field");
            assertInstanceOf(ArrayRedisMessage.class, versionstampsMessage);

            ArrayRedisMessage versionstampsArray = (ArrayRedisMessage) versionstampsMessage;
            for (RedisMessage versionstampMsg : versionstampsArray.children()) {
                SimpleStringRedisMessage versionstamp = (SimpleStringRedisMessage) versionstampMsg;
                allDeletedVersionstamps.add(versionstamp.content());
            }
        }

        // Should have deleted exactly 1 document in the first batch
        assertEquals(1, allDeletedVersionstamps.size(), "Should have deleted exactly 1 document with limit=1");

        // Step 3: Use BUCKET.ADVANCE DELETE to continue deleting remaining documents
        int maxAdvanceCalls = 10; // Safety limit
        int advanceCalls = 0;

        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceDelete(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            MapRedisMessage mapResponse = (MapRedisMessage) msg;

            // Extract versionstamps from the map response
            RedisMessage versionstampsMessage = findInMapMessage(mapResponse, "versionstamps");
            if (versionstampsMessage == null) {
                break; // No more deletions to process
            }

            assertInstanceOf(ArrayRedisMessage.class, versionstampsMessage);
            ArrayRedisMessage advanceResponse = (ArrayRedisMessage) versionstampsMessage;

            // If no more entries, we're done
            if (advanceResponse.children().isEmpty()) {
                break;
            }

            // Extract versionstamps from advance response
            for (RedisMessage versionstampMsg : advanceResponse.children()) {
                SimpleStringRedisMessage versionstamp = (SimpleStringRedisMessage) versionstampMsg;
                allDeletedVersionstamps.add(versionstamp.content());
            }

            advanceCalls++;
        }

        // Should have deleted 4 documents total (age > 30: Alice=35, Bob=40, Charlie=45, Eve=50)
        assertEquals(4, allDeletedVersionstamps.size(), "Should have deleted 4 documents with age > 30");

        // Verify all deleted versionstamps were from our original insert
        for (String deletedVs : allDeletedVersionstamps) {
            assertTrue(allInsertedVersionstamps.contains(deletedVs),
                    "Deleted versionstamp should be from original insert: " + deletedVs);
        }

        // Step 4: Query remaining documents to verify only Diana (age=25) remains
        Map<String, Document> remainingDocuments = new HashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(BUCKET_NAME, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            MapRedisMessage queryResponse = extractEntriesMap(msg);

            for (Map.Entry<RedisMessage, RedisMessage> entry : queryResponse.children().entrySet()) {
                SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
                FullBulkStringRedisMessage valueMessage = (FullBulkStringRedisMessage) entry.getValue();

                String versionstamp = keyMessage.content();
                byte[] docBytes = ByteBufUtil.getBytes(valueMessage.content());
                Document document = BSONUtil.toDocument(docBytes);
                remainingDocuments.put(versionstamp, document);
            }
        }

        // Step 5: Verify only Diana (age=25) remains
        assertEquals(1, remainingDocuments.size(), "Should have 1 remaining document after delete");

        Document remainingDoc = remainingDocuments.values().iterator().next();
        assertEquals("Diana", remainingDoc.getString("name"), "Remaining document should be Diana");
        assertEquals(25, remainingDoc.getInteger("age"), "Remaining document should have age 25");

        // Verify that none of the remaining versionstamps were deleted
        for (String remainingVs : remainingDocuments.keySet()) {
            assertFalse(allDeletedVersionstamps.contains(remainingVs),
                    "Remaining versionstamp should NOT be in deleted versionstamps: " + remainingVs);
        }
    }
}