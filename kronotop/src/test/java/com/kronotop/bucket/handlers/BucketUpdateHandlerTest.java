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

import com.kronotop.bucket.BSONUtil;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class BucketUpdateHandlerTest extends BaseBucketHandlerTest {

    @Test
    void test_bucket_update_with_set_operation() {
        // Step 1: Insert test documents with different ages
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25, \"city\": \"New York\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35, \"city\": \"London\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 45, \"city\": \"Paris\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 28, \"city\": \"Tokyo\"}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert documents and collect versionstamps
        Map<String, byte[]> insertedDocs = insertDocuments(testDocuments);
        List<String> allInsertedVersionstamps = new ArrayList<>(insertedDocs.keySet());

        assertEquals(4, allInsertedVersionstamps.size(), "Should have inserted 4 documents");

        // Step 2: Update documents with age > 30 using BUCKET.UPDATE to add a "status" field
        Set<String> updatedVersionstamps = new HashSet<>();
        {
            ByteBuf buf = Unpooled.buffer();
            byte[] update = BSONUtil.jsonToDocumentThenBytes("{\"$set\": {\"status\": \"senior\"}}");
            cmd.update(BUCKET_NAME, "{\"age\": {\"$gt\": 30}}", new String(update)).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            MapRedisMessage updateResponse = (MapRedisMessage) msg;

            // Extract versionstamps from update response
            RedisMessage versionstampsMessage = findInMapMessage(updateResponse, "versionstamp");
            assertNotNull(versionstampsMessage, "Update response should contain versionstamp field");
            assertInstanceOf(ArrayRedisMessage.class, versionstampsMessage);

            ArrayRedisMessage versionstampsArray = (ArrayRedisMessage) versionstampsMessage;
            for (RedisMessage versionstampMsg : versionstampsArray.children()) {
                SimpleStringRedisMessage versionstamp = (SimpleStringRedisMessage) versionstampMsg;
                updatedVersionstamps.add(versionstamp.content());
            }
        }

        // Should have updated 2 documents (Bob age 35, Charlie age 45)
        assertEquals(2, updatedVersionstamps.size(), "Should have updated 2 documents with age > 30");

        // Step 3: Query all documents to verify the update
        Map<String, Document> allDocuments = new HashMap<>();
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
                allDocuments.put(versionstamp, document);
            }
        }

        assertEquals(4, allDocuments.size(), "Should retrieve all 4 documents");

        // Step 4: Verify the updates
        for (Map.Entry<String, Document> entry : allDocuments.entrySet()) {
            String versionstamp = entry.getKey();
            Document document = entry.getValue();

            int age = document.getInteger("age");
            if (age > 30) {
                // Documents with age > 30 should have the new "status" field
                assertTrue(updatedVersionstamps.contains(versionstamp),
                    "Document with age " + age + " should be in updated versionstamps");
                assertEquals("senior", document.getString("status"),
                    "Document with age " + age + " should have status 'senior'");
            } else {
                // Documents with age <= 30 should NOT have the "status" field
                assertFalse(updatedVersionstamps.contains(versionstamp),
                    "Document with age " + age + " should NOT be in updated versionstamps");
                assertNull(document.getString("status"),
                    "Document with age " + age + " should NOT have status field");
            }
        }

        // Verify specific names that should have been updated
        boolean foundBobWithStatus = false;
        boolean foundCharlieWithStatus = false;
        boolean foundAliceWithoutStatus = false;
        boolean foundDianaWithoutStatus = false;

        for (Document doc : allDocuments.values()) {
            String name = doc.getString("name");
            String status = doc.getString("status");

            switch (name) {
                case "Bob", "Charlie" -> {
                    assertEquals("senior", status, name + " should have status 'senior'");
                    if ("Bob".equals(name)) foundBobWithStatus = true;
                    if ("Charlie".equals(name)) foundCharlieWithStatus = true;
                }
                case "Alice", "Diana" -> {
                    assertNull(status, name + " should NOT have status field");
                    if ("Alice".equals(name)) foundAliceWithoutStatus = true;
                    if ("Diana".equals(name)) foundDianaWithoutStatus = true;
                }
            }
        }

        assertTrue(foundBobWithStatus, "Bob should have been updated with status");
        assertTrue(foundCharlieWithStatus, "Charlie should have been updated with status");
        assertTrue(foundAliceWithoutStatus, "Alice should remain unchanged");
        assertTrue(foundDianaWithoutStatus, "Diana should remain unchanged");
    }

    @Test
    void test_bucket_update_with_unset_operation() {
        // Step 1: Insert test documents with extra fields that we'll remove
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25, \"city\": \"New York\", \"temp\": \"value1\", \"deprecated\": \"old\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35, \"city\": \"London\", \"temp\": \"value2\", \"deprecated\": \"old\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 45, \"city\": \"Paris\", \"temp\": \"value3\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 28, \"city\": \"Tokyo\", \"deprecated\": \"old\"}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert documents and collect versionstamps
        Map<String, byte[]> insertedDocs = insertDocuments(testDocuments);
        List<String> allInsertedVersionstamps = new ArrayList<>(insertedDocs.keySet());

        assertEquals(4, allInsertedVersionstamps.size(), "Should have inserted 4 documents");

        // Step 2: Update documents with age > 30 using BUCKET.UPDATE to remove "temp" and "deprecated" fields
        Set<String> updatedVersionstamps = new HashSet<>();
        {
            ByteBuf buf = Unpooled.buffer();
            byte[] update = BSONUtil.jsonToDocumentThenBytes("{\"$unset\": [\"temp\", \"deprecated\"]}");
            cmd.update(BUCKET_NAME, "{\"age\": {\"$gt\": 30}}", new String(update)).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            MapRedisMessage updateResponse = (MapRedisMessage) msg;

            // Extract versionstamps from update response
            RedisMessage versionstampsMessage = findInMapMessage(updateResponse, "versionstamp");
            assertNotNull(versionstampsMessage, "Update response should contain versionstamp field");
            assertInstanceOf(ArrayRedisMessage.class, versionstampsMessage);

            ArrayRedisMessage versionstampsArray = (ArrayRedisMessage) versionstampsMessage;
            for (RedisMessage versionstampMsg : versionstampsArray.children()) {
                SimpleStringRedisMessage versionstamp = (SimpleStringRedisMessage) versionstampMsg;
                updatedVersionstamps.add(versionstamp.content());
            }
        }

        // Should have updated 2 documents (Bob age 35, Charlie age 45)
        assertEquals(2, updatedVersionstamps.size(), "Should have updated 2 documents with age > 30");

        // Step 3: Query all documents to verify the unset operation
        Map<String, Document> allDocuments = new HashMap<>();
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
                allDocuments.put(versionstamp, document);
            }
        }

        assertEquals(4, allDocuments.size(), "Should retrieve all 4 documents");

        // Step 4: Verify the unset operations
        for (Map.Entry<String, Document> entry : allDocuments.entrySet()) {
            String versionstamp = entry.getKey();
            Document document = entry.getValue();

            int age = document.getInteger("age");
            if (age > 30) {
                // Documents with age > 30 should have "temp" and "deprecated" fields removed
                assertTrue(updatedVersionstamps.contains(versionstamp),
                    "Document with age " + age + " should be in updated versionstamps");
                assertNull(document.getString("temp"),
                    "Document with age " + age + " should NOT have temp field after unset");
                assertNull(document.getString("deprecated"),
                    "Document with age " + age + " should NOT have deprecated field after unset");
                // Other fields should remain
                assertNotNull(document.getString("name"), "name field should remain");
                assertNotNull(document.getString("city"), "city field should remain");
            } else {
                // Documents with age <= 30 should keep their original fields
                assertFalse(updatedVersionstamps.contains(versionstamp),
                    "Document with age " + age + " should NOT be in updated versionstamps");
            }
        }

        // Verify specific names that should have fields removed
        boolean foundBobWithoutFields = false;
        boolean foundCharlieWithoutTemp = false;
        boolean foundAliceWithFields = false;
        boolean foundDianaWithDeprecated = false;

        for (Document doc : allDocuments.values()) {
            String name = doc.getString("name");
            String temp = doc.getString("temp");
            String deprecated = doc.getString("deprecated");

            switch (name) {
                case "Bob" -> {
                    // Bob should have both temp and deprecated removed
                    assertNull(temp, "Bob should NOT have temp field");
                    assertNull(deprecated, "Bob should NOT have deprecated field");
                    foundBobWithoutFields = true;
                }
                case "Charlie" -> {
                    // Charlie should have temp removed (didn't have deprecated originally)
                    assertNull(temp, "Charlie should NOT have temp field");
                    foundCharlieWithoutTemp = true;
                }
                case "Alice" -> {
                    // Alice should keep all original fields (age <= 30)
                    assertEquals("value1", temp, "Alice should keep temp field");
                    assertEquals("old", deprecated, "Alice should keep deprecated field");
                    foundAliceWithFields = true;
                }
                case "Diana" -> {
                    // Diana should keep deprecated field (age <= 30, didn't have temp originally)
                    assertEquals("old", deprecated, "Diana should keep deprecated field");
                    foundDianaWithDeprecated = true;
                }
            }
        }

        assertTrue(foundBobWithoutFields, "Bob should have had fields removed");
        assertTrue(foundCharlieWithoutTemp, "Charlie should have had temp field removed");
        assertTrue(foundAliceWithFields, "Alice should have kept all fields");
        assertTrue(foundDianaWithDeprecated, "Diana should have kept deprecated field");
    }

    @Test
    void test_bucket_update_set_all_bson_types() {
        // Step 1: Insert a single test document that we'll update with all BSON types
        List<byte[]> testDocuments = Collections.singletonList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"TestDoc\", \"age\": 30}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert document and get its versionstamp
        Map<String, byte[]> insertedDocs = insertDocuments(testDocuments);
        List<String> allInsertedVersionstamps = new ArrayList<>(insertedDocs.keySet());

        assertEquals(1, allInsertedVersionstamps.size(), "Should have inserted 1 document");
        String targetVersionstamp = allInsertedVersionstamps.get(0);

        // Step 2: Update the specific document by _id to add all BSON value types
        Set<String> updatedVersionstamps = new HashSet<>();
        {
            ByteBuf buf = Unpooled.buffer();
            // Create update with all BSON types
            String updateJson = """
                {
                    "$set": {
                        "stringField": "Hello World",
                        "intField": 42,
                        "longField": 9223372036854775807,
                        "doubleField": 3.14159,
                        "booleanField": true,
                        "dateField": {"$date": "2023-01-01T00:00:00.000Z"},
                        "arrayField": [1, "two", true, null],
                        "objectField": {"nested": "value", "count": 5},
                        "nullField": null,
                        "binaryField": {"$binary": {"base64": "SGVsbG8=", "subType": "00"}}
                    }
                }
                """;

            byte[] update = BSONUtil.jsonToDocumentThenBytes(updateJson);
            cmd.update(BUCKET_NAME, "{\"_id\": {\"$eq\": \"" + targetVersionstamp + "\"}}", new String(update)).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            MapRedisMessage updateResponse = (MapRedisMessage) msg;

            // Extract versionstamps from update response
            RedisMessage versionstampsMessage = findInMapMessage(updateResponse, "versionstamp");
            assertNotNull(versionstampsMessage, "Update response should contain versionstamp field");
            assertInstanceOf(ArrayRedisMessage.class, versionstampsMessage);

            ArrayRedisMessage versionstampsArray = (ArrayRedisMessage) versionstampsMessage;
            for (RedisMessage versionstampMsg : versionstampsArray.children()) {
                SimpleStringRedisMessage versionstamp = (SimpleStringRedisMessage) versionstampMsg;
                updatedVersionstamps.add(versionstamp.content());
            }
        }

        // Should have updated exactly 1 document (the one with matching _id)
        assertEquals(1, updatedVersionstamps.size(), "Should have updated exactly 1 document");
        assertTrue(updatedVersionstamps.contains(targetVersionstamp), "Should have updated the target document");

        // Step 3: Query the specific document to verify all BSON types were set
        Document updatedDocument = null;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(BUCKET_NAME, "{\"_id\": {\"$eq\": \"" + targetVersionstamp + "\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            MapRedisMessage queryResponse = extractEntriesMap(msg);

            assertEquals(1, queryResponse.children().size(), "Should retrieve exactly 1 document");

            for (Map.Entry<RedisMessage, RedisMessage> entry : queryResponse.children().entrySet()) {
                SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
                FullBulkStringRedisMessage valueMessage = (FullBulkStringRedisMessage) entry.getValue();

                String versionstamp = keyMessage.content();
                assertEquals(targetVersionstamp, versionstamp, "Should be the target document");

                byte[] docBytes = ByteBufUtil.getBytes(valueMessage.content());
                updatedDocument = BSONUtil.toDocument(docBytes);
            }
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        // Step 4: Verify all BSON field types were set correctly
        // Original fields should remain
        assertEquals("TestDoc", updatedDocument.getString("name"), "Original name field should remain");
        assertEquals(30, updatedDocument.getInteger("age").intValue(), "Original age field should remain");

        // New BSON type fields should be set
        assertEquals("Hello World", updatedDocument.getString("stringField"), "String field should be set");
        assertEquals(42, updatedDocument.getInteger("intField").intValue(), "Int field should be set");
        assertEquals(9223372036854775807L, updatedDocument.getLong("longField").longValue(), "Long field should be set");
        assertEquals(3.14159, updatedDocument.getDouble("doubleField"), 0.00001, "Double field should be set");
        assertEquals(true, updatedDocument.getBoolean("booleanField"), "Boolean field should be set");

        // Verify date field (should be a Date object)
        assertNotNull(updatedDocument.getDate("dateField"), "Date field should be set");

        // Verify array field
        List<?> arrayField = updatedDocument.getList("arrayField", Object.class);
        assertNotNull(arrayField, "Array field should be set");
        assertEquals(4, arrayField.size(), "Array should have 4 elements");
        assertEquals(1, arrayField.get(0), "Array first element should be 1");
        assertEquals("two", arrayField.get(1), "Array second element should be 'two'");
        assertEquals(true, arrayField.get(2), "Array third element should be true");
        assertNull(arrayField.get(3), "Array fourth element should be null");

        // Verify nested object field
        Document objectField = updatedDocument.get("objectField", Document.class);
        assertNotNull(objectField, "Object field should be set");
        assertEquals("value", objectField.getString("nested"), "Nested object should have correct value");
        assertEquals(5, objectField.getInteger("count").intValue(), "Nested object should have correct count");

        // Verify null field
        assertNull(updatedDocument.get("nullField"), "Null field should be null");

        // Verify binary field exists (actual binary data verification depends on BSON implementation)
        assertNotNull(updatedDocument.get("binaryField"), "Binary field should be set");
    }
}