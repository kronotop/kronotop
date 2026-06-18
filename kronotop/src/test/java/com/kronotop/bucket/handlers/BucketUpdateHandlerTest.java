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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BsonHelper;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.*;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.commands.BucketQueryArgs;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.*;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class BucketUpdateHandlerTest extends BaseBucketHandlerTest {

    private static final String COLLATION_BUCKET = "collation-bucket";
    private static final String VECTOR_BUCKET = "vector-test-bucket";
    private static final String VECTOR_INDEX_JSON = "{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}";

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
    void shouldUpdateWithRegexFilter() {
        // Behavior: BUCKET.UPDATE with a $regex filter applies the mutation only to matching string documents.
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alana\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        insertDocumentsAndGetObjectIds(documents);

        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"name\": {\"$regex\": \"^Al\"}}", "{\"$set\": {\"group\": \"a\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(2, updatedObjectIds.size(), "Should update the two documents whose name starts with 'Al'");

        ByteBuf queryBuf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(queryBuf);
        Object queryMsg = runCommand(channel, queryBuf);
        assertInstanceOf(MapRedisMessage.class, queryMsg);

        for (BsonDocument doc : extractEntries(queryMsg)) {
            String name = BsonHelper.getString(doc, "name");
            if ("Alice".equals(name) || "Alana".equals(name)) {
                assertEquals("a", BsonHelper.getString(doc, "group"), name + " should have group 'a'");
            } else {
                assertNull(BsonHelper.getString(doc, "group"), name + " should not have a group field");
            }
        }
    }

    @Test
    void shouldUpdateWithSetOperation() {
        // Step 1: Insert test documents with different ages
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25, \"city\": \"New York\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35, \"city\": \"London\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 45, \"city\": \"Paris\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 28, \"city\": \"Tokyo\"}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert documents and collect object IDs
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        List<ObjectId> allInsertedObjectIds = new ArrayList<>(insertedDocs.keySet());

        assertEquals(4, allInsertedObjectIds.size(), "Should have inserted 4 documents");

        // Step 2: Update documents with age > 30 using BUCKET.UPDATE to add a "status" field
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"age\": {\"$gt\": 30}}", "{\"$set\": {\"status\": \"senior\"}}").encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            updatedObjectIds = extractObjectIds(msg);
        }

        // Should have updated 2 documents (Bob age 35, Charlie age 45)
        assertEquals(2, updatedObjectIds.size(), "Should have updated 2 documents with age > 30");

        // Step 3: Query all documents to verify the update
        Map<ObjectId, BsonDocument> allDocuments = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument document : entries) {
                ObjectId oid = document.getObjectId("_id").getValue();
                allDocuments.put(oid, document);
            }
        }

        assertEquals(4, allDocuments.size(), "Should retrieve all 4 documents");

        // Step 4: Verify the updates
        for (Map.Entry<ObjectId, BsonDocument> entry : allDocuments.entrySet()) {
            ObjectId oid = entry.getKey();
            BsonDocument document = entry.getValue();

            Integer age = BsonHelper.getInteger(document, "age");
            assertNotNull(age);

            if (age > 30) {
                // Documents with age > 30 should have the new "status" field
                assertTrue(updatedObjectIds.contains(oid),
                        "Document with age " + age + " should be in updated object IDs");
                assertEquals("senior", BsonHelper.getString(document, "status"),
                        "Document with age " + age + " should have status 'senior'");
            } else {
                // Documents with age <= 30 should NOT have the "status" field
                assertFalse(updatedObjectIds.contains(oid),
                        "Document with age " + age + " should NOT be in updated object IDs");
                assertNull(BsonHelper.getString(document, "status"),
                        "Document with age " + age + " should NOT have status field");
            }
        }

        // Verify specific names that should have been updated
        boolean foundBobWithStatus = false;
        boolean foundCharlieWithStatus = false;
        boolean foundAliceWithoutStatus = false;
        boolean foundDianaWithoutStatus = false;

        for (BsonDocument doc : allDocuments.values()) {
            String name = BsonHelper.getString(doc, "name");
            String status = BsonHelper.getString(doc, "status");

            assertNotNull(name);

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
    void shouldUpdateWithUnsetOperation() {
        // Step 1: Insert test documents with extra fields that we'll remove
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25, \"city\": \"New York\", \"temp\": \"value1\", \"deprecated\": \"old\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35, \"city\": \"London\", \"temp\": \"value2\", \"deprecated\": \"old\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 45, \"city\": \"Paris\", \"temp\": \"value3\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 28, \"city\": \"Tokyo\", \"deprecated\": \"old\"}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert documents and collect object IDs
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        List<ObjectId> allInsertedObjectIds = new ArrayList<>(insertedDocs.keySet());

        assertEquals(4, allInsertedObjectIds.size(), "Should have inserted 4 documents");

        // Step 2: Update documents with age > 30 using BUCKET.UPDATE to remove "temp" and "deprecated" fields
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"age\": {\"$gt\": 30}}", "{\"$unset\": [\"temp\", \"deprecated\"]}").encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            updatedObjectIds = extractObjectIds(msg);
        }

        // Should have updated 2 documents (Bob age 35, Charlie age 45)
        assertEquals(2, updatedObjectIds.size(), "Should have updated 2 documents with age > 30");

        // Step 3: Query all documents to verify the unset operation
        Map<ObjectId, BsonDocument> allDocuments = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument document : entries) {
                ObjectId oid = document.getObjectId("_id").getValue();
                allDocuments.put(oid, document);
            }
        }

        assertEquals(4, allDocuments.size(), "Should retrieve all 4 documents");

        // Step 4: Verify the unset operations
        for (Map.Entry<ObjectId, BsonDocument> entry : allDocuments.entrySet()) {
            ObjectId oid = entry.getKey();
            BsonDocument document = entry.getValue();

            Integer age = BsonHelper.getInteger(document, "age");
            assertNotNull(age);

            if (age > 30) {
                // Documents with age > 30 should have "temp" and "deprecated" fields removed
                assertTrue(updatedObjectIds.contains(oid),
                        "Document with age " + age + " should be in updated object IDs");
                assertNull(BsonHelper.getString(document, "temp"),
                        "Document with age " + age + " should NOT have temp field after unset");
                assertNull(BsonHelper.getString(document, "deprecated"),
                        "Document with age " + age + " should NOT have deprecated field after unset");
                // Other fields should remain
                assertNotNull(BsonHelper.getString(document, "name"), "name field should remain");
                assertNotNull(BsonHelper.getString(document, "city"), "city field should remain");
            } else {
                // Documents with age <= 30 should keep their original fields
                assertFalse(updatedObjectIds.contains(oid),
                        "Document with age " + age + " should NOT be in updated object IDs");
            }
        }

        // Verify specific names that should have fields removed
        boolean foundBobWithoutFields = false;
        boolean foundCharlieWithoutTemp = false;
        boolean foundAliceWithFields = false;
        boolean foundDianaWithDeprecated = false;

        for (BsonDocument doc : allDocuments.values()) {
            String name = BsonHelper.getString(doc, "name");
            String temp = BsonHelper.getString(doc, "temp");
            String deprecated = BsonHelper.getString(doc, "deprecated");

            assertNotNull(name);

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
                    // Diana should keep the deprecated field (age <= 30, didn't have temp originally)
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
    void shouldSetAllBsonTypes() {
        // Step 1: Insert a single test document that we'll update with all BSON types
        List<byte[]> testDocuments = Collections.singletonList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"TestDoc\", \"age\": 30}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert document and get its object ID
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        List<ObjectId> allInsertedObjectIds = new ArrayList<>(insertedDocs.keySet());

        assertEquals(1, allInsertedObjectIds.size(), "Should have inserted 1 document");
        ObjectId targetObjectId = allInsertedObjectIds.getFirst();

        // Step 2: Update the specific document by _id to add all BSON value types
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();

            // Create update document with native BSON types
            BsonDocument setDoc = new BsonDocument()
                    .append("stringField", new BsonString("Hello World"))
                    .append("intField", new BsonInt32(42))
                    .append("longField", new BsonInt64(9223372036854775807L))
                    .append("doubleField", new BsonDouble(3.14159))
                    .append("booleanField", new BsonBoolean(true))
                    .append("dateField", new BsonDateTime(1672531200000L)) // 2023-01-01T00:00:00.000Z
                    .append("arrayField", new BsonArray(Arrays.asList(
                            new BsonInt32(1),
                            new BsonString("two"),
                            new BsonBoolean(true),
                            new BsonNull()
                    )))
                    .append("objectField", new BsonDocument()
                            .append("nested", new BsonString("value"))
                            .append("count", new BsonInt32(5)))
                    .append("nullField", new BsonNull())
                    .append("binaryField", new BsonBinary("Hello".getBytes()))
                    .append("decimal128Field", new BsonDecimal128(new Decimal128(new BigDecimal("123.456"))));

            BsonDocument updateDoc = new BsonDocument("$set", setDoc);
            byte[] update = BSONUtil.toBytes(updateDoc);
            cmd.update(TEST_BUCKET, "{\"_id\": {\"$eq\": \"" + targetObjectId.toHexString() + "\"}}", update).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            updatedObjectIds = extractObjectIds(msg);
        }

        // Should have updated exactly 1 document (the one with matching _id)
        assertEquals(1, updatedObjectIds.size(), "Should have updated exactly 1 document");
        assertTrue(updatedObjectIds.contains(targetObjectId), "Should have updated the target document");

        // Step 3: Query the specific document to verify all BSON types were set
        BsonDocument updatedDocument = null;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"_id\": {\"$eq\": \"" + targetObjectId.toHexString() + "\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");

            updatedDocument = entries.getFirst();
            ObjectId resultOid = updatedDocument.getObjectId("_id").getValue();
            assertEquals(targetObjectId, resultOid, "Should be the target document");
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        // Step 4: Verify all BSON field types were set correctly
        // Original fields should remain
        assertEquals("TestDoc", BsonHelper.getString(updatedDocument, "name"), "Original name field should remain");
        assertEquals(30, BsonHelper.getInteger(updatedDocument, "age").intValue(), "Original age field should remain");

        // New BSON type fields should be set
        assertEquals("Hello World", BsonHelper.getString(updatedDocument, "stringField"), "String field should be set");
        assertEquals(42, BsonHelper.getInteger(updatedDocument, "intField").intValue(), "Int field should be set");
        assertEquals(9223372036854775807L, BsonHelper.getLong(updatedDocument, "longField").longValue(), "Long field should be set");
        assertEquals(3.14159, BsonHelper.getDouble(updatedDocument, "doubleField"), 0.00001, "Double field should be set");
        assertEquals(true, BsonHelper.getBoolean(updatedDocument, "booleanField"), "Boolean field should be set");

        // Verify date field
        assertNotNull(BsonHelper.getDateTime(updatedDocument, "dateField"), "Date field should be set");

        // Verify array field
        BsonArray arrayField = BsonHelper.getArray(updatedDocument, "arrayField");
        assertNotNull(arrayField, "Array field should be set");
        assertEquals(4, arrayField.size(), "Array should have 4 elements");
        assertEquals(1, arrayField.get(0).asInt32().getValue(), "Array first element should be 1");
        assertEquals("two", arrayField.get(1).asString().getValue(), "Array second element should be 'two'");
        assertTrue(arrayField.get(2).asBoolean().getValue(), "Array third element should be true");
        assertTrue(arrayField.get(3).isNull(), "Array fourth element should be null");

        // Verify nested object field
        BsonDocument objectField = BsonHelper.getDocument(updatedDocument, "objectField");
        assertNotNull(objectField, "Object field should be set");
        assertEquals("value", BsonHelper.getString(objectField, "nested"), "Nested object should have correct value");
        assertEquals(5, Objects.requireNonNull(BsonHelper.getInteger(objectField, "count")).intValue(), "Nested object should have correct count");

        // Verify null field
        assertTrue(updatedDocument.get("nullField").isNull(), "Null field should be null");

        // Verify binary field exists
        assertNotNull(updatedDocument.get("binaryField"), "Binary field should be set");

        // Verify decimal128 field
        assertNotNull(updatedDocument.get("decimal128Field"), "Decimal128 field should be set");
    }

    @Test
    void shouldUpdateWithLimitAndAdvance() {
        // Step 1: Insert test documents - more than limit to test pagination
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 35, \"city\": \"New York\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 40, \"city\": \"London\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 45, \"city\": \"Paris\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 25, \"city\": \"Tokyo\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 50, \"city\": \"Berlin\"}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert documents and collect object IDs
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        List<ObjectId> allInsertedObjectIds = new ArrayList<>(insertedDocs.keySet());

        assertEquals(5, allInsertedObjectIds.size(), "Should have inserted 5 documents");

        // Step 2: Update documents with age > 30 using BUCKET.UPDATE with limit=1 to add a "status" field
        List<ObjectId> allUpdatedObjectIds = new ArrayList<>();
        int cursorId = -1;

        // Initial update with limit=1
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"age\": {\"$gt\": 30}}", "{\"$set\": {\"status\": \"senior\"}}", BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            allUpdatedObjectIds.addAll(extractObjectIds(msg));
        }

        // Should have updated exactly 1 document in the first batch
        assertEquals(1, allUpdatedObjectIds.size(), "Should have updated exactly 1 document with limit=1");

        // Step 3: Use BUCKET.ADVANCE UPDATE to continue updating remaining documents
        int maxAdvanceCalls = 10; // Safety limit
        int advanceCalls = 0;

        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceUpdate(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<ObjectId> advancedIds = extractObjectIds(msg);
            if (advancedIds.isEmpty()) {
                break;
            }

            allUpdatedObjectIds.addAll(advancedIds);

            advanceCalls++;
        }

        // Should have updated 4 documents total (age > 30: Alice=35, Bob=40, Charlie=45, Eve=50)
        assertEquals(4, allUpdatedObjectIds.size(), "Should have updated 4 documents with age > 30");

        // Step 4: Query all documents to verify the updates
        Map<ObjectId, BsonDocument> allDocuments = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument document : entries) {
                ObjectId oid = document.getObjectId("_id").getValue();
                allDocuments.put(oid, document);
            }
        }

        assertEquals(5, allDocuments.size(), "Should retrieve all 5 documents");

        // Step 5: Verify the updates were applied correctly
        int documentsWithStatus = 0;
        int documentsWithoutStatus = 0;

        for (Map.Entry<ObjectId, BsonDocument> entry : allDocuments.entrySet()) {
            ObjectId oid = entry.getKey();
            BsonDocument document = entry.getValue();

            int age = BsonHelper.getInteger(document, "age");
            String status = BsonHelper.getString(document, "status");

            if (age > 30) {
                // Documents with age > 30 should have been updated
                assertTrue(allUpdatedObjectIds.contains(oid),
                        "Document with age " + age + " should be in updated object IDs");
                assertEquals("senior", status,
                        "Document with age " + age + " should have status 'senior'");
                documentsWithStatus++;
            } else {
                // Documents with age <= 30 should NOT have been updated
                assertFalse(allUpdatedObjectIds.contains(oid),
                        "Document with age " + age + " should NOT be in updated object IDs");
                assertNull(status,
                        "Document with age " + age + " should NOT have status field");
                documentsWithoutStatus++;
            }
        }

        // Verify counts
        assertEquals(4, documentsWithStatus, "Should have 4 documents with status field");
        assertEquals(1, documentsWithoutStatus, "Should have 1 document without status field (Diana age=25)");

        // Verify specific names that should have been updated
        List<String> updatedNames = new ArrayList<>();
        for (BsonDocument doc : allDocuments.values()) {
            if ("senior".equals(BsonHelper.getString(doc, "status"))) {
                updatedNames.add(BsonHelper.getString(doc, "name"));
            }
        }

        updatedNames.sort(Comparator.naturalOrder());
        assertEquals(List.of("Alice", "Bob", "Charlie", "Eve"), updatedNames, "Should have updated Alice, Bob, Charlie, and Eve");
    }

    @Test
    void shouldThrowBucketBeingRemovedExceptionWhenUpdatingRemovedBucket() {
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

        // Try to update the dropped bucket
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"status\": \"updated\"}}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertEquals("BUCKETBEINGREMOVED Bucket 'test-bucket' is being removed", errorMessage.content());
    }

    @Test
    void shouldThrowIndexTypeMismatchExceptionWhenUpdatingWithWrongType() {
        // Create an index expecting INT32 for 'age' field
        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        // Insert a document with correct type
        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf insertBuf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"John\", \"age\": 25}");
        insertCmd.insert(TEST_BUCKET, documentBytes).encode(insertBuf);
        Object insertMsg = runCommand(channel, insertBuf);
        assertInstanceOf(ArrayRedisMessage.class, insertMsg);

        // Update the document with wrong type (string instead of int)
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"age\": \"twenty-five\"}}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().startsWith("INDEXTYPE_MISMATCH"),
                "Should return INDEXTYPE_MISMATCH error");
        assertTrue(errorMessage.content().contains("index 'age-index' expects 'INT32'"),
                "Error message should mention the index name and expected type");
        assertTrue(errorMessage.content().contains("selector 'age' matched a value of type 'STRING'"),
                "Error message should mention the selector and actual type");
    }

    @Test
    void shouldThrowIndexTypeMismatchExceptionWhenUpdatingInt32IndexWithDouble() {
        // Create an index expecting INT32 for 'count' field
        SingleFieldIndexDefinition countIndexDefinition = SingleFieldIndexDefinition.create("count-index", "count", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(countIndexDefinition);

        // Insert a document with correct type
        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf insertBuf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Test\", \"count\": 10}");
        insertCmd.insert(TEST_BUCKET, documentBytes).encode(insertBuf);
        Object insertMsg = runCommand(channel, insertBuf);
        assertInstanceOf(ArrayRedisMessage.class, insertMsg);

        // Update the document with wrong type (double instead of int32)
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"count\": 3.14}}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().startsWith("INDEXTYPE_MISMATCH"),
                "Should return INDEXTYPE_MISMATCH error for DOUBLE value with INT32 index");
    }

    @Test
    void shouldThrowIndexTypeMismatchExceptionWhenArrayFilterUpdateProducesWrongType() {
        // Behavior: When strict_types=true (default), array_filter updates that produce
        // type-mismatched values for indexed fields throw IndexTypeMismatchException.
        // The update is rejected to maintain index integrity.

        // Create an INT32 multikey index on 'scores'
        SingleFieldIndexDefinition scoresIndexDefinition = SingleFieldIndexDefinition.create("scores-index", "scores", BsonType.INT32, true, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(scoresIndexDefinition);

        // Insert document with INT32 array [55, 60, 65]
        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf insertBuf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"scores\": [55, 60, 65]}");
        insertCmd.insert(TEST_BUCKET, documentBytes).encode(insertBuf);
        Object insertMsg = runCommand(channel, insertBuf);
        assertInstanceOf(ArrayRedisMessage.class, insertMsg);

        // Update: change elements <= 60 to STRING "zero" using array_filters
        // This should throw IndexTypeMismatchException because strict_types=true
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(
                TEST_BUCKET,
                "{\"name\": {\"$eq\": \"Alice\"}}",
                "{\"$set\": {\"scores.$[low]\": \"zero\"}, \"array_filters\": [{\"low\": {\"$lte\": 60}}]}"
        ).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().startsWith("INDEXTYPE_MISMATCH"),
                "Should return INDEXTYPE_MISMATCH error for STRING value with INT32 index");
        assertTrue(errorMessage.content().contains("index 'scores-index' expects 'INT32'"),
                "Error message should mention the index name and expected type");
        assertTrue(errorMessage.content().contains("selector 'scores' matched a value of type 'STRING'"),
                "Error message should mention the selector and actual type");
    }

    @Test
    void shouldUpdateWithSortByLimitAndAdvance() {
        // Behavior: UPDATE with SORTBY and LIMIT preserves sort order across BUCKET.ADVANCE calls.
        // Documents are updated in the specified order, and cursor continuation maintains global ordering.

        // Step 1: Create index on age field for sorted updates
        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        // Step 2: Insert documents with shuffled ages (50, 20, 40, 10, 30)
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 40}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 30}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert documents and build id-to-age mapping
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        Map<ObjectId, Integer> idToAge = new LinkedHashMap<>();
        for (Map.Entry<ObjectId, byte[]> entry : insertedDocs.entrySet()) {
            BsonDocument doc = BSONUtil.toBsonDocument(entry.getValue());
            idToAge.put(entry.getKey(), BsonHelper.getInteger(doc, "age"));
        }

        assertEquals(5, insertedDocs.size(), "Should have inserted 5 documents");

        // Step 3: Execute UPDATE with SORTBY age ASC, LIMIT 2
        List<ObjectId> allUpdatedObjectIds = new ArrayList<>();
        int cursorId;

        // Initial update with SORTBY age ASC, LIMIT 2
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"age\": {\"$gte\": 0}}", "{\"$set\": {\"processed\": true}}",
                    BucketQueryArgs.Builder.limit(2).sortBy("age", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            allUpdatedObjectIds.addAll(extractObjectIds(msg));
        }

        // First batch should have 2 documents (ages 10, 20 - the two lowest)
        assertEquals(2, allUpdatedObjectIds.size(), "First batch should update 2 documents");

        // Step 4: Use BUCKET.ADVANCE UPDATE to continue updating remaining documents
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;

        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceUpdate(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<ObjectId> advancedIds = extractObjectIds(msg);
            if (advancedIds.isEmpty()) {
                break;
            }

            allUpdatedObjectIds.addAll(advancedIds);

            advanceCalls++;
        }

        // Step 5: Verify all 5 documents were updated
        assertEquals(5, allUpdatedObjectIds.size(), "Should have updated all 5 documents");

        // Step 6: Verify the update order matches ascending age order (10, 20, 30, 40, 50)
        List<Integer> actualUpdateOrder = new ArrayList<>();
        for (ObjectId oid : allUpdatedObjectIds) {
            Integer age = idToAge.get(oid);
            assertNotNull(age, "ObjectId should have a corresponding age");
            actualUpdateOrder.add(age);
        }

        List<Integer> expectedOrder = Arrays.asList(10, 20, 30, 40, 50);
        assertEquals(expectedOrder, actualUpdateOrder,
                "Documents should be updated in ascending age order across all cursor advances");

        // Step 7: Verify all documents have processed=true
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(5, entries.size(), "Should retrieve all 5 documents");

            for (BsonDocument document : entries) {
                assertEquals(Boolean.TRUE, BsonHelper.getBoolean(document, "processed"), "All documents should have processed=true");
            }
        }
    }

    @Test
    void shouldUpdateWithSortByLimitAndAdvanceDescending() {
        // Behavior: UPDATE with SORTBY DESC and LIMIT preserves descending sort order across
        // BUCKET.ADVANCE calls. Documents are updated from highest to lowest value.

        // Step 1: Create index on age field
        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        // Step 2: Insert documents with shuffled ages
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 40}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 30}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        Map<ObjectId, Integer> idToAge = new LinkedHashMap<>();
        for (Map.Entry<ObjectId, byte[]> entry : insertedDocs.entrySet()) {
            BsonDocument doc = BSONUtil.toBsonDocument(entry.getValue());
            idToAge.put(entry.getKey(), BsonHelper.getInteger(doc, "age"));
        }

        // Step 3: Execute UPDATE with SORTBY age DESC, LIMIT 2
        List<ObjectId> allUpdatedObjectIds = new ArrayList<>();
        int cursorId;

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"age\": {\"$gte\": 0}}", "{\"$set\": {\"processed\": true}}",
                    BucketQueryArgs.Builder.limit(2).sortBy("age", "DESC")).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            allUpdatedObjectIds.addAll(extractObjectIds(msg));
        }

        assertEquals(2, allUpdatedObjectIds.size(), "First batch should update 2 documents");

        // Step 4: Continue with BUCKET.ADVANCE
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;

        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceUpdate(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<ObjectId> advancedIds = extractObjectIds(msg);
            if (advancedIds.isEmpty()) {
                break;
            }

            allUpdatedObjectIds.addAll(advancedIds);

            advanceCalls++;
        }

        // Step 5: Verify update order matches descending age order (50, 40, 30, 20, 10)
        assertEquals(5, allUpdatedObjectIds.size(), "Should have updated all 5 documents");

        List<Integer> actualUpdateOrder = new ArrayList<>();
        for (ObjectId oid : allUpdatedObjectIds) {
            actualUpdateOrder.add(idToAge.get(oid));
        }

        List<Integer> expectedOrder = Arrays.asList(50, 40, 30, 20, 10);
        assertEquals(expectedOrder, actualUpdateOrder,
                "Documents should be updated in descending age order across all cursor advances");
    }

    @Test
    void shouldUpdateWithSortByAscending() {
        // Behavior: UPDATE with SORTBY ASC processes documents in ascending order by the
        // specified indexed field when updating all matching documents in a single batch.

        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 40}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        Map<ObjectId, Integer> idToAge = new LinkedHashMap<>();
        for (Map.Entry<ObjectId, byte[]> entry : insertedDocs.entrySet()) {
            BsonDocument doc = BSONUtil.toBsonDocument(entry.getValue());
            idToAge.put(entry.getKey(), BsonHelper.getInteger(doc, "age"));
        }

        // Execute UPDATE with SORTBY age ASC (no LIMIT - all in one batch)
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"age\": {\"$gte\": 0}}", "{\"$set\": {\"processed\": true}}",
                    BucketQueryArgs.Builder.sortBy("age", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(5, updatedObjectIds.size(), "Should have updated all 5 documents");

        // Verify order matches ascending age
        List<Integer> actualOrder = new ArrayList<>();
        for (ObjectId oid : updatedObjectIds) {
            actualOrder.add(idToAge.get(oid));
        }

        List<Integer> expectedOrder = Arrays.asList(10, 20, 30, 40, 50);
        assertEquals(expectedOrder, actualOrder, "Documents should be updated in ascending age order");
    }

    @Test
    void shouldUpdateWithSortByDescending() {
        // Behavior: UPDATE with SORTBY DESC processes documents in descending order by the
        // specified indexed field when updating all matching documents in a single batch.

        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 40}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        Map<ObjectId, Integer> idToAge = new LinkedHashMap<>();
        for (Map.Entry<ObjectId, byte[]> entry : insertedDocs.entrySet()) {
            BsonDocument doc = BSONUtil.toBsonDocument(entry.getValue());
            idToAge.put(entry.getKey(), BsonHelper.getInteger(doc, "age"));
        }

        // Execute UPDATE with SORTBY age DESC
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"age\": {\"$gte\": 0}}", "{\"$set\": {\"processed\": true}}",
                    BucketQueryArgs.Builder.sortBy("age", "DESC")).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(5, updatedObjectIds.size(), "Should have updated all 5 documents");

        List<Integer> actualOrder = new ArrayList<>();
        for (ObjectId oid : updatedObjectIds) {
            actualOrder.add(idToAge.get(oid));
        }

        List<Integer> expectedOrder = Arrays.asList(50, 40, 30, 20, 10);
        assertEquals(expectedOrder, actualOrder, "Documents should be updated in descending age order");
    }

    @Test
    void shouldUpdateWithSortByAndLimit() {
        // Behavior: UPDATE with SORTBY and LIMIT only updates the specified number of documents,
        // selecting them in sorted order.

        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 40}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Frank\", \"age\": 60}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        Map<ObjectId, Integer> idToAge = new LinkedHashMap<>();
        for (Map.Entry<ObjectId, byte[]> entry : insertedDocs.entrySet()) {
            BsonDocument doc = BSONUtil.toBsonDocument(entry.getValue());
            idToAge.put(entry.getKey(), BsonHelper.getInteger(doc, "age"));
        }

        // Execute UPDATE with SORTBY age ASC, LIMIT 2 - should update only the two youngest
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"age\": {\"$gte\": 0}}", "{\"$set\": {\"processed\": true}}",
                    BucketQueryArgs.Builder.limit(2).sortBy("age", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(2, updatedObjectIds.size(), "Should have updated exactly 2 documents");

        List<Integer> actualAges = new ArrayList<>();
        for (ObjectId oid : updatedObjectIds) {
            actualAges.add(idToAge.get(oid));
        }

        List<Integer> expectedAges = Arrays.asList(10, 20);
        assertEquals(expectedAges, actualAges, "Should update the two youngest documents (ages 10 and 20)");
    }

    @Test
    void shouldUpdateWithSortByStringField() {
        // Behavior: UPDATE with SORTBY on a STRING indexed field processes documents in
        // alphabetical order.

        SingleFieldIndexDefinition nameIndexDefinition = SingleFieldIndexDefinition.create("name-index", "name", BsonType.STRING, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(nameIndexDefinition);

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 40}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        Map<ObjectId, String> idToName = new LinkedHashMap<>();
        for (Map.Entry<ObjectId, byte[]> entry : insertedDocs.entrySet()) {
            BsonDocument doc = BSONUtil.toBsonDocument(entry.getValue());
            idToName.put(entry.getKey(), BsonHelper.getString(doc, "name"));
        }

        // Execute UPDATE with SORTBY name ASC
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"processed\": true}}",
                    BucketQueryArgs.Builder.sortBy("name", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(5, updatedObjectIds.size(), "Should have updated all 5 documents");

        List<String> actualOrder = new ArrayList<>();
        for (ObjectId oid : updatedObjectIds) {
            actualOrder.add(idToName.get(oid));
        }

        List<String> expectedOrder = Arrays.asList("Alice", "Bob", "Charlie", "Diana", "Eve");
        assertEquals(expectedOrder, actualOrder, "Documents should be updated in alphabetical name order");
    }

    @Test
    void shouldUpdateWithSortByReturnsEmptyWhenNoMatch() {
        // Behavior: UPDATE with SORTBY on a non-matching filter returns empty versionstamps array.

        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 20}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);
        insertDocumentsAndGetObjectIds(testDocuments);

        // Execute UPDATE with filter that matches nothing
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"age\": {\"$gt\": 100}}", "{\"$set\": {\"processed\": true}}",
                    BucketQueryArgs.Builder.sortBy("age", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            List<ObjectId> objectIds = extractObjectIds(msg);
            assertTrue(objectIds.isEmpty(), "Object IDs should be empty when no documents match");
        }
    }

    @Test
    void shouldUpdateSingleDocumentWithSortBy() {
        // Behavior: UPDATE with SORTBY works correctly when only one document matches the filter.

        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 20}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        Map<ObjectId, Integer> idToAge = new LinkedHashMap<>();
        for (Map.Entry<ObjectId, byte[]> entry : insertedDocs.entrySet()) {
            BsonDocument doc = BSONUtil.toBsonDocument(entry.getValue());
            idToAge.put(entry.getKey(), BsonHelper.getInteger(doc, "age"));
        }

        // Execute UPDATE with filter matching only Bob (age > 40)
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"age\": {\"$gt\": 40}}", "{\"$set\": {\"processed\": true}}",
                    BucketQueryArgs.Builder.sortBy("age", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated exactly 1 document");
        assertEquals(50, idToAge.get(updatedObjectIds.get(0)).intValue(),
                "Updated document should be Bob (age 50)");
    }

    @Test
    void shouldRejectUpdateWithSortByOnNonIndexedField() {
        // Behavior: UPDATE with SORTBY on a non-indexed field is rejected because SORTBY
        // requires an index that provides natural ordering.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);
        insertDocumentsAndGetObjectIds(Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 20}")
        ));

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"processed\": true}}",
                BucketQueryArgs.Builder.sortBy("age", "ASC")).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertEquals(
                "ERR query cannot be executed: SORTBY 'age' requires an index that provides natural ordering. Hint: create an index on 'age'",
                errorMessage.content()
        );
    }

    @Test
    void shouldUpdateWithSortByOnFilterFieldAndResidualPredicate() {
        // Behavior: UPDATE filters on age with an EQ predicate and a range predicate on score,
        // then sorts by score. The planner picks the score index for ordering and applies the
        // age filter as a residual predicate.
        //
        // The score predicate ({$gte: 0}) is a no-op filter that exists solely to bring the
        // score index into the physical plan. Without it, the plan would be a single
        // PhysicalIndexScan on age — which provides no ordering on score, so SORTBY score
        // would be rejected. With the AND, the planner sees both indexes and can select
        // score as the primary scan for sort ordering.

        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition scoreIndexDefinition = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);
        createIndexThenWaitForReadiness(scoreIndexDefinition);

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25, \"score\": 90}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30, \"score\": 70}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 25, \"score\": 80}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 35, \"score\": 60}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 25, \"score\": 100}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        Map<ObjectId, Integer> idToAge = new LinkedHashMap<>();
        for (Map.Entry<ObjectId, byte[]> entry : insertedDocs.entrySet()) {
            BsonDocument doc = BSONUtil.toBsonDocument(entry.getValue());
            idToAge.put(entry.getKey(), BsonHelper.getInteger(doc, "age"));
        }

        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"age\": {\"$eq\": 25}, \"score\": {\"$gte\": 0}}", "{\"$set\": {\"processed\": true}}",
                    BucketQueryArgs.Builder.sortBy("score", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(3, updatedObjectIds.size(), "Should have updated 3 documents with age=25");
        for (ObjectId oid : updatedObjectIds) {
            assertEquals(25, idToAge.get(oid).intValue(),
                    "All updated documents should have age=25");
        }
    }

    @Test
    void shouldUpdateWithSortByNullValues() {
        // Behavior: UPDATE with SORTBY handles documents where the sort field is missing or null.
        // Documents with missing/null sort field values are included in results.

        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        // Insert documents: some have age, some don't
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}"),  // missing age
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": null}"),  // null age
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 10}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        Map<ObjectId, String> idToName = new LinkedHashMap<>();
        for (Map.Entry<ObjectId, byte[]> entry : insertedDocs.entrySet()) {
            BsonDocument doc = BSONUtil.toBsonDocument(entry.getValue());
            idToName.put(entry.getKey(), BsonHelper.getString(doc, "name"));
        }

        // Execute UPDATE with SORTBY age ASC
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"processed\": true}}",
                    BucketQueryArgs.Builder.sortBy("age", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            updatedObjectIds = extractObjectIds(msg);
        }

        // Should have updated all 5 documents (including those with missing/null age)
        assertEquals(5, updatedObjectIds.size(), "Should have updated all 5 documents");

        // Collect names of updated documents
        List<String> updatedNames = new ArrayList<>();
        for (ObjectId oid : updatedObjectIds) {
            updatedNames.add(idToName.get(oid));
        }

        // Verify all documents were updated
        assertTrue(updatedNames.contains("Alice"), "Alice should be updated");
        assertTrue(updatedNames.contains("Bob"), "Bob (missing age) should be updated");
        assertTrue(updatedNames.contains("Charlie"), "Charlie should be updated");
        assertTrue(updatedNames.contains("Diana"), "Diana (null age) should be updated");
        assertTrue(updatedNames.contains("Eve"), "Eve should be updated");
    }

    @Test
    void shouldUpdateWithSortByNestedField() {
        // Behavior: UPDATE with SORTBY on a nested field (e.g., "address.city") sorts documents
        // by the nested field value when that field is indexed.

        SingleFieldIndexDefinition cityIndexDefinition = SingleFieldIndexDefinition.create("city-index", "address.city", BsonType.STRING, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(cityIndexDefinition);

        // Insert documents with nested address.city field
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"address\": {\"city\": \"Chicago\"}}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"address\": {\"city\": \"Austin\"}}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"address\": {\"city\": \"Denver\"}}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"address\": {\"city\": \"Boston\"}}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"address\": {\"city\": \"Atlanta\"}}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        Map<ObjectId, String> idToCity = new LinkedHashMap<>();
        for (Map.Entry<ObjectId, byte[]> entry : insertedDocs.entrySet()) {
            BsonDocument doc = BSONUtil.toBsonDocument(entry.getValue());
            BsonDocument address = doc.getDocument("address");
            idToCity.put(entry.getKey(), address.getString("city").getValue());
        }

        // Execute UPDATE with SORTBY address.city ASC
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"processed\": true}}",
                    BucketQueryArgs.Builder.sortBy("address.city", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(5, updatedObjectIds.size(), "Should have updated all 5 documents");

        // Verify sort order: Atlanta, Austin, Boston, Chicago, Denver (alphabetical)
        List<String> actualCityOrder = new ArrayList<>();
        for (ObjectId oid : updatedObjectIds) {
            actualCityOrder.add(idToCity.get(oid));
        }

        List<String> expectedCityOrder = Arrays.asList("Atlanta", "Austin", "Boston", "Chicago", "Denver");
        assertEquals(expectedCityOrder, actualCityOrder,
                "Documents should be sorted by address.city in ascending alphabetical order");
    }

    @Test
    void shouldUpdateWithSortByWithoutLimit() {
        // Behavior: UPDATE with SORTBY but without LIMIT returns all matching documents
        // in sorted order within a single response.

        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        // Insert documents with shuffled ages
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 40}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 20}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        Map<ObjectId, Integer> idToAge = new LinkedHashMap<>();
        for (Map.Entry<ObjectId, byte[]> entry : insertedDocs.entrySet()) {
            BsonDocument doc = BSONUtil.toBsonDocument(entry.getValue());
            idToAge.put(entry.getKey(), BsonHelper.getInteger(doc, "age"));
        }

        // Execute UPDATE with SORTBY only (no LIMIT)
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"processed\": true}}",
                    BucketQueryArgs.Builder.sortBy("age", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(5, updatedObjectIds.size(), "Should have updated all 5 documents");

        // Verify ascending sort order: 10, 20, 30, 40, 50
        List<Integer> actualAgeOrder = new ArrayList<>();
        for (ObjectId oid : updatedObjectIds) {
            actualAgeOrder.add(idToAge.get(oid));
        }

        List<Integer> expectedAgeOrder = Arrays.asList(10, 20, 30, 40, 50);
        assertEquals(expectedAgeOrder, actualAgeOrder,
                "Documents should be sorted by age in ascending order");
    }

    @Test
    void shouldUpdateWithSortByOnlyMultipleAdvance() {
        // Behavior: UPDATE with SORTBY only (no explicit LIMIT) can still use BUCKET.ADVANCE
        // to iterate through results while maintaining sort order across advances.

        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        // Insert documents with shuffled ages
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 40}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Frank\", \"age\": 60}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        Map<ObjectId, Integer> idToAge = new LinkedHashMap<>();
        for (Map.Entry<ObjectId, byte[]> entry : insertedDocs.entrySet()) {
            BsonDocument doc = BSONUtil.toBsonDocument(entry.getValue());
            idToAge.put(entry.getKey(), BsonHelper.getInteger(doc, "age"));
        }

        // Execute UPDATE with SORTBY age ASC, LIMIT 2 (to force multiple advances)
        List<ObjectId> allUpdatedObjectIds = new ArrayList<>();
        int cursorId;

        // Initial update
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"processed\": true}}",
                    BucketQueryArgs.Builder.limit(2).sortBy("age", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            allUpdatedObjectIds.addAll(extractObjectIds(msg));
        }

        assertEquals(2, allUpdatedObjectIds.size(), "First batch should update 2 documents");

        // Continue with BUCKET.ADVANCE until exhausted
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;

        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceUpdate(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<ObjectId> advancedIds = extractObjectIds(msg);
            if (advancedIds.isEmpty()) {
                break;
            }

            allUpdatedObjectIds.addAll(advancedIds);

            advanceCalls++;
        }

        // Should have updated all 6 documents
        assertEquals(6, allUpdatedObjectIds.size(), "Should have updated all 6 documents");

        // Verify global sort order across all advances: 10, 20, 30, 40, 50, 60
        List<Integer> actualAgeOrder = new ArrayList<>();
        for (ObjectId oid : allUpdatedObjectIds) {
            actualAgeOrder.add(idToAge.get(oid));
        }

        List<Integer> expectedAgeOrder = Arrays.asList(10, 20, 30, 40, 50, 60);
        assertEquals(expectedAgeOrder, actualAgeOrder,
                "Documents should be sorted by age in ascending order across all cursor advances");
    }

    @Test
    void shouldRejectUpdateWithSortByFullScanAscending() {
        // Behavior: UPDATE with SORTBY ASC on a non-indexed field is rejected because SORTBY
        // requires an index that provides natural ordering.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);
        insertDocumentsAndGetObjectIds(Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 20}")
        ));

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"processed\": true}}",
                BucketQueryArgs.Builder.limit(2).sortBy("age", "ASC")).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertEquals(
                "ERR query cannot be executed: SORTBY 'age' requires an index that provides natural ordering. Hint: create an index on 'age'",
                errorMessage.content()
        );
    }

    @Test
    void shouldRejectUpdateWithSortByFullScanDescending() {
        // Behavior: UPDATE with SORTBY DESC on a non-indexed field is rejected because SORTBY
        // requires an index that provides natural ordering.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);
        insertDocumentsAndGetObjectIds(Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 20}")
        ));

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"processed\": true}}",
                BucketQueryArgs.Builder.limit(2).sortBy("age", "DESC")).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertEquals(
                "ERR query cannot be executed: SORTBY 'age' requires an index that provides natural ordering. Hint: create an index on 'age'",
                errorMessage.content()
        );
    }

    @Test
    void shouldThrowIndexTypeMismatchExceptionWhenUpdatingWithMixedTypeArray() {
        // Create an index expecting INT32 for 'values' field
        SingleFieldIndexDefinition valuesIndexDefinition = SingleFieldIndexDefinition.create("values-index", "values", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(valuesIndexDefinition);

        // Insert a document with the correct type
        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf insertBuf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Test\", \"values\": [1, 2, 3]}");
        insertCmd.insert(TEST_BUCKET, documentBytes).encode(insertBuf);
        Object insertMsg = runCommand(channel, insertBuf);
        assertInstanceOf(ArrayRedisMessage.class, insertMsg);

        // Update the document with mixed-type array [1, 2, 3, "five"]
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        BsonDocument setDoc = new BsonDocument()
                .append("values", new BsonArray(Arrays.asList(
                        new BsonInt32(1),
                        new BsonInt32(2),
                        new BsonInt32(3),
                        new BsonString("five")
                )));
        BsonDocument updateDoc = new BsonDocument("$set", setDoc);
        byte[] update = BSONUtil.toBytes(updateDoc);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{}", update).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().startsWith("INDEXTYPE_MISMATCH"),
                "Should return INDEXTYPE_MISMATCH error for mixed-type array with INT32 index");
        assertTrue(errorMessage.content().contains("index 'values-index' expects 'INT32'"),
                "Error message should mention the index name and expected type");
        assertTrue(errorMessage.content().contains("selector 'values' matched a value of type 'STRING'"),
                "Error message should mention the selector and actual type");
    }

    @Test
    void shouldUpdateWithOrQueryLimitAndAdvance() {
        // Behavior: UPDATE with $or on two indexed fields and limit=1 triggers child rewind in
        // UnionNode. Each ADVANCE UPDATE batch updates at most 1 document. All matching documents
        // are updated exactly once across multiple ADVANCE calls.

        SingleFieldIndexDefinition deptIndex = SingleFieldIndexDefinition.create("dept-idx", "department", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition salaryIndex = SingleFieldIndexDefinition.create("salary-idx", "salary", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(deptIndex);
        createIndexThenWaitForReadiness(salaryIndex);

        // 8 docs: 2 match both, 2 match dept only, 2 match salary only, 2 match neither
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"E1\", \"department\": \"engineering\", \"salary\": 90000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"E2\", \"department\": \"engineering\", \"salary\": 95000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"D1\", \"department\": \"engineering\", \"salary\": 60000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"D2\", \"department\": \"engineering\", \"salary\": 70000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"S1\", \"department\": \"sales\", \"salary\": 85000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"S2\", \"department\": \"sales\", \"salary\": 92000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"N1\", \"department\": \"sales\", \"salary\": 50000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"N2\", \"department\": \"sales\", \"salary\": 55000}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(8, insertedDocs.size());

        // UPDATE with $or filter, limit=1
        List<ObjectId> allUpdatedObjectIds = new ArrayList<>();
        int cursorId;

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET,
                    "{\"$or\": [{\"department\": {\"$eq\": \"engineering\"}}, {\"salary\": {\"$gt\": 80000}}]}",
                    "{\"$set\": {\"reviewed\": true}}",
                    BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);
            allUpdatedObjectIds.addAll(extractObjectIds(msg));
        }

        assertTrue(allUpdatedObjectIds.size() <= 1, "First batch should update at most 1 document");

        // ADVANCE UPDATE until exhausted
        int maxAdvanceCalls = 15;
        int advanceCalls = 0;

        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceUpdate(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<ObjectId> advancedIds = extractObjectIds(msg);
            if (advancedIds.isEmpty()) {
                break;
            }

            assertTrue(advancedIds.size() <= 1, "Each batch should update at most 1 document");
            allUpdatedObjectIds.addAll(advancedIds);
            advanceCalls++;
        }

        // 6 matching: E1, E2 (both), D1, D2 (dept only), S1, S2 (salary only)
        assertEquals(6, allUpdatedObjectIds.size(), "Should have updated 6 matching documents");

        // Verify no duplicate ObjectIds
        Set<ObjectId> uniqueUpdatedIds = new HashSet<>(allUpdatedObjectIds);
        assertEquals(allUpdatedObjectIds.size(), uniqueUpdatedIds.size(), "No duplicate updates");

        // Verify post-update: 6 docs have reviewed=true, 2 don't
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(8, entries.size(), "All 8 documents should exist");

            int reviewedCount = 0;
            Set<String> unmodifiedNames = new HashSet<>();
            for (BsonDocument doc : entries) {
                if (doc.containsKey("reviewed") && doc.getBoolean("reviewed").getValue()) {
                    reviewedCount++;
                } else {
                    unmodifiedNames.add(BsonHelper.getString(doc, "name"));
                }
            }
            assertEquals(6, reviewedCount, "6 documents should have reviewed=true");
            assertEquals(Set.of("N1", "N2"), unmodifiedNames, "Only N1 and N2 should be unmodified");
        }
    }

    // --- Numeric Widening Integration Tests ---

    @Test
    void shouldUpdateWithOrQueryLimitTwoAndAdvance() {
        // Behavior: UPDATE with $or on two indexed fields and limit=2 exercises the keptHandles/excessHandles
        // split in UnionNode.writeResultsAndRewindChildren where a single child contributes both kept and
        // excess entries in the same batch. All matching documents are updated exactly once across multiple
        // ADVANCE calls.

        SingleFieldIndexDefinition deptIndex = SingleFieldIndexDefinition.create("dept-idx", "department", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition salaryIndex = SingleFieldIndexDefinition.create("salary-idx", "salary", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(deptIndex);
        createIndexThenWaitForReadiness(salaryIndex);

        // 10 docs: 3 match both, 2 match dept only, 3 match salary only, 2 match neither
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"E1\", \"department\": \"engineering\", \"salary\": 90000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"E2\", \"department\": \"engineering\", \"salary\": 95000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"E3\", \"department\": \"engineering\", \"salary\": 85000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"D1\", \"department\": \"engineering\", \"salary\": 60000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"D2\", \"department\": \"engineering\", \"salary\": 70000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"S1\", \"department\": \"sales\", \"salary\": 85000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"S2\", \"department\": \"sales\", \"salary\": 92000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"S3\", \"department\": \"sales\", \"salary\": 88000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"N1\", \"department\": \"sales\", \"salary\": 50000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"N2\", \"department\": \"sales\", \"salary\": 55000}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(10, insertedDocs.size());

        // UPDATE with $or filter, limit=2
        List<ObjectId> allUpdatedObjectIds = new ArrayList<>();
        int cursorId;

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET,
                    "{\"$or\": [{\"department\": {\"$eq\": \"engineering\"}}, {\"salary\": {\"$gt\": 80000}}]}",
                    "{\"$set\": {\"reviewed\": true}}",
                    BucketQueryArgs.Builder.limit(2)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);
            allUpdatedObjectIds.addAll(extractObjectIds(msg));
        }

        assertTrue(allUpdatedObjectIds.size() <= 2, "First batch should update at most 2 documents");

        // ADVANCE UPDATE until exhausted
        int maxAdvanceCalls = 15;
        int advanceCalls = 0;

        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceUpdate(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<ObjectId> advancedIds = extractObjectIds(msg);
            if (advancedIds.isEmpty()) {
                break;
            }

            assertTrue(advancedIds.size() <= 2, "Each batch should update at most 2 documents");
            allUpdatedObjectIds.addAll(advancedIds);
            advanceCalls++;
        }

        // 8 matching: E1, E2, E3 (both), D1, D2 (dept only), S1, S2, S3 (salary only)
        assertEquals(8, allUpdatedObjectIds.size(), "Should have updated 8 matching documents");

        // Verify no duplicate ObjectIds
        Set<ObjectId> uniqueUpdatedIds = new HashSet<>(allUpdatedObjectIds);
        assertEquals(allUpdatedObjectIds.size(), uniqueUpdatedIds.size(), "No duplicate updates");

        // Verify post-update: 8 docs have reviewed=true, 2 don't
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(10, entries.size(), "All 10 documents should exist");

            int reviewedCount = 0;
            Set<String> unmodifiedNames = new HashSet<>();
            for (BsonDocument doc : entries) {
                if (doc.containsKey("reviewed") && doc.getBoolean("reviewed").getValue()) {
                    reviewedCount++;
                } else {
                    unmodifiedNames.add(BsonHelper.getString(doc, "name"));
                }
            }
            assertEquals(8, reviewedCount, "8 documents should have reviewed=true");
            assertEquals(Set.of("N1", "N2"), unmodifiedNames, "Only N1 and N2 should be unmodified");
        }
    }

    @Test
    void shouldUpdateAllDocumentsMatchingBothOrBranches() {
        // Behavior: UPDATE with $or where ALL documents match BOTH branches exercises the worst-case
        // ObjectId dedup path. Every iteration produces the full union set from both children, and
        // returnedObjectIds must filter all previously-updated documents before selecting the next batch.

        SingleFieldIndexDefinition deptIndex = SingleFieldIndexDefinition.create("dept-idx", "department", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition salaryIndex = SingleFieldIndexDefinition.create("salary-idx", "salary", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(deptIndex);
        createIndexThenWaitForReadiness(salaryIndex);

        // 6 docs: ALL match both branches (department=engineering AND salary>80000)
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"A1\", \"department\": \"engineering\", \"salary\": 85000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"A2\", \"department\": \"engineering\", \"salary\": 90000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"A3\", \"department\": \"engineering\", \"salary\": 95000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"A4\", \"department\": \"engineering\", \"salary\": 100000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"A5\", \"department\": \"engineering\", \"salary\": 105000}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"A6\", \"department\": \"engineering\", \"salary\": 110000}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(6, insertedDocs.size());

        // UPDATE with $or filter, limit=2
        List<ObjectId> allUpdatedObjectIds = new ArrayList<>();
        int cursorId;

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET,
                    "{\"$or\": [{\"department\": {\"$eq\": \"engineering\"}}, {\"salary\": {\"$gt\": 80000}}]}",
                    "{\"$set\": {\"reviewed\": true}}",
                    BucketQueryArgs.Builder.limit(2)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);
            allUpdatedObjectIds.addAll(extractObjectIds(msg));
        }

        assertTrue(allUpdatedObjectIds.size() <= 2, "First batch should update at most 2 documents");

        // ADVANCE UPDATE until exhausted
        int maxAdvanceCalls = 15;
        int advanceCalls = 0;

        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceUpdate(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<ObjectId> advancedIds = extractObjectIds(msg);
            if (advancedIds.isEmpty()) {
                break;
            }

            assertTrue(advancedIds.size() <= 2, "Each batch should update at most 2 documents");
            allUpdatedObjectIds.addAll(advancedIds);
            advanceCalls++;
        }

        // All 6 documents match both branches
        assertEquals(6, allUpdatedObjectIds.size(), "Should have updated 6 matching documents");

        // Verify no duplicate ObjectIds
        Set<ObjectId> uniqueUpdatedIds = new HashSet<>(allUpdatedObjectIds);
        assertEquals(allUpdatedObjectIds.size(), uniqueUpdatedIds.size(), "No duplicate updates");

        // Verify post-update: all 6 docs have reviewed=true
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(6, entries.size(), "All 6 documents should exist");

            int reviewedCount = 0;
            for (BsonDocument doc : entries) {
                if (doc.containsKey("reviewed") && doc.getBoolean("reviewed").getValue()) {
                    reviewedCount++;
                }
            }
            assertEquals(6, reviewedCount, "All 6 documents should have reviewed=true");
        }
    }

    @Test
    void shouldWidenInt32SetValueToInt64IndexDuringUpdate() {
        // Behavior: When $set writes an INT32 value to a field backed by an INT64 index,
        // the value is widened to INT64 in the index and the update succeeds.
        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create(
                "age-index", "age", BsonType.INT64, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        // Insert a document with INT64 age so the initial insert matches the index type
        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf insertBuf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": {\"$numberLong\": \"25\"}}");
        insertCmd.insert(TEST_BUCKET, documentBytes).encode(insertBuf);
        Object insertMsg = runCommand(channel, insertBuf);
        assertInstanceOf(ArrayRedisMessage.class, insertMsg);

        // Update: $set age to INT32 value 42 — should be widened to INT64 in the index
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"age\": 42}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query to verify the update was applied
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size());
            assertEquals(42, BsonHelper.getInteger(entries.getFirst(), "age"),
                    "Age should be updated to 42");
        }
    }

    @Test
    void shouldWidenInt32SetValueToDoubleIndexDuringUpdate() {
        // Behavior: When $set writes an INT32 value to a field backed by a DOUBLE index,
        // the value is widened to DOUBLE in the index and the update succeeds.
        SingleFieldIndexDefinition scoreIndexDefinition = SingleFieldIndexDefinition.create(
                "score-index", "score", BsonType.DOUBLE, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(scoreIndexDefinition);

        // Insert a document with DOUBLE score
        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf insertBuf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"score\": 5.5}");
        insertCmd.insert(TEST_BUCKET, documentBytes).encode(insertBuf);
        Object insertMsg = runCommand(channel, insertBuf);
        assertInstanceOf(ArrayRedisMessage.class, insertMsg);

        // Update: $set score to INT32 value 100 — should be widened to DOUBLE in the index
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"score\": 100}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query to verify the update was applied
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size());
            assertEquals(100, BsonHelper.getInteger(entries.getFirst(), "score"),
                    "Score should be updated to 100");
        }
    }

    @Test
    void shouldRejectInt64ToDoubleWideningDuringUpdate() {
        // Behavior: INT64 to DOUBLE widening is forbidden (lossy: 64-bit integers exceed
        // double's 53-bit mantissa). $set with INT64 value on DOUBLE index throws INDEXTYPE_MISMATCH.
        SingleFieldIndexDefinition valueIndexDefinition = SingleFieldIndexDefinition.create(
                "value-index", "value", BsonType.DOUBLE, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(valueIndexDefinition);

        // Insert a document with DOUBLE value
        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf insertBuf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Test\", \"value\": 5.5}");
        insertCmd.insert(TEST_BUCKET, documentBytes).encode(insertBuf);
        Object insertMsg = runCommand(channel, insertBuf);
        assertInstanceOf(ArrayRedisMessage.class, insertMsg);

        // Update: $set value to INT64 — should fail because INT64→DOUBLE is lossy
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"value\": {\"$numberLong\": \"999999999999\"}}}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().startsWith("INDEXTYPE_MISMATCH"),
                "Should return INDEXTYPE_MISMATCH error for INT64 value with DOUBLE index");
    }

    @Test
    void shouldMatchInt32DocumentFieldWithInt64PredicateDuringUpdate() {
        // Behavior: PredicateEvaluator cross-type widening matches INT32 document field
        // with INT64 predicate during full-scan UPDATE (no index).
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 42}")
        );

        insertDocumentsAndGetObjectIds(testDocuments);

        // Update with INT64 predicate matching INT32 field
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET,
                    "{\"age\": {\"$eq\": {\"$numberLong\": \"42\"}}}",
                    "{\"$set\": {\"found\": true}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query to verify only age=42 document has found=true
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(3, entries.size());

            for (BsonDocument doc : entries) {
                Integer age = BsonHelper.getInteger(doc, "age");
                assert age != null;
                Boolean found = BsonHelper.getBoolean(doc, "found");
                if (age == 42) {
                    assert found != null;
                    assertTrue(found, "Document with age 42 should have found=true");
                } else {
                    assertNull(found, "Document with age " + age + " should not have found field");
                }
            }
        }
    }

    @Test
    void shouldUpdateViaInt64IndexWithInt32Predicate() {
        // Behavior: INT32 values widened to INT64 at insert-time are found by INT32
        // predicate via index scan during UPDATE (IndexPredicateResolver widening).
        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create(
                "age-index", "age", BsonType.INT64, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        // Insert INT32 documents — widened to INT64 at store time
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 40}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 50}")
        );

        insertDocumentsAndGetObjectIds(testDocuments);

        // Update with INT32 predicate on INT64 index
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET,
                    "{\"age\": {\"$gt\": 30}}",
                    "{\"$set\": {\"status\": \"senior\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(2, updatedObjectIds.size(), "Should have updated 2 documents with age > 30");

        // Query to verify correct documents were updated
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(4, entries.size());

            for (BsonDocument doc : entries) {
                Integer age = BsonHelper.getInteger(doc, "age");
                assert age != null;
                String status = BsonHelper.getString(doc, "status");
                if (age > 30) {
                    assertEquals("senior", status,
                            "Document with age " + age + " should have status 'senior'");
                } else {
                    assertNull(status,
                            "Document with age " + age + " should not have status field");
                }
            }
        }
    }

    @Test
    void shouldUpdateViaDoubleIndexWithInt32RangePredicate() {
        // Behavior: DOUBLE values in a DOUBLE index are found by INT32 range predicate
        // via index scan during UPDATE (IndexPredicateResolver widening).
        SingleFieldIndexDefinition scoreIndexDefinition = SingleFieldIndexDefinition.create(
                "score-index", "score", BsonType.DOUBLE, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(scoreIndexDefinition);

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"score\": 5.5}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"score\": 10.0}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"score\": 15.7}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"score\": 22.3}")
        );

        insertDocumentsAndGetObjectIds(testDocuments);

        // Update with INT32 predicate on DOUBLE index
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET,
                    "{\"score\": {\"$gte\": 15}}",
                    "{\"$set\": {\"passed\": true}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(2, updatedObjectIds.size(), "Should have updated 2 documents with score >= 15");

        // Query to verify correct documents were updated
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(4, entries.size());

            for (BsonDocument doc : entries) {
                Double score = BsonHelper.getDouble(doc, "score");
                assert score != null;
                Boolean passed = BsonHelper.getBoolean(doc, "passed");
                if (score >= 15.0) {
                    assert passed != null;
                    assertTrue(passed,
                            "Document with score " + score + " should have passed=true");
                } else {
                    assertNull(passed,
                            "Document with score " + score + " should not have passed field");
                }
            }
        }
    }

    @Test
    void shouldWidenInt32UpsertValueInInt64Index() {
        // Behavior: Upsert that creates a new document with INT32 value widens it
        // in the INT64 index (exercises setSecondaryIndexesForUpsert).
        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create(
                "age-index", "age", BsonType.INT64, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        // Upsert a new document with INT32 age
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        List<ObjectId> upsertedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET,
                    "{\"name\": \"NewUser\"}",
                    "{\"$set\": {\"age\": 30}, \"upsert\": true}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            upsertedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, upsertedObjectIds.size(), "Should have upserted 1 document");

        // Query via INT32 predicate on INT64 index to verify widening worked
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"age\": {\"$eq\": 30}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should find upserted document via INT64 index with INT32 predicate");
            assertEquals(30, BsonHelper.getInteger(entries.getFirst(), "age"));
        }
    }

    // --- Vector Index Tests ---

    @Test
    void shouldUpdateAndReindexWithWidenedValuesEndToEnd() {
        // Behavior: Full pipeline — INT32 docs are widened to INT64 at insert, found by
        // INT32 predicate via index scan, and $set new INT32 value is widened back to INT64
        // in the index.
        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create(
                "age-index", "age", BsonType.INT64, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        // Insert INT32 documents — widened to INT64 at store time
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 40}")
        );

        insertDocumentsAndGetObjectIds(testDocuments);

        // Update: match age=30 via index scan with widening, $set age=35 (INT32 widened to INT64)
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET,
                    "{\"age\": {\"$eq\": 30}}",
                    "{\"$set\": {\"age\": 35}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Verify new value is findable via widened index
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"age\": {\"$eq\": 35}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should find document with updated age=35 via widened index");
            assertEquals("Bob", BsonHelper.getString(entries.getFirst(), "name"));
        }

        // Verify old value is gone
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"age\": {\"$eq\": 30}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(0, entries.size(), "Should find 0 documents with old age=30");
        }
    }

    @Test
    void shouldUpdateWithSortByOnWidenedInt64Index() {
        // Behavior: SORTBY on an INT64 index works correctly when documents were inserted
        // with INT32 values (widened at store time). LIMIT selects the lowest-aged documents.
        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create(
                "age-index", "age", BsonType.INT64, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        // Insert INT32 documents with shuffled ages — widened to INT64 at store time
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 40}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 30}")
        );

        insertDocumentsAndGetObjectIds(testDocuments);

        // Update with INT64 predicate (matches index type) + SORTBY age ASC, LIMIT 2
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET,
                    "{\"age\": {\"$gte\": {\"$numberLong\": \"0\"}}}",
                    "{\"$set\": {\"processed\": true}}",
                    BucketQueryArgs.Builder.limit(2).sortBy("age", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(2, updatedObjectIds.size(), "Should have updated 2 documents");

        // Query to verify only age 10 and 20 have processed=true
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(5, entries.size());

            for (BsonDocument doc : entries) {
                Integer age = BsonHelper.getInteger(doc, "age");
                assert age != null;
                Boolean processed = BsonHelper.getBoolean(doc, "processed");
                if (age <= 20) {
                    assert processed != null;
                    assertTrue(processed,
                            "Document with age " + age + " should have processed=true");
                } else {
                    assertNull(processed,
                            "Document with age " + age + " should not have processed field");
                }
            }
        }
    }

    private BucketMetadata createBucketWithVectorIndex() {
        createBucket(VECTOR_BUCKET, List.of(TEST_SHARD_ID), VECTOR_INDEX_JSON);
        return getBucketMetadata(VECTOR_BUCKET);
    }

    private void insertIntoVectorBucket(byte[]... docs) {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(VECTOR_BUCKET, docs).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
    }

    private List<KeyValue> fetchAllIndexedEntries(DirectorySubspace indexSubspace) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return tr.getRange(begin, end).asList().join();
        }
    }

    @Test
    void shouldPreserveVectorIndexEntryWhenUpdatingNonVectorField() {
        // Behavior: Updating a non-vector field preserves the vector index entry metadata and vector data.
        BucketMetadata metadata = createBucketWithVectorIndex();

        insertIntoVectorBucket(
                BSONUtil.jsonToDocumentThenBytes("{\"age\": 25, \"embedding\": [0.1, 0.2, 0.3]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"age\": 35, \"embedding\": [0.4, 0.5, 0.6]}")
        );

        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
        assertEquals(2, fetchAllIndexedEntries(vectorIndex.subspace()).size(), "Should have 2 vector index entries before update");

        // Update age via $set on all docs
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(VECTOR_BUCKET, "{}", "{\"$set\": {\"age\": 99}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<ObjectId> updatedIds = extractObjectIds(msg);
        assertEquals(2, updatedIds.size(), "Should have updated 2 documents");

        // Verify vector index entries still exist with the count unchanged
        List<KeyValue> entries = fetchAllIndexedEntries(vectorIndex.subspace());
        assertEquals(2, entries.size(), "Should still have 2 vector index entries after non-vector update");

        // Verify vector data is preserved
        for (KeyValue entry : entries) {
            VectorIndexValue decoded = VectorIndexValue.decode(entry.getValue());
            assertEquals(3, decoded.vector().length, "Vector should have 3 dimensions");
        }
    }

    @Test
    void shouldUpdateVectorIndexEntryWhenVectorFieldIsSet() {
        // Behavior: $set on the vector field drops the old entry and inserts a new one with updated vector data.
        BucketMetadata metadata = createBucketWithVectorIndex();

        insertIntoVectorBucket(
                BSONUtil.jsonToDocumentThenBytes("{\"label\": \"a\", \"embedding\": [0.1, 0.2, 0.3]}")
        );

        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
        assertEquals(1, fetchAllIndexedEntries(vectorIndex.subspace()).size(), "Should have 1 vector index entry before update");

        // $set the embedding to new values
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(VECTOR_BUCKET, "{}", "{\"$set\": {\"embedding\": [0.7, 0.8, 0.9]}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<ObjectId> updatedIds = extractObjectIds(msg);
        assertEquals(1, updatedIds.size(), "Should have updated 1 document");

        // Verify 1 vector index entry exists
        List<KeyValue> entries = fetchAllIndexedEntries(vectorIndex.subspace());
        assertEquals(1, entries.size(), "Should have 1 vector index entry after update");

        // Decode and verify new vector values
        VectorIndexValue decoded = VectorIndexValue.decode(entries.getFirst().getValue());
        assertEquals(3, decoded.vector().length);
        assertEquals(0.7f, decoded.vector()[0], 0.001f);
        assertEquals(0.8f, decoded.vector()[1], 0.001f);
        assertEquals(0.9f, decoded.vector()[2], 0.001f);
    }

    @Test
    void shouldRemoveVectorIndexEntryWhenVectorFieldIsUnset() {
        // Behavior: $unset on the vector field removes the vector index entry and decrements cardinality.
        BucketMetadata metadata = createBucketWithVectorIndex();

        insertIntoVectorBucket(
                BSONUtil.jsonToDocumentThenBytes("{\"label\": \"a\", \"embedding\": [0.1, 0.2, 0.3]}")
        );

        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
        assertEquals(1, fetchAllIndexedEntries(vectorIndex.subspace()).size(), "Should have 1 vector index entry before unset");

        // $unset the embedding field
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(VECTOR_BUCKET, "{}", "{\"$unset\": {\"embedding\": \"\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<ObjectId> updatedIds = extractObjectIds(msg);
        assertEquals(1, updatedIds.size(), "Should have updated 1 document");

        // Verify 0 vector index entries remain
        List<KeyValue> entries = fetchAllIndexedEntries(vectorIndex.subspace());
        assertEquals(0, entries.size(), "Should have 0 vector index entries after unset");

        // Verify cardinality = 0
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics stats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), vectorIndex.definition().id());
            assertEquals(0L, stats.cardinality(), "Cardinality should be 0 after unset");
        }
    }

    @Test
    void shouldUpdateVectorIndexCardinalityAfterUpdate() {
        // Behavior: $unset on the vector field for a subset of docs decrements cardinality accordingly.
        BucketMetadata metadata = createBucketWithVectorIndex();

        insertIntoVectorBucket(
                BSONUtil.jsonToDocumentThenBytes("{\"age\": 25, \"embedding\": [0.1, 0.2, 0.3]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"age\": 35, \"embedding\": [0.4, 0.5, 0.6]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"age\": 45, \"embedding\": [0.7, 0.8, 0.9]}")
        );

        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);

        // Verify cardinality = 3
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics stats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), vectorIndex.definition().id());
            assertEquals(3L, stats.cardinality(), "Cardinality should be 3 before update");
        }

        // $unset embedding on docs with age > 30
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(VECTOR_BUCKET, "{\"age\": {\"$gt\": 30}}", "{\"$unset\": {\"embedding\": \"\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<ObjectId> updatedIds = extractObjectIds(msg);
        assertEquals(2, updatedIds.size(), "Should have updated 2 documents with age > 30");

        // Verify cardinality = 1
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics stats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), vectorIndex.definition().id());
            assertEquals(1L, stats.cardinality(), "Cardinality should be 1 after unsetting 2 docs");
        }
    }

    @Test
    void shouldNotAffectVectorIndexWhenUpdatedDocHasNoVectorField() {
        // Behavior: Updating a non-vector field on a doc without a vector field leaves the vector index intact.
        BucketMetadata metadata = createBucketWithVectorIndex();

        insertIntoVectorBucket(
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"with-vector\", \"embedding\": [0.1, 0.2, 0.3]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"no-vector\", \"age\": 25}")
        );

        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
        assertEquals(1, fetchAllIndexedEntries(vectorIndex.subspace()).size(), "Should have 1 vector index entry");

        // $set a non-vector field on the doc WITHOUT embedding
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(VECTOR_BUCKET, "{\"type\": \"no-vector\"}", "{\"$set\": {\"age\": 30}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<ObjectId> updatedIds = extractObjectIds(msg);
        assertEquals(1, updatedIds.size(), "Should have updated 1 document");

        // Verify 1 vector index entry still exists (unchanged)
        assertEquals(1, fetchAllIndexedEntries(vectorIndex.subspace()).size(), "Vector index entry should still exist");
    }

    @Test
    void shouldCreateVectorIndexEntryOnUpsert() {
        // Behavior: Upsert with a vector field creates a new vector index entry for the inserted doc.
        BucketMetadata metadata = createBucketWithVectorIndex();

        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
        assertEquals(0, fetchAllIndexedEntries(vectorIndex.subspace()).size(), "Should have 0 vector index entries before upsert");

        // Upsert a new doc with embedding
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(VECTOR_BUCKET, "{\"label\": \"new\"}", "{\"$set\": {\"embedding\": [0.1, 0.2, 0.3]}, \"upsert\": true}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        // Verify 1 vector index entry exists
        List<KeyValue> entries = fetchAllIndexedEntries(vectorIndex.subspace());
        assertEquals(1, entries.size(), "Should have 1 vector index entry after upsert");

        // Decode and verify vector values
        VectorIndexValue decoded = VectorIndexValue.decode(entries.getFirst().getValue());
        assertEquals(3, decoded.vector().length);
        assertEquals(0.1f, decoded.vector()[0], 0.001f);
        assertEquals(0.2f, decoded.vector()[1], 0.001f);
        assertEquals(0.3f, decoded.vector()[2], 0.001f);
    }

    @Test
    void shouldWriteMutationLogOnUpsertWithVectorField() {
        // Behavior: Upsert with a vector field writes an INSERT mutation log entry for crash recovery.
        BucketMetadata metadata = createBucketWithVectorIndex();

        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);

        // Upsert a new doc with embedding
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(VECTOR_BUCKET, "{\"label\": \"new\"}", "{\"$set\": {\"embedding\": [0.1, 0.2, 0.3]}, \"upsert\": true}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        // Verify mutation log has an INSERT entry
        byte[] prefix = vectorIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.MUTATION_LOG.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> mutationLogEntries = tr.getRange(begin, end).asList().join();
            assertEquals(1, mutationLogEntries.size(), "Should have 1 mutation log entry after upsert");

            MutationLogValue decoded = MutationLogValue.decode(mutationLogEntries.getFirst().getValue());
            assertEquals(MutationLogMarker.INSERT, decoded.marker());
            assertEquals(3, decoded.vectorPayload().vector().length);
            assertEquals(0.1f, decoded.vectorPayload().vector()[0], 0.001f);
        }
    }

    // --- Collation tests ---

    @Test
    void shouldUpdateWithTurkishCollation() {
        // Behavior: UPDATE with Turkish PRIMARY collation matches i and İ as equivalent.
        insertDocumentsAndGetObjectIds(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"istanbul\", \"status\": \"old\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"\u0130stanbul\", \"status\": \"old\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"ankara\", \"status\": \"old\"}")
        ));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"name\": {\"$eq\": \"\u0130stanbul\"}}",
                "{\"$set\": {\"status\": \"updated\"}}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"tr\", \"strength\": 1}")).encode(buf);
        Object msg = runCommand(channel, buf);

        List<ObjectId> updatedIds = extractObjectIds(msg);
        assertEquals(2, updatedIds.size());

        ByteBuf queryBuf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"status\": {\"$eq\": \"updated\"}}").encode(queryBuf);
        Object queryMsg = runCommand(channel, queryBuf);
        List<BsonDocument> updated = extractEntries(queryMsg);
        assertEquals(2, updated.size());
    }

    @Test
    void shouldUpdateWithFrenchSecondaryCollation() {
        // Behavior: UPDATE with French SECONDARY collation is case-insensitive but accent-sensitive.
        insertDocumentsAndGetObjectIds(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"cafe\", \"status\": \"old\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Cafe\", \"status\": \"old\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"caf\u00e9\", \"status\": \"old\"}")
        ));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"name\": {\"$eq\": \"cafe\"}}",
                "{\"$set\": {\"status\": \"updated\"}}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"fr\", \"strength\": 2}")).encode(buf);
        Object msg = runCommand(channel, buf);

        List<ObjectId> updatedIds = extractObjectIds(msg);
        assertEquals(2, updatedIds.size());

        ByteBuf queryBuf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"status\": {\"$eq\": \"old\"}}").encode(queryBuf);
        Object queryMsg = runCommand(channel, queryBuf);
        List<BsonDocument> notUpdated = extractEntries(queryMsg);
        assertEquals(1, notUpdated.size());
        assertEquals("caf\u00e9", BsonHelper.getString(notUpdated.getFirst(), "name"));
    }

    @Test
    void shouldUpdateWithCollationOverridingBucket() {
        // Behavior: Query-level collation overrides bucket-level collation for UPDATE.
        createBucketWithCollation(COLLATION_BUCKET, "{\"locale\": \"en\", \"strength\": 1}");

        insertDocumentsIntoBucket(COLLATION_BUCKET, List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"cafe\", \"status\": \"old\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Cafe\", \"status\": \"old\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"caf\u00e9\", \"status\": \"old\"}")
        ));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(COLLATION_BUCKET, "{\"name\": {\"$eq\": \"cafe\"}}",
                "{\"$set\": {\"status\": \"updated\"}}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"fr\", \"strength\": 3}")).encode(buf);
        Object msg = runCommand(channel, buf);

        List<ObjectId> updatedIds = extractObjectIds(msg);
        assertEquals(1, updatedIds.size());

        ByteBuf queryBuf = Unpooled.buffer();
        cmd.query(COLLATION_BUCKET, "{\"status\": {\"$eq\": \"old\"}}").encode(queryBuf);
        Object queryMsg = runCommand(channel, queryBuf);
        List<BsonDocument> notUpdated = extractEntries(queryMsg);
        assertEquals(2, notUpdated.size());
    }

    @Test
    void shouldApplyCollationToIndexEntryOnUpdate() {
        // Behavior: When a bucket has collation and an index exists on the updated field,
        // the new index entry must be written with collation applied so that
        // collation-aware queries remain correct after the update.
        // Uses city names without the Turkish i/I distinction (ANKARA, TRABZON) so that
        // lowercase queries match their uppercase counterparts at Turkish PRIMARY strength.
        createBucketWithCollation(COLLATION_BUCKET, "{\"locale\": \"tr\", \"strength\": 1}");

        BucketCommandBuilder<byte[], byte[]> cmdBytes = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf indexBuf = Unpooled.buffer();
        cmdBytes.indexCreate(COLLATION_BUCKET, "{\"name\": {\"bson_type\": \"string\"}}").encode(indexBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, runCommand(channel, indexBuf));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, COLLATION_BUCKET);
            waitForIndexReadiness(metadata.indexes().getIndex("name", IndexSelectionPolicy.ALL).subspace());
        }
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, COLLATION_BUCKET);

        insertDocumentsIntoBucket(COLLATION_BUCKET, List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"ANKARA\", \"status\": \"active\"}")
        ));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // UPDATE: filter uses lowercase "ankara" — bucket Turkish PRIMARY collation matches "ANKARA"
        ByteBuf buf = Unpooled.buffer();
        cmd.update(COLLATION_BUCKET, "{\"name\": {\"$eq\": \"ankara\"}}",
                "{\"$set\": {\"name\": \"TRABZON\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertEquals(1, extractObjectIds(msg).size());

        // Old index entry must be gone: querying for "ANKARA" returns nothing
        ByteBuf oldBuf = Unpooled.buffer();
        cmd.query(COLLATION_BUCKET, "{\"name\": {\"$eq\": \"ANKARA\"}}").encode(oldBuf);
        assertEquals(0, extractEntries(runCommand(channel, oldBuf)).size());

        // New index entry must carry collation: "trabzon" (lower) matches stored "TRABZON" via Turkish PRIMARY
        ByteBuf newBuf = Unpooled.buffer();
        cmd.query(COLLATION_BUCKET, "{\"name\": {\"$eq\": \"trabzon\"}}").encode(newBuf);
        List<BsonDocument> results = extractEntries(runCommand(channel, newBuf));
        assertEquals(1, results.size());
        assertEquals("TRABZON", BsonHelper.getString(results.getFirst(), "name"));
    }

    @Test
    void shouldApplyIndexLevelCollationToIndexEntryOnUpdateIgnoringQueryCollation() {
        // Behavior: When index has explicit Turkish PRIMARY collation but UPDATE uses English PRIMARY
        // query collation, new index entries must be written using the index-level TR collation — not the
        // query-level EN collation. Uses ANKARA → TRABZON (pure ASCII) so EN query collation trivially
        // matches the filter; the collation distinction shows only in how the new index entry is stored.
        // A follow-up query with TR collation for "trabzon" (lowercase) finds stored "TRABZON" via
        // TR PRIMARY case-insensitivity, proving the index entry used TR — not EN — for key encoding.

        // Bucket with no collation — only the index carries Turkish PRIMARY
        createBucket(COLLATION_BUCKET);

        BucketCommandBuilder<byte[], byte[]> cmdBytes = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf indexBuf = Unpooled.buffer();
        cmdBytes.indexCreate(COLLATION_BUCKET,
                "{\"name\": {\"bson_type\": \"string\", \"collation\": {\"locale\": \"tr\", \"strength\": 1}}}").encode(indexBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, runCommand(channel, indexBuf));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, COLLATION_BUCKET);
            waitForIndexReadiness(metadata.indexes().getIndex("name", IndexSelectionPolicy.ALL).subspace());
        }
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, COLLATION_BUCKET);

        insertDocumentsIntoBucket(COLLATION_BUCKET, List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"ANKARA\", \"status\": \"old\"}")
        ));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // UPDATE: English PRIMARY query collation finds "ANKARA" (exact match), sets name to "TRABZON"
        // New index entry for "TRABZON" must use Turkish PRIMARY (index-level), not English PRIMARY (query-level)
        ByteBuf buf = Unpooled.buffer();
        cmd.update(COLLATION_BUCKET, "{\"name\": {\"$eq\": \"ANKARA\"}}",
                "{\"$set\": {\"name\": \"TRABZON\"}}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"en\", \"strength\": 1}")).encode(buf);
        Object msg = runCommand(channel, buf);
        assertEquals(1, extractObjectIds(msg).size());

        // Old document data is gone: binary query for "ANKARA" returns nothing (document now stores "TRABZON")
        ByteBuf oldBuf = Unpooled.buffer();
        cmd.query(COLLATION_BUCKET, "{\"name\": {\"$eq\": \"ANKARA\"}}").encode(oldBuf);
        assertEquals(0, extractEntries(runCommand(channel, oldBuf)).size());

        // New index entry uses Turkish PRIMARY: "trabzon" (lower) matches stored "TRABZON" via TR PRIMARY strength 1
        // If English PRIMARY had been used for the index write, this query would return 0 results
        // because TR_PRIMARY("trabzon") bytes differ from EN_PRIMARY("TRABZON") bytes
        ByteBuf newBuf = Unpooled.buffer();
        cmd.query(COLLATION_BUCKET, "{\"name\": {\"$eq\": \"trabzon\"}}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"tr\", \"strength\": 1}")).encode(newBuf);
        List<BsonDocument> results = extractEntries(runCommand(channel, newBuf));
        assertEquals(1, results.size());
        assertEquals("TRABZON", BsonHelper.getString(results.getFirst(), "name"));
    }

    @Test
    void shouldUpdateWithNoCollationPreservingBehavior() {
        // Behavior: UPDATE without COLLATION uses binary comparison (exact match only).
        insertDocumentsAndGetObjectIds(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"istanbul\", \"status\": \"old\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"\u0130stanbul\", \"status\": \"old\"}")
        ));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"name\": {\"$eq\": \"\u0130stanbul\"}}",
                "{\"$set\": {\"status\": \"updated\"}}").encode(buf);
        Object msg = runCommand(channel, buf);

        List<ObjectId> updatedIds = extractObjectIds(msg);
        assertEquals(1, updatedIds.size());

        ByteBuf queryBuf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"status\": {\"$eq\": \"old\"}}").encode(queryBuf);
        Object queryMsg = runCommand(channel, queryBuf);
        List<BsonDocument> notUpdated = extractEntries(queryMsg);
        assertEquals(1, notUpdated.size());
        assertEquals("istanbul", BsonHelper.getString(notUpdated.getFirst(), "name"));
    }

    @Test
    void shouldExcludeUnchangedDocumentFromObjectIdsWhenSetValueIsIdentical() {
        // Behavior: UPDATE with $set writing the value a field already holds leaves the document
        // unchanged. The document is not modified and its id does not appear in object_ids.
        List<byte[]> testDocuments = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"name\": \"Alice\"}", "{\"$set\": {\"age\": 25}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            updatedObjectIds = extractObjectIds(msg);
        }

        assertTrue(updatedObjectIds.isEmpty(), "object_ids should be empty when the update is a no-op");

        // The document is still intact and unchanged
        List<BsonDocument> entries;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"name\": \"Alice\"}").encode(buf);
            Object msg = runCommand(channel, buf);
            entries = extractEntries(msg);
        }
        assertEquals(1, entries.size(), "Document should still be retrievable");
        BsonDocument alice = entries.getFirst();
        assertEquals(25, BsonHelper.getInteger(alice, "age"), "Field value should be unchanged");
        assertEquals("Alice", BsonHelper.getString(alice, "name"));
    }

    @Test
    void shouldFindDocumentByChildSelectorAfterParentPathUpdate() {
        // Behavior: After BUCKET.UPDATE replaces a parent array, BUCKET.QUERY on a child-selector
        // index returns the document by its new values and no longer by the removed values.
        SingleFieldIndexDefinition tagsIndexDefinition = SingleFieldIndexDefinition.create("tags-name-index", "tags.name", BsonType.STRING, true, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(tagsIndexDefinition);

        List<byte[]> testDocuments = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"tags\": [{\"name\": \"java\"}]}")
        );
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Replace the whole tags array
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"tags\": [{\"name\": \"go\"}]}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            List<ObjectId> updatedObjectIds = extractObjectIds(msg);
            assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");
        }

        // Query by the new value through the child-selector index
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"tags.name\": {\"$eq\": \"go\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should find the document by the new tag value");
            assertEquals("Alice", BsonHelper.getString(entries.getFirst(), "name"));
        }

        // The removed value no longer matches
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"tags.name\": {\"$eq\": \"java\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            List<BsonDocument> entries = extractEntries(msg);
            assertTrue(entries.isEmpty(), "Should NOT find the document by the removed tag value");
        }
    }
}