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

import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BsonHelper;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BucketUpdateHandlerWithUpsertTest extends BaseBucketHandlerTest {

    @BeforeEach
    void setUp() {
        createBucket(TEST_BUCKET);
    }

    @Test
    void shouldUpsertWhenNoMatchingDocument() {
        // Behavior: When upsert is true and no documents match, a new document is created
        // combining equality conditions from the query with $set operations.
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Upsert with a query that matches nothing
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"name\": \"NewUser\"}", "{\"$set\": {\"age\": 30, \"city\": \"Berlin\"}, \"upsert\": true}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);

        // Should have created 1 document
        List<ObjectId> objectIds = extractObjectIds(msg);
        assertEquals(1, objectIds.size(), "Should have upserted 1 document");

        // Query to verify the upserted document
        buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": \"NewUser\"}").encode(buf);
        msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size(), "Should find the upserted document");

        // Verify the document content
        for (BsonDocument document : entries) {
            assertEquals("NewUser", BsonHelper.getString(document, "name"), "name should be from query");
            assertEquals(30, BsonHelper.getInteger(document, "age"), "age should be from $set");
            assertEquals("Berlin", BsonHelper.getString(document, "city"), "city should be from $set");
        }
    }

    @Test
    void shouldNotUpsertWhenMatchingDocumentExists() {
        // Behavior: When upsert is true but a matching document exists, update it instead of inserting.
        List<byte[]> testDocuments = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"ExistingUser\", \"age\": 25, \"city\": \"London\"}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        insertDocumentsAndGetObjectIds(testDocuments);

        // Upsert with a query that matches the existing document
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"name\": \"ExistingUser\"}", "{\"$set\": {\"age\": 35}, \"upsert\": true}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);

        // Should have updated 1 document
        List<ObjectId> objectIds = extractObjectIds(msg);
        assertEquals(1, objectIds.size(), "Should have updated 1 document");

        // Query all documents to verify only 1 exists
        buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(buf);
        msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size(), "Should still have only 1 document");

        // Verify the updated document
        for (BsonDocument document : entries) {
            assertEquals("ExistingUser", BsonHelper.getString(document, "name"));
            assertEquals(35, BsonHelper.getInteger(document, "age"), "age should be updated to 35");
            assertEquals("London", BsonHelper.getString(document, "city"), "city should remain unchanged");
        }
    }

    @Test
    void shouldUpsertWithSetOverridingQueryConditions() {
        // Behavior: When $set contains a field also in the query equality condition,
        // $set value takes precedence.
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Upsert with query having name=OldName but $set having name=NewName
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"name\": \"OldName\", \"type\": \"user\"}", "{\"$set\": {\"name\": \"NewName\", \"active\": true}, \"upsert\": true}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);

        // Query to verify the upserted document
        buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(buf);
        msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size(), "Should find 1 upserted document");

        for (BsonDocument document : entries) {
            assertEquals("NewName", BsonHelper.getString(document, "name"), "name should be from $set, not query");
            assertEquals("user", BsonHelper.getString(document, "type"), "type should be from query");
            assertTrue(BsonHelper.getBoolean(document, "active"), "active should be from $set");
        }
    }

    @Test
    void shouldIgnoreNonEqualityConditionsInUpsert() {
        // Behavior: Non-equality operators ($gt, $lt, $in, $or, etc.) don't contribute to upsert document.
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Upsert with non-equality conditions
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"age\": {\"$gt\": 30}, \"status\": \"active\"}", "{\"$set\": {\"name\": \"TestUser\"}, \"upsert\": true}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);

        // Query to verify the upserted document
        buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(buf);
        msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size());

        for (BsonDocument document : entries) {
            assertEquals("TestUser", BsonHelper.getString(document, "name"), "name should be from $set");
            assertEquals("active", BsonHelper.getString(document, "status"), "status (equality) should be in doc");
            assertNull(BsonHelper.getInteger(document, "age"), "age ($gt is not equality) should not be in doc");
        }
    }

    @Test
    void shouldUpsertWithEmptyQuery() {
        // Behavior: When query is empty {}, upsert creates document with only $set fields.
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Upsert with empty query
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"x\": 1, \"y\": 2}, \"upsert\": true}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);

        // Query to verify the upserted document
        buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(buf);
        msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size());

        for (BsonDocument document : entries) {
            assertEquals(1, BsonHelper.getInteger(document, "x"));
            assertEquals(2, BsonHelper.getInteger(document, "y"));
        }
    }

    @Test
    void shouldUpsertWithNestedEquality() {
        // Behavior: Nested equality conditions like "address.city" should be extracted correctly.
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Upsert with nested equality
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"address.city\": \"Paris\", \"address.country\": \"France\"}", "{\"$set\": {\"name\": \"Pierre\"}, \"upsert\": true}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);

        // Query to verify the upserted document
        buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(buf);
        msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size());

        for (BsonDocument document : entries) {
            assertEquals("Pierre", BsonHelper.getString(document, "name"));
            BsonDocument address = document.getDocument("address");
            assertNotNull(address, "address should be a nested document");
            assertEquals("Paris", address.getString("city").getValue());
            assertEquals("France", address.getString("country").getValue());
        }
    }

    @Test
    void shouldReturnCorrectObjectIdAfterUpsert() {
        // Behavior: The object_id returned after upsert should allow direct lookup
        // of the created document.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"unique\": \"doc123\"}", "{\"$set\": {\"value\": 42}, \"upsert\": true}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        List<ObjectId> objectIds = extractObjectIds(msg);
        assertEquals(1, objectIds.size(), "Should have 1 object_id after upsert");

        // Query using the query that was used for upsert
        buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"unique\": \"doc123\"}").encode(buf);
        msg = runCommand(channel, buf);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size(), "Should find the upserted document");

        // Verify the document content
        for (BsonDocument document : entries) {
            assertEquals("doc123", BsonHelper.getString(document, "unique"));
            assertEquals(42, BsonHelper.getInteger(document, "value"));
        }
    }

    @Test
    void shouldUpsertWithMultikeyIndexedField() {
        // Behavior: Upsert creates document with array field that's multikey-indexed.
        // Index should have one entry per unique array element.

        // First, create a multikey index on "tags"
        SingleFieldIndexDefinition tagsIndex = SingleFieldIndexDefinition.create("tags_idx", "tags", BsonType.STRING, true, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(tagsIndex);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"name\": \"Tagged\"}", "{\"$set\": {\"tags\": [\"a\", \"b\", \"c\"]}, \"upsert\": true}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);

        // Verify document created
        buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": \"Tagged\"}").encode(buf);
        msg = runCommand(channel, buf);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size(), "Should find the upserted document");

        // Verify the document content has the array
        for (BsonDocument document : entries) {
            assertEquals("Tagged", BsonHelper.getString(document, "name"));
            assertTrue(document.containsKey("tags"), "Document should have tags field");
        }
    }

    @Test
    void shouldUpsertWithInQueryNotExtractingEquality() {
        // Behavior: $in queries don't contribute the EQ(equality) conditions.
        // Document is created with only $set fields.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"status\": {\"$in\": [\"a\", \"b\"]}}", "{\"$set\": {\"created\": true}, \"upsert\": true}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);

        // Query all documents
        buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(buf);
        msg = runCommand(channel, buf);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size(), "Should have 1 document");

        // Verify "status" was NOT extracted from $in
        for (BsonDocument doc : entries) {
            assertFalse(doc.containsKey("status"), "status should not be in doc from $in");
            assertTrue(doc.containsKey("created"), "created should be from $set");
        }
    }

    @Test
    void shouldThrowIndexTypeMismatchExceptionOnUpsert() {
        // Behavior: Upsert with a value that doesn't match the indexed field's expected type
        // throws IndexTypeMismatchException when strict_types=true (default).

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndex);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"name\": \"John\"}", "{\"$set\": {\"age\": \"twenty-five\"}, \"upsert\": true}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().startsWith("INDEXTYPE_MISMATCH"),
                "Should return INDEXTYPE_MISMATCH error");
        assertTrue(errorMessage.content().contains("index 'age_idx' expects 'INT32'"),
                "Error message should mention the index name and expected type");
        assertTrue(errorMessage.content().contains("selector 'age' matched a value of type 'STRING'"),
                "Error message should mention the selector and actual type");
    }

    @Test
    void shouldUpsertWithUserProvidedIdFromQueryFilter() {
        // Behavior: When the query filter contains _id = <ObjectId> and upsert creates a new
        // document, the upserted document should use that _id instead of auto-generating one.
        ObjectId providedId = new ObjectId();
        String query = String.format("{\"_id\": {\"$oid\": \"%s\"}, \"name\": \"Alice\"}", providedId.toHexString());

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, query, "{\"$set\": {\"age\": 25}, \"upsert\": true}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        List<ObjectId> objectIds = extractObjectIds(msg);
        assertEquals(1, objectIds.size(), "Should have upserted 1 document");
        assertEquals(providedId, objectIds.getFirst(), "Upserted document should use the provided _id");

        // Query to verify the document
        buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(buf);
        msg = runCommand(channel, buf);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size());

        BsonDocument document = entries.getFirst();
        assertEquals(providedId, document.getObjectId("_id").getValue(), "Document _id should match provided id");
        assertEquals("Alice", BsonHelper.getString(document, "name"));
        assertEquals(25, BsonHelper.getInteger(document, "age"));
    }

    @Test
    void shouldRejectDuplicateIdOnUpsert() {
        // Behavior: When upserting with _id that already exists in the bucket,
        // a DuplicateKeyException should be thrown.
        ObjectId existingId = new ObjectId();
        String json = String.format("{\"_id\": {\"$oid\": \"%s\"}, \"name\": \"Existing\"}", existingId.toHexString());
        List<byte[]> testDocuments = List.of(BSONUtil.jsonToDocumentThenBytes(json));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        insertDocumentsAndGetObjectIds(testDocuments);

        // Now upsert with the same _id
        String query = String.format("{\"_id\": {\"$oid\": \"%s\"}, \"name\": \"NewName\"}", existingId.toHexString());

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, query, "{\"$set\": {\"age\": 30}, \"upsert\": true}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("DUPLICATEKEY"),
                "Should return DUPLICATEKEY error");
        assertTrue(errorMessage.content().contains(existingId.toHexString()),
                "Error should contain the duplicate ObjectId");
    }

    @Test
    void shouldRejectNonObjectIdTypeInUpsertFilter() {
        // Behavior: When the query filter contains _id with a non-ObjectId type (e.g., string)
        // and upsert creates a new document, an error should be returned.
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"_id\": \"not-an-objectid\"}", "{\"$set\": {\"name\": \"Test\"}, \"upsert\": true}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("_id field must be of type ObjectId"),
                "Should return error about _id type");
    }

    @Test
    void shouldRejectIdInSetOperations() {
        // Behavior: Using $set to modify _id should fail because _id is immutable.
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ObjectId newId = new ObjectId();
        String setOp = String.format("{\"$set\": {\"_id\": {\"$oid\": \"%s\"}}, \"upsert\": true}", newId.toHexString());

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"name\": \"Test\"}", setOp).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("_id field is immutable"),
                "Should return error about _id immutability");
    }
}
