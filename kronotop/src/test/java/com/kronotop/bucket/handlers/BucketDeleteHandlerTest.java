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
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class BucketDeleteHandlerTest extends BaseBucketHandlerTest {

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
    void shouldDeleteWithRegexFilter() {
        // Behavior: BUCKET.DELETE with a $regex filter removes only the matching string documents and returns their ObjectIds.
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alana\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        insertDocumentsAndGetObjectIds(documents);

        List<ObjectId> deletedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{\"name\": {\"$regex\": \"^Al\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            deletedObjectIds = extractObjectIds(msg);
        }

        assertEquals(2, deletedObjectIds.size(), "Should delete the two documents whose name starts with 'Al'");

        ByteBuf queryBuf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(queryBuf);
        Object queryMsg = runCommand(channel, queryBuf);
        assertInstanceOf(MapRedisMessage.class, queryMsg);

        List<BsonDocument> remaining = extractEntries(queryMsg);
        assertEquals(1, remaining.size(), "Only 'Bob' should remain after the regex delete");
        assertEquals("Bob", remaining.getFirst().getString("name").getValue());
    }

    @Test
    void shouldDeleteWithAgeFilter() {
        // Behavior: Deleting documents with a filter returns ObjectIds of matched documents and removes them from the bucket.

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

        // Insert documents and collect object IDs
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        List<ObjectId> allInsertedObjectIds = new ArrayList<>(insertedDocs.keySet());

        assertEquals(5, allInsertedObjectIds.size(), "Should have inserted 5 documents");

        // Step 2: Delete documents with age > 30 using BUCKET.DELETE
        List<ObjectId> deletedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{\"age\": {\"$gt\": 30}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            deletedObjectIds = extractObjectIds(msg);
        }

        // Verify that we deleted the right number of documents (Bob: 35, Charlie: 45, Eve: 38)
        assertEquals(3, deletedObjectIds.size(), "Should have deleted 3 documents with age > 30");

        // Verify all deleted object IDs were from our original insert
        for (ObjectId deletedOid : deletedObjectIds) {
            assertTrue(allInsertedObjectIds.contains(deletedOid),
                    "Deleted object ID should be from original insert: " + deletedOid);
        }

        // Step 3: Calculate remaining object IDs (should be Alice: 25, Diana: 28)
        List<ObjectId> expectedRemainingObjectIds = allInsertedObjectIds.stream()
                .filter(oid -> !deletedObjectIds.contains(oid))
                .toList();

        assertEquals(2, expectedRemainingObjectIds.size(), "Should have 2 remaining documents");

        // Step 4: Query all remaining documents and verify
        Map<ObjectId, BsonDocument> actualRemainingDocuments = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                ObjectId oid = doc.getObjectId("_id").getValue();
                actualRemainingDocuments.put(oid, doc);
            }
        }

        // Step 5: Verify that remaining documents match expectations
        assertEquals(expectedRemainingObjectIds.size(), actualRemainingDocuments.size(),
                "Query should return exactly the expected remaining documents");

        // Verify that all returned object IDs are in our expected set
        for (ObjectId actualOid : actualRemainingDocuments.keySet()) {
            assertTrue(expectedRemainingObjectIds.contains(actualOid),
                    "Remaining object ID should be in expected set: " + actualOid);
        }

        // Step 6: Verify that remaining documents have age <= 30
        for (BsonDocument doc : actualRemainingDocuments.values()) {
            BsonInt32 age = doc.getInt32("age");
            assertNotNull(age, "Document should have age field");
            assertTrue(age.getValue() <= 30, "Remaining document should have age <= 30, but was: " + age);
        }

        // Verify specific expected documents remain (Alice: 25, Diana: 28)
        Set<String> remainingNames = actualRemainingDocuments.values().stream()
                .map(doc -> doc.getString("name").getValue())
                .collect(Collectors.toSet());

        assertEquals(Set.of("Alice", "Diana"), remainingNames,
                "Should have Alice and Diana remaining after delete");
    }

    @Test
    void shouldHandleNoMatchingDeletes() {
        // Behavior: Deleting with a filter that matches no documents returns an empty object_ids array and leaves all documents intact.

        // Insert documents but delete with a filter that matches none
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert documents
        insertDocumentsAndGetObjectIds(testDocuments);

        // Try to delete documents with age > 50 (should match none)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{\"age\": {\"$gt\": 50}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<ObjectId> deletedIds = extractObjectIds(msg);
            assertEquals(0, deletedIds.size(), "Should delete no documents");
        }

        // Verify all documents still exist
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(2, entries.size(), "All documents should still exist");
        }
    }

    @Test
    void shouldDeleteAllDocuments() {
        // Behavior: Deleting with an empty filter removes all documents from the bucket and returns their ObjectIds.

        // Insert documents and delete all with an empty filter
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 35}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert documents
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        List<ObjectId> insertedObjectIds = new ArrayList<>(insertedDocs.keySet());

        // Delete all documents with an empty filter
        List<ObjectId> deletedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            deletedObjectIds = extractObjectIds(msg);
        }

        // Verify all documents were deleted
        assertEquals(3, deletedObjectIds.size(), "Should delete all 3 documents");
        assertTrue(insertedObjectIds.containsAll(deletedObjectIds),
                "All deleted object IDs should be from original insert");

        // Verify the bucket is empty
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(0, entries.size(), "Bucket should be empty after delete all");
        }
    }

    @Test
    void shouldDeleteWithinTransaction() {
        // Behavior: Delete within a BEGIN/COMMIT transaction is only committed after COMMIT; the correct documents are removed.

        // Step 1: Insert test documents with different ages (outside transaction)
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25, \"city\": \"New York\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35, \"city\": \"London\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 45, \"city\": \"Paris\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 28, \"city\": \"Tokyo\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 38, \"city\": \"Berlin\"}")
        );

        // Insert documents and collect object IDs
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        List<ObjectId> allInsertedObjectIds = new ArrayList<>(insertedDocs.keySet());

        assertEquals(5, allInsertedObjectIds.size(), "Should have inserted 5 documents");

        // Step 2: Delete documents with age > 30 within a transaction
        KronotopCommandBuilder<String, String> kronotopCmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        BucketCommandBuilder<String, String> bucketCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(bucketCmd, RESPVersion.RESP3);

        List<ObjectId> deletedObjectIds;

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
            bucketCmd.delete(TEST_BUCKET, "{\"age\": {\"$gt\": 30}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            deletedObjectIds = extractObjectIds(msg);

            // Verify that we deleted the right number of documents (Bob: 35, Charlie: 45, Eve: 38)
            assertEquals(3, deletedObjectIds.size(), "Should have deleted 3 documents with age > 30");

            // Verify all deleted object IDs were from our original insert
            for (ObjectId deletedOid : deletedObjectIds) {
                assertTrue(allInsertedObjectIds.contains(deletedOid),
                        "Deleted object ID should be from original insert: " + deletedOid);
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
        Map<ObjectId, BsonDocument> actualRemainingDocuments = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                ObjectId oid = doc.getObjectId("_id").getValue();
                actualRemainingDocuments.put(oid, doc);
            }
        }

        // Step 4: Verify that documents with age > 30 were deleted
        assertEquals(2, actualRemainingDocuments.size(), "Should have 2 remaining documents after transaction delete");

        // Verify that remaining documents have age <= 30
        for (BsonDocument doc : actualRemainingDocuments.values()) {
            Integer age = BsonHelper.getInteger(doc, "age");
            assertNotNull(age, "Document should have age field");
            assertTrue(age <= 30, "Remaining document should have age <= 30, but was: " + age);
        }

        // Verify specific expected documents remain (Alice: 25, Diana: 28)
        Set<String> remainingNames = actualRemainingDocuments.values().stream()
                .map(doc -> BsonHelper.getString(doc, "name"))
                .collect(Collectors.toSet());

        assertEquals(Set.of("Alice", "Diana"), remainingNames,
                "Should have Alice and Diana remaining after transaction delete");
    }

    @Test
    void shouldDeleteWithLimitAndAdvance() {
        // Behavior: Delete with LIMIT restricts per-batch count; BUCKET.ADVANCE DELETE continues deleting remaining matches.

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

        // Insert documents and collect object IDs
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        List<ObjectId> allInsertedObjectIds = new ArrayList<>(insertedDocs.keySet());

        assertEquals(5, allInsertedObjectIds.size(), "Should have inserted 5 documents");

        // Step 2: Delete documents with age > 30 using limit=1
        List<ObjectId> allDeletedObjectIds = new ArrayList<>();
        int cursorId;

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{\"age\": {\"$gt\": 30}}", BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);

            // Extract cursor_id
            cursorId = extractCursorId(msg);

            // Extract object_ids from initial delete response
            List<ObjectId> deletedIds = extractObjectIds(msg);
            allDeletedObjectIds.addAll(deletedIds);
        }

        // Should have deleted exactly 1 document in the first batch
        assertEquals(1, allDeletedObjectIds.size(), "Should have deleted exactly 1 document with limit=1");

        // Step 3: Use BUCKET.ADVANCE DELETE to continue deleting remaining documents
        int maxAdvanceCalls = 10; // Safety limit
        int advanceCalls = 0;

        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceDelete(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<ObjectId> advancedIds = extractObjectIds(msg);
            if (advancedIds.isEmpty()) {
                break;
            }

            allDeletedObjectIds.addAll(advancedIds);

            advanceCalls++;
        }

        // Should have deleted 4 documents total (age > 30: Alice=35, Bob=40, Charlie=45, Eve=50)
        assertEquals(4, allDeletedObjectIds.size(), "Should have deleted 4 documents with age > 30");

        // Verify all deleted object IDs were from our original insert
        for (ObjectId deletedOid : allDeletedObjectIds) {
            assertTrue(allInsertedObjectIds.contains(deletedOid),
                    "Deleted object ID should be from original insert: " + deletedOid);
        }

        // Step 4: Query remaining documents to verify only Diana (age=25) remains
        Map<ObjectId, BsonDocument> remainingDocuments = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                ObjectId oid = doc.getObjectId("_id").getValue();
                remainingDocuments.put(oid, doc);
            }
        }

        // Step 5: Verify only Diana (age=25) remains
        assertEquals(1, remainingDocuments.size(), "Should have 1 remaining document after delete");

        BsonDocument remainingDoc = remainingDocuments.values().iterator().next();
        assertEquals("Diana", BsonHelper.getString(remainingDoc, "name"), "Remaining document should be Diana");
        assertEquals(25, BsonHelper.getInteger(remainingDoc, "age"), "Remaining document should have age 25");

        // Verify that none of the remaining object IDs were deleted
        for (ObjectId remainingOid : remainingDocuments.keySet()) {
            assertFalse(allDeletedObjectIds.contains(remainingOid),
                    "Remaining object ID should NOT be in deleted object IDs: " + remainingOid);
        }
    }

    @Test
    void shouldThrowUnsupportedArgumentExceptionWhenUsingSortBy() {
        // Behavior: BUCKET.DELETE does not support SORTBY; using it throws an error.
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.delete(TEST_BUCKET, "{}", BucketQueryArgs.Builder.sortBy("age", "ASC")).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR 'SORTBY' is an unsupported argument", errorMessage.content());
    }

    // --- Numeric Widening Tests ---

    @Test
    void shouldThrowBucketBeingRemovedExceptionWhenDeletingFromRemovedBucket() {
        // Behavior: Deleting from a bucket marked as removed returns a BUCKETBEINGREMOVED error.

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

        // Try to delete from the removed bucket
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.delete(TEST_BUCKET, "{}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertEquals("BUCKETBEINGREMOVED Bucket 'test-bucket' is being removed", errorMessage.content());
    }

    @Test
    void shouldDeleteWithOrQueryLimitAndAdvance() {
        // Behavior: DELETE with $or on two indexed fields and limit=2 triggers child rewind in
        // UnionNode. Each ADVANCE DELETE batch deletes at most 2 documents. All matching documents
        // are deleted across multiple ADVANCE calls without missing or double-deleting.

        SingleFieldIndexDefinition typeIndex = SingleFieldIndexDefinition.create("type-idx", "type", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition levelIndex = SingleFieldIndexDefinition.create("level-idx", "level", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(typeIndex);
        createIndexThenWaitForReadiness(levelIndex);

        // 10 docs: 3 match both, 3 match type only, 2 match level only, 2 match neither
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"B1\", \"type\": \"active\", \"level\": 60}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"B2\", \"type\": \"active\", \"level\": 70}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"B3\", \"type\": \"active\", \"level\": 80}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"T1\", \"type\": \"active\", \"level\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"T2\", \"type\": \"active\", \"level\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"T3\", \"type\": \"active\", \"level\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"L1\", \"type\": \"inactive\", \"level\": 90}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"L2\", \"type\": \"inactive\", \"level\": 100}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"N1\", \"type\": \"inactive\", \"level\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"N2\", \"type\": \"inactive\", \"level\": 20}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(10, insertedDocs.size());

        // DELETE with $or filter, limit=2
        List<ObjectId> allDeletedObjectIds = new ArrayList<>();
        int cursorId;

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{\"$or\": [{\"type\": {\"$eq\": \"active\"}}, {\"level\": {\"$gt\": 50}}]}",
                    BucketQueryArgs.Builder.limit(2)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);
            allDeletedObjectIds.addAll(extractObjectIds(msg));
        }

        assertTrue(allDeletedObjectIds.size() <= 2, "First batch should delete at most 2 documents");

        // ADVANCE DELETE until exhausted
        int maxAdvanceCalls = 15;
        int advanceCalls = 0;

        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceDelete(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<ObjectId> advancedIds = extractObjectIds(msg);
            if (advancedIds.isEmpty()) {
                break;
            }

            assertTrue(advancedIds.size() <= 2, "Each batch should delete at most 2 documents");
            allDeletedObjectIds.addAll(advancedIds);
            advanceCalls++;
        }

        // 8 matching docs: B1-B3 (both), T1-T3 (type only), L1-L2 (level only)
        assertEquals(8, allDeletedObjectIds.size(), "Should have deleted 8 matching documents");

        // Verify no duplicate ObjectIds
        Set<ObjectId> uniqueDeletedIds = new HashSet<>(allDeletedObjectIds);
        assertEquals(allDeletedObjectIds.size(), uniqueDeletedIds.size(), "No duplicate deletes");

        // Verify remaining documents: only N1 and N2 should survive
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(2, entries.size(), "Should have 2 remaining documents");

            Set<String> remainingNames = new HashSet<>();
            for (BsonDocument doc : entries) {
                remainingNames.add(BsonHelper.getString(doc, "name"));
            }
            assertEquals(Set.of("N1", "N2"), remainingNames);
        }
    }

    @Test
    void shouldDeleteWidenedInt32DocumentAndCleanUpInt64IndexEntry() {
        // Behavior: Deleting a document whose INT32 value was widened to INT64 at insert-time
        // correctly removes the widened index entry and decrements cardinality.
        SingleFieldIndexDefinition indexDefinition = SingleFieldIndexDefinition.create(
                "age-index", "age", BsonType.INT64, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(indexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 45}")
        );
        insertDocumentsAndGetObjectIds(testDocuments);

        Index index = metadata.indexes().getIndex(indexDefinition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index);

        // Precondition: 3 widened index entries, cardinality 3
        assertEquals(3, fetchAllIndexedEntries(index.subspace()).size(),
                "Should have 3 index entries before delete");
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics stats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), indexDefinition.id());
            assertEquals(3L, stats.cardinality(), "Cardinality should be 3 before delete");
        }

        // Delete documents with age > 30 (Bob and Charlie)
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        List<ObjectId> deletedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{\"age\": {\"$gt\": 30}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            deletedObjectIds = extractObjectIds(msg);
        }

        assertEquals(2, deletedObjectIds.size(), "Should have deleted 2 documents with age > 30");

        // Post-deletion: 1 index entry remains (Alice, age=25 widened to 25L)
        List<KeyValue> remainingEntries = fetchAllIndexedEntries(index.subspace());
        assertEquals(1, remainingEntries.size(), "Should have 1 index entry after delete");

        Tuple unpackedIndex = index.subspace().unpack(remainingEntries.get(0).getKey());
        assertEquals(25L, unpackedIndex.get(1),
                "Remaining widened INT32 value 25 should be stored as Long 25L");

        // Cardinality should be 1
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics stats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), indexDefinition.id());
            assertEquals(1L, stats.cardinality(), "Cardinality should be 1 after deleting 2 documents");
        }

        // Only Alice should remain
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size());
            assertEquals("Alice", BsonHelper.getString(entries.getFirst(), "name"));
        }
    }

    @Test
    void shouldDeleteWidenedInt32DocumentAndCleanUpDoubleIndexEntry() {
        // Behavior: Deleting a document whose INT32 value was widened to DOUBLE at insert-time
        // correctly removes the widened index entry and decrements cardinality.
        SingleFieldIndexDefinition indexDefinition = SingleFieldIndexDefinition.create(
                "score-index", "score", BsonType.DOUBLE, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(indexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"score\": 100}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"score\": 200}")
        );
        insertDocumentsAndGetObjectIds(testDocuments);

        Index index = metadata.indexes().getIndex(indexDefinition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index);

        // Precondition: 2 widened index entries, cardinality 2
        assertEquals(2, fetchAllIndexedEntries(index.subspace()).size(),
                "Should have 2 index entries before delete");
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics stats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), indexDefinition.id());
            assertEquals(2L, stats.cardinality(), "Cardinality should be 2 before delete");
        }

        // Delete Bob via non-indexed field (full-scan path)
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        List<ObjectId> deletedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{\"name\": \"Bob\"}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            deletedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, deletedObjectIds.size(), "Should have deleted 1 document");

        // Post-deletion: 1 index entry remains (Alice, score=100 widened to 100.0)
        List<KeyValue> remainingEntries = fetchAllIndexedEntries(index.subspace());
        assertEquals(1, remainingEntries.size(), "Should have 1 index entry after delete");

        Tuple unpackedIndex = index.subspace().unpack(remainingEntries.get(0).getKey());
        Object actualIndexValue = unpackedIndex.get(1);
        assertInstanceOf(Double.class, actualIndexValue,
                "Remaining widened value should be stored as Double");
        assertEquals(100.0, actualIndexValue,
                "Remaining widened INT32 value 100 should be stored as Double 100.0");

        // Cardinality should be 1
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics stats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), indexDefinition.id());
            assertEquals(1L, stats.cardinality(), "Cardinality should be 1 after deleting 1 document");
        }
    }

    @Test
    void shouldDeleteWidenedInt32DocumentViaInt64IndexRangeScan() {
        // Behavior: DELETE with INT32 range predicate on INT64 index uses IndexPredicateResolver
        // widening to find and delete widened documents via index range scan.
        SingleFieldIndexDefinition indexDefinition = SingleFieldIndexDefinition.create(
                "age-index", "age", BsonType.INT64, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(indexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 40}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 50}")
        );
        insertDocumentsAndGetObjectIds(testDocuments);

        Index index = metadata.indexes().getIndex(indexDefinition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index);

        // Precondition: 4 widened index entries, cardinality 4
        assertEquals(4, fetchAllIndexedEntries(index.subspace()).size(),
                "Should have 4 index entries before delete");
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics stats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), indexDefinition.id());
            assertEquals(4L, stats.cardinality(), "Cardinality should be 4 before delete");
        }

        // Delete with INT32 range predicate — IndexPredicateResolver must widen 40 to BsonInt64(40)
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        List<ObjectId> deletedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{\"age\": {\"$gte\": 40}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            deletedObjectIds = extractObjectIds(msg);
        }

        assertEquals(2, deletedObjectIds.size(), "Should have deleted 2 documents with age >= 40");

        // Post-deletion: 2 index entries remain (20L and 30L)
        List<KeyValue> remainingEntries = fetchAllIndexedEntries(index.subspace());
        assertEquals(2, remainingEntries.size(), "Should have 2 index entries after delete");

        Set<Object> remainingValues = new HashSet<>();
        for (KeyValue kv : remainingEntries) {
            Tuple unpacked = index.subspace().unpack(kv.getKey());
            remainingValues.add(unpacked.get(1));
        }
        assertEquals(Set.of(20L, 30L), remainingValues,
                "Remaining index entries should be widened values 20L and 30L");

        // Cardinality should be 2
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics stats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), indexDefinition.id());
            assertEquals(2L, stats.cardinality(), "Cardinality should be 2 after deleting 2 documents");
        }

        // Only Alice and Bob should remain
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(2, entries.size());

            Set<String> remainingNames = new HashSet<>();
            for (BsonDocument doc : entries) {
                remainingNames.add(BsonHelper.getString(doc, "name"));
            }
            assertEquals(Set.of("Alice", "Bob"), remainingNames);
        }
    }

    // --- Vector Index Tests ---

    @Test
    void shouldDeleteAllWidenedInt32DocumentsAndResetInt64IndexCardinalityToZero() {
        // Behavior: Deleting all documents whose INT32 values were widened to INT64 at insert-time
        // removes every index entry and resets cardinality to zero.
        SingleFieldIndexDefinition indexDefinition = SingleFieldIndexDefinition.create(
                "age-index", "age", BsonType.INT64, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(indexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 40}")
        );
        insertDocumentsAndGetObjectIds(testDocuments);

        Index index = metadata.indexes().getIndex(indexDefinition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index);

        // Precondition: 3 widened index entries, cardinality 3
        assertEquals(3, fetchAllIndexedEntries(index.subspace()).size(),
                "Should have 3 index entries before delete");
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics stats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), indexDefinition.id());
            assertEquals(3L, stats.cardinality(), "Cardinality should be 3 before delete");
        }

        // Delete all documents
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        List<ObjectId> deletedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            deletedObjectIds = extractObjectIds(msg);
        }

        assertEquals(3, deletedObjectIds.size(), "Should have deleted all 3 documents");

        // Post-deletion: 0 index entries, cardinality 0
        assertEquals(0, fetchAllIndexedEntries(index.subspace()).size(),
                "Should have 0 index entries after deleting all documents");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics stats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), indexDefinition.id());
            assertEquals(0L, stats.cardinality(), "Cardinality should be 0 after deleting all documents");
        }

        // Bucket should be empty
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(0, entries.size(), "Bucket should be empty after delete all");
        }
    }

    @Test
    void shouldMatchInt32DocumentFieldWithInt64PredicateDuringDelete() {
        // Behavior: PredicateEvaluator cross-type widening matches INT32 document field with INT64
        // predicate during full-scan DELETE (no index).
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 42}")
        );
        insertDocumentsAndGetObjectIds(testDocuments);

        // Delete with INT64 predicate matching INT32 document field
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        List<ObjectId> deletedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{\"age\": {\"$eq\": {\"$numberLong\": \"42\"}}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            deletedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, deletedObjectIds.size(), "Should have deleted 1 document");

        // Verify remaining documents: Alice and Bob
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(2, entries.size(), "Should have 2 remaining documents");

            Set<String> remainingNames = new HashSet<>();
            for (BsonDocument doc : entries) {
                remainingNames.add(BsonHelper.getString(doc, "name"));
            }
            assertEquals(Set.of("Alice", "Bob"), remainingNames);

            for (BsonDocument doc : entries) {
                int age = BsonHelper.getInteger(doc, "age");
                assertTrue(age == 25 || age == 35,
                        "Remaining document should have age 25 or 35, but was: " + age);
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
    void shouldDropVectorIndexEntriesOnDelete() {
        // Behavior: Deleting documents with vector fields removes the corresponding vector index entries from FDB.
        BucketMetadata metadata = createBucketWithVectorIndex();

        insertIntoVectorBucket(
                BSONUtil.jsonToDocumentThenBytes("{\"age\": 25, \"embedding\": [0.1, 0.2, 0.3]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"age\": 35, \"embedding\": [0.4, 0.5, 0.6]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"age\": 45, \"embedding\": [0.7, 0.8, 0.9]}")
        );

        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
        assertEquals(3, fetchAllIndexedEntries(vectorIndex.subspace()).size(), "Should have 3 vector index entries before delete");

        // Delete docs where age > 30 (should match 2 docs)
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.delete(VECTOR_BUCKET, "{\"age\": {\"$gt\": 30}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<ObjectId> deletedIds = extractObjectIds(msg);
        assertEquals(2, deletedIds.size(), "Should have deleted 2 documents");

        List<KeyValue> remaining = fetchAllIndexedEntries(vectorIndex.subspace());
        assertEquals(1, remaining.size(), "Should have 1 vector index entry remaining");
    }

    @Test
    void shouldDropVectorIndexEntriesAndUpdateCardinality() {
        // Behavior: Deleting all documents with vector fields decrements vector index cardinality to zero.
        BucketMetadata metadata = createBucketWithVectorIndex();

        insertIntoVectorBucket(
                BSONUtil.jsonToDocumentThenBytes("{\"label\": \"a\", \"embedding\": [0.1, 0.2, 0.3]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"label\": \"b\", \"embedding\": [0.4, 0.5, 0.6]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"label\": \"c\", \"embedding\": [0.7, 0.8, 0.9]}")
        );

        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics stats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), vectorIndex.definition().id());
            assertEquals(3L, stats.cardinality(), "Cardinality should be 3 before delete");
        }

        // Delete all docs
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.delete(VECTOR_BUCKET, "{}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<ObjectId> deletedIds = extractObjectIds(msg);
        assertEquals(3, deletedIds.size(), "Should have deleted all 3 documents");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics stats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), vectorIndex.definition().id());
            assertEquals(0L, stats.cardinality(), "Cardinality should be 0 after deleting all documents");
        }
    }

    @Test
    void shouldNotAffectVectorIndexWhenDeletedDocHasNoVectorField() {
        // Behavior: Deleting a document that has no vector field leaves the vector index entries intact.
        BucketMetadata metadata = createBucketWithVectorIndex();

        insertIntoVectorBucket(
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"with-vector\", \"embedding\": [0.1, 0.2, 0.3]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"no-vector\"}")
        );

        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
        assertEquals(1, fetchAllIndexedEntries(vectorIndex.subspace()).size(), "Should have 1 vector index entry");

        // Delete the doc without embedding
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.delete(VECTOR_BUCKET, "{\"type\": \"no-vector\"}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        List<ObjectId> deletedIds = extractObjectIds(msg);
        assertEquals(1, deletedIds.size(), "Should have deleted 1 document");

        assertEquals(1, fetchAllIndexedEntries(vectorIndex.subspace()).size(), "Vector index entry should still exist");
    }

    // --- Collation tests ---

    @Test
    void shouldDeleteWithTurkishCollation() {
        // Behavior: DELETE with Turkish PRIMARY collation matches i and İ as equivalent.
        insertDocumentsAndGetObjectIds(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"istanbul\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"\u0130stanbul\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"ankara\"}")
        ));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.delete(TEST_BUCKET, "{\"name\": {\"$eq\": \"\u0130stanbul\"}}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"tr\", \"strength\": 1}")).encode(buf);
        Object msg = runCommand(channel, buf);

        List<ObjectId> deletedIds = extractObjectIds(msg);
        assertEquals(2, deletedIds.size());

        ByteBuf queryBuf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(queryBuf);
        Object queryMsg = runCommand(channel, queryBuf);
        List<BsonDocument> remaining = extractEntries(queryMsg);
        assertEquals(1, remaining.size());
        assertEquals("ankara", BsonHelper.getString(remaining.getFirst(), "name"));
    }

    @Test
    void shouldDeleteWithFrenchSecondaryCollation() {
        // Behavior: DELETE with French SECONDARY collation is case-insensitive but accent-sensitive.
        insertDocumentsAndGetObjectIds(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"cafe\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Cafe\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"caf\u00e9\"}")
        ));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.delete(TEST_BUCKET, "{\"name\": {\"$eq\": \"cafe\"}}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"fr\", \"strength\": 2}")).encode(buf);
        Object msg = runCommand(channel, buf);

        List<ObjectId> deletedIds = extractObjectIds(msg);
        assertEquals(2, deletedIds.size());

        ByteBuf queryBuf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(queryBuf);
        Object queryMsg = runCommand(channel, queryBuf);
        List<BsonDocument> remaining = extractEntries(queryMsg);
        assertEquals(1, remaining.size());
        assertEquals("caf\u00e9", BsonHelper.getString(remaining.getFirst(), "name"));
    }

    @Test
    void shouldDeleteWithCollationOverridingBucket() {
        // Behavior: Query-level collation overrides bucket-level collation for DELETE.
        createBucketWithCollation(COLLATION_BUCKET, "{\"locale\": \"en\", \"strength\": 1}");

        insertDocumentsIntoBucket(COLLATION_BUCKET, List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"cafe\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Cafe\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"caf\u00e9\"}")
        ));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.delete(COLLATION_BUCKET, "{\"name\": {\"$eq\": \"cafe\"}}",
                BucketQueryArgs.Builder.collation("{\"locale\": \"fr\", \"strength\": 3}")).encode(buf);
        Object msg = runCommand(channel, buf);

        List<ObjectId> deletedIds = extractObjectIds(msg);
        assertEquals(1, deletedIds.size());

        ByteBuf queryBuf = Unpooled.buffer();
        cmd.query(COLLATION_BUCKET, "{}").encode(queryBuf);
        Object queryMsg = runCommand(channel, queryBuf);
        List<BsonDocument> remaining = extractEntries(queryMsg);
        assertEquals(2, remaining.size());
    }

    @Test
    void shouldDeleteWithNoCollationPreservingBehavior() {
        // Behavior: DELETE without COLLATION uses binary comparison (exact match only).
        insertDocumentsAndGetObjectIds(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"istanbul\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"\u0130stanbul\"}")
        ));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.delete(TEST_BUCKET, "{\"name\": {\"$eq\": \"\u0130stanbul\"}}").encode(buf);
        Object msg = runCommand(channel, buf);

        List<ObjectId> deletedIds = extractObjectIds(msg);
        assertEquals(1, deletedIds.size());

        ByteBuf queryBuf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(queryBuf);
        Object queryMsg = runCommand(channel, queryBuf);
        List<BsonDocument> remaining = extractEntries(queryMsg);
        assertEquals(1, remaining.size());
        assertEquals("istanbul", BsonHelper.getString(remaining.getFirst(), "name"));
    }
}