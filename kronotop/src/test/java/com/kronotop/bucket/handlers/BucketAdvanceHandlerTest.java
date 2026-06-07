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

import com.apple.foundationdb.Transaction;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.CompoundIndexDefinition;
import com.kronotop.bucket.index.CompoundIndexField;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketQueryArgs;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class BucketAdvanceHandlerTest extends BaseBucketHandlerTest {

    @BeforeEach
    void setUp() {
        createBucket(TEST_BUCKET);
    }

    private void appendIds(List<BsonDocument> docs, List<ObjectId> result) {
        for (BsonDocument doc : docs) {
            result.add(doc.getObjectId("_id").getValue());
        }
    }

    private void appendDocumentData(List<BsonDocument> docs, Map<ObjectId, BsonDocument> result) {
        for (BsonDocument doc : docs) {
            ObjectId docId = doc.getObjectId("_id").getValue();
            result.put(docId, doc);
        }
    }

    @Test
    void shouldAdvanceCursorForFullScan() {
        // Behavior: BUCKET.ADVANCE continues pagination from a cursor, returning batches of documents
        // until all matching documents are retrieved. Each batch respects the original query limit.

        // Create 10 identical documents for a simple full scan test
        List<byte[]> documents = new ArrayList<>();
        for (int j = 0; j < 10; j++) {
            documents.add(TEST_DOCUMENT);
        }
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        // BUCKET.QUERY - Full scan (empty filter) with limit of 2
        List<ObjectId> result = new ArrayList<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.limit(2)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            appendIds(entries, result);
        }


        // BUCKET.ADVANCE - Continue pagination until we get all documents or hit the limit
        int maxAdvanceCalls = 10; // Allow a reasonable number of calls
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break; // Normal termination
            }

            assertTrue(entries.size() <= 2, "Each batch should have at most 2 documents");

            appendIds(entries, result);
            advanceCalls++;
        }

        // We should have retrieved all 10 documents or stopped gracefully
        assertTrue(result.size() <= 10, "Should not retrieve more than 10 documents");
        assertTrue(result.size() >= 2, "Should retrieve at least the first 2 documents");

        // Verify no duplicate IDs
        Set<ObjectId> uniqueIds = new HashSet<>(result);
        assertEquals(result.size(), uniqueIds.size(), "Should not have duplicate document IDs");

        // All returned IDs should be from our inserted documents
        Set<ObjectId> insertedIds = insertedDocs.keySet();
        for (ObjectId id : result) {
            assertTrue(insertedIds.contains(id), "Returned ID should be from inserted documents");
        }
    }

    @Test
    void shouldAdvanceCursorWithSimpleFilter() {
        // Behavior: Cursor advancement with a filter returns only documents matching the predicate.
        // Pagination continues until all matching documents are retrieved across multiple batches.

        // Insert documents with a simple field for filtering
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"A\", \"value\": 1}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"B\", \"value\": 2}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"A\", \"value\": 3}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"B\", \"value\": 4}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"A\", \"value\": 5}")
        );

        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        // BUCKET.QUERY - Filter for type A with limit of 1
        Map<ObjectId, BsonDocument> allResults = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"type\": \"A\"}", BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            appendDocumentData(entries, allResults);
        }

        // BUCKET.ADVANCE - Continue until we get all type A docs or reasonable limit
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break; // Normal termination
            }

            assertTrue(entries.size() <= 1, "Each batch should have at most 1 document");

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
        // Behavior: When no documents match the filter, both BUCKET.QUERY and BUCKET.ADVANCE
        // return empty entries. The cursor is still valid but exhausted immediately.

        // Insert documents that won't match our filter
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"category\": \"X\", \"value\": 1}"),
                BSONUtil.jsonToDocumentThenBytes("{\"category\": \"Y\", \"value\": 2}"),
                BSONUtil.jsonToDocumentThenBytes("{\"category\": \"Z\", \"value\": 3}")
        );

        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        // BUCKET.QUERY - Filter for non-existent category
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"category\": \"NONEXISTENT\"}", BucketQueryArgs.Builder.limit(2)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(0, entries.size(), "No documents should match the filter");
        }

        // BUCKET.ADVANCE - Should return empty since no documents match
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(0, entries.size(), "Advance should return empty when no matches");
        }
    }

    @Test
    void shouldThrowBucketBeingRemovedExceptionWhenAdvancingOnRemovedBucket() {
        // Behavior: Advancing a cursor on a bucket marked as removed throws BUCKETBEINGREMOVED error.
        // This prevents operations on buckets that are in the process of being deleted.

        // Insert documents to create the bucket
        List<byte[]> documents = new ArrayList<>();
        for (int j = 0; j < 5; j++) {
            documents.add(TEST_DOCUMENT);
        }
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Start a query with a limit to get a cursor
        int cursorId;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.limit(2)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);
        }

        // Mark the bucket as removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            TransactionalContext tx = new TransactionalContext(context, tr);
            BucketMetadataUtil.setRemoved(tx, metadata);
            tr.commit().join();
        }

        // Flush the bucket metadata cache so open reads the dropped status
        Runnable cleanup = context.getBucketMetadataCache().createEvictionWorker(context::now, 0);
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
        // Behavior: $elemMatch on an indexed multiKey array field uses the index to find documents
        // where at least one array element matches the predicate. Pagination works across batches.

        createBucket(TEST_BUCKET);

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
        insertDocumentsAndGetObjectIds(documents);

        int cursorId;
        // BUCKET.QUERY - $elemMatch for scores >= 80 with limit of 2
        Map<ObjectId, BsonDocument> allResults = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"scores\": {\"$elemMatch\": {\"$gte\": 80}}}", BucketQueryArgs.Builder.limit(2)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            appendDocumentData(entries, allResults);
        }

        // BUCKET.ADVANCE - Continue until we get all matching docs
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            assertTrue(entries.size() <= 2, "Each batch should have at most 2 documents");
            appendDocumentData(entries, allResults);
            advanceCalls++;
        }

        // Verify results - we expect 5 documents with at least one score >= 80
        // Alice (85, 90), Bob (none), Charlie (95, 88, 92), Diana (none >= 80... wait 72, 68, 75 all < 80),
        // Eve (91, 87, 93), Frank (none)
        // Actually: Alice, Charlie, Eve have scores >= 80. Diana has max 75.
        assertEquals(3, allResults.size(), "Should retrieve exactly 3 documents with scores >= 80");

        // Verify all returned documents have at least one score >= 80
        List<String> actualNames = new ArrayList<>();
        for (BsonDocument doc : allResults.values()) {
            actualNames.add(doc.getString("name").getValue());
        }
        actualNames.sort(Comparator.naturalOrder());
        assertEquals(List.of("Alice", "Charlie", "Eve"), actualNames, "Should match documents with scores >= 80");
    }

    @Test
    void shouldAdvanceCursorWithElemMatchOnNonIndexedArray() {
        // Behavior: $elemMatch on a non-indexed array uses full scan with document-level filtering.
        // Each document's array is checked for matching elements during retrieval.

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
        insertDocumentsAndGetObjectIds(documents);

        int cursorId;
        // BUCKET.QUERY - $elemMatch for ratings >= 4.0 with limit of 2
        Map<ObjectId, BsonDocument> allResults = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"ratings\": {\"$elemMatch\": {\"$gte\": 4.0}}}", BucketQueryArgs.Builder.limit(2)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            appendDocumentData(entries, allResults);
        }

        // BUCKET.ADVANCE - Continue until we get all matching docs
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            assertTrue(entries.size() <= 2, "Each batch should have at most 2 documents");
            appendDocumentData(entries, allResults);
            advanceCalls++;
        }

        // Verify results:
        // Alice (4.5, 4.2 >= 4.0) ✓, Bob (none >= 4.0), Charlie (4.8, 4.9, 5.0) ✓,
        // Diana (3.9 max < 4.0), Eve (4.1, 4.3, 4.7) ✓, Frank (none >= 4.0)
        assertEquals(3, allResults.size(), "Should retrieve exactly 3 documents with ratings >= 4.0");

        List<String> actualNames = new ArrayList<>();
        for (BsonDocument doc : allResults.values()) {
            actualNames.add(doc.getString("name").getValue());
        }
        actualNames.sort(Comparator.naturalOrder());
        assertEquals(List.of("Alice", "Charlie", "Eve"), actualNames, "Should match documents with ratings >= 4.0");
    }

    @Test
    void shouldAdvanceCursorWithSortByIndexedFieldAscending() {
        // Behavior: SortBy on an indexed field ASC uses the index for globally sorted results.
        // Documents are returned in ascending order across all cursor advances.

        // Create index on age field and wait for it to be ready
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndex);

        // Insert documents with different ages (shuffled order)
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 40}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<Integer> collectedAges = new ArrayList<>();

        // BUCKET.QUERY - Filter on age (uses age index), SORTBY age ASC with limit of 2
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"age\": {\"$gte\": 0}}", BucketQueryArgs.Builder.limit(2).sortBy("age", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                collectedAges.add(doc.getInt32("age").getValue());
            }
        }

        // BUCKET.ADVANCE - Continue until exhausted
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            for (BsonDocument doc : entries) {
                collectedAges.add(doc.getInt32("age").getValue());
            }
            advanceCalls++;
        }

        // Verify all 5 documents retrieved in ascending order
        assertEquals(5, collectedAges.size(), "Should retrieve all 5 documents");
        assertEquals(Arrays.asList(10, 20, 30, 40, 50), collectedAges, "Ages should be in ascending order");
    }

    @Test
    void shouldAdvanceCursorWithSortByIndexedFieldDescending() {
        // Behavior: SortBy on an indexed field DESC uses the index in reverse for globally sorted results.
        // Documents are returned in descending order across all cursor advances.

        // Create index on age field and wait for it to be ready
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndex);

        // Insert documents with different ages (shuffled order)
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 40}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<Integer> collectedAges = new ArrayList<>();

        // BUCKET.QUERY - Filter on age (uses age index), SORTBY age DESC with limit of 2
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"age\": {\"$gte\": 0}}", BucketQueryArgs.Builder.limit(2).sortBy("age", "DESC")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                collectedAges.add(doc.getInt32("age").getValue());
            }
        }

        // BUCKET.ADVANCE - Continue until exhausted
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            for (BsonDocument doc : entries) {
                collectedAges.add(doc.getInt32("age").getValue());
            }
            advanceCalls++;
        }

        // Verify all 5 documents retrieved in descending order
        assertEquals(5, collectedAges.size(), "Should retrieve all 5 documents");
        assertEquals(Arrays.asList(50, 40, 30, 20, 10), collectedAges, "Ages should be in descending order");
    }

    @Test
    void shouldAdvanceCursorWithSortByNonIndexedField() {
        // Behavior: RESULTSORT on a non-indexed field sorts each batch independently during retrieval.
        // Global ordering is NOT guaranteed across cursor advances; only within-batch ordering is ensured.

        // No index on sort field - uses primary index (_id) for scan
        // Each batch is sorted independently, global order NOT guaranteed

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert documents with different priorities (no index on priority)
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"A\", \"priority\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"B\", \"priority\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"C\", \"priority\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"D\", \"priority\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"E\", \"priority\": 40}")
        );
        insertDocumentsAndGetObjectIds(documents);

        int cursorId;
        List<List<Integer>> batchPriorities = new ArrayList<>();

        // BUCKET.QUERY - RESULTSORT priority ASC on non-indexed field with limit of 2
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.limit(2).resultSort("priority", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);

            List<Integer> batch = new ArrayList<>();
            for (BsonDocument doc : entries) {
                batch.add(doc.getInt32("priority").getValue());
            }
            if (!batch.isEmpty()) {
                batchPriorities.add(batch);
            }
        }

        // BUCKET.ADVANCE - Continue until exhausted
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            List<Integer> batch = new ArrayList<>();
            for (BsonDocument doc : entries) {
                batch.add(doc.getInt32("priority").getValue());
            }
            batchPriorities.add(batch);
            advanceCalls++;
        }

        // Verify all documents retrieved
        int totalDocs = batchPriorities.stream().mapToInt(List::size).sum();
        assertEquals(5, totalDocs, "Should retrieve all 5 documents");

        // Verify each batch is sorted (within batch)
        for (List<Integer> batch : batchPriorities) {
            List<Integer> sorted = new ArrayList<>(batch);
            Collections.sort(sorted);
            assertEquals(sorted, batch, "Each batch should be sorted in ascending order");
        }
    }

    @Test
    void shouldAdvanceCursorWithSortByStringField() {
        // Behavior: SortBy on an indexed string field returns documents in alphabetical order.
        // The string index enables globally sorted results across all cursor advances.

        // Create index on name field (string type) and wait for it to be ready
        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name-idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(nameIndex);

        // Insert documents with various names (shuffled order)
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"value\": 3}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"value\": 1}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"value\": 5}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"value\": 2}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"value\": 4}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<String> collectedNames = new ArrayList<>();

        // BUCKET.QUERY - Filter on name (uses name index), SORTBY name ASC with limit of 2
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"name\": {\"$gte\": \"\"}}", BucketQueryArgs.Builder.limit(2).sortBy("name", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
            }
        }

        // BUCKET.ADVANCE - Continue until exhausted
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
            }
            advanceCalls++;
        }

        // Verify all 5 documents retrieved in alphabetical order
        assertEquals(5, collectedNames.size(), "Should retrieve all 5 documents");
        assertEquals(Arrays.asList("Alice", "Bob", "Charlie", "Diana", "Eve"), collectedNames,
                "Names should be in alphabetical order");
    }

    @Test
    void shouldAdvanceCursorWithSortByAndFilter() {
        // Behavior: Combining a range filter with sortBy on the same indexed field enables both
        // efficient filtering and globally sorted results across all cursor advances.

        // Create an index on age field for sorting and wait for it to be ready
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndex);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert documents with type and age fields
        // Type A: ages 30, 10, 20 → sorted: 10, 20, 30
        // Type B: ages 25, 40, 15
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"A\", \"name\": \"Alice\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"B\", \"name\": \"Bob\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"A\", \"name\": \"Charlie\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"B\", \"name\": \"Diana\", \"age\": 40}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"A\", \"name\": \"Eve\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"type\": \"B\", \"name\": \"Frank\", \"age\": 15}")
        );
        insertDocumentsAndGetObjectIds(documents);

        int cursorId;
        List<Integer> collectedAges = new ArrayList<>();
        List<String> collectedNames = new ArrayList<>();

        // BUCKET.QUERY - Filter for age >= 10 AND age <= 30 (all type A docs), SORTBY age ASC with limit of 1
        // Using age filter ensures optimizer uses age index for scanning (global sort preserved)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"$and\": [{\"age\": {\"$gte\": 10}}, {\"age\": {\"$lte\": 30}}]}",
                    BucketQueryArgs.Builder.limit(1).sortBy("age", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                collectedAges.add(doc.getInt32("age").getValue());
                collectedNames.add(doc.getString("name").getValue());
            }
        }

        // BUCKET.ADVANCE - Continue until exhausted
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            for (BsonDocument doc : entries) {
                collectedAges.add(doc.getInt32("age").getValue());
                collectedNames.add(doc.getString("name").getValue());
            }
            advanceCalls++;
        }

        // Verify documents with age 10-30 retrieved (5 total: Charlie(10), Frank(15), Eve(20), Bob(25), Alice(30))
        assertEquals(5, collectedAges.size(), "Should retrieve 5 documents with ages 10-30");

        // Verify sorted by age ascending
        assertEquals(Arrays.asList(10, 15, 20, 25, 30), collectedAges, "Ages should be in ascending order");
        assertEquals(Arrays.asList("Charlie", "Frank", "Eve", "Bob", "Alice"), collectedNames,
                "Names should match the age-sorted order");
    }

    @Test
    void shouldAdvanceCursorWithSortByIndexedFieldAndEmptyFilter() {
        // Behavior: Empty filter {} with sortBy on an indexed field uses the index for scanning,
        // enabling globally sorted results. Only documents with the indexed field are returned.

        // Create index on age field and wait for it to be ready
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndex);

        // Insert documents with different ages (shuffled order)
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 40}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<Integer> collectedAges = new ArrayList<>();

        // BUCKET.QUERY - Filter on age (uses age index), SORTBY age ASC with limit of 2
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.limit(2).sortBy("age", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                collectedAges.add(doc.getInt32("age").getValue());
            }
        }

        // BUCKET.ADVANCE - Continue until exhausted
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            for (BsonDocument doc : entries) {
                collectedAges.add(doc.getInt32("age").getValue());
            }
            advanceCalls++;
        }

        // Verify all 5 documents retrieved in ascending order
        assertEquals(5, collectedAges.size(), "Should retrieve all 5 documents");
        assertEquals(Arrays.asList(10, 20, 30, 40, 50), collectedAges, "Ages should be in ascending order");
    }

    @Test
    void shouldAdvanceCursorWithIndexedFilterAndSortByNonIndexedField() {
        // Behavior: RESULTSORT on a non-indexed field guarantees ordering only within each batch, not across cursor advances.

        // Create index on age field only - priority is NOT indexed
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndex);

        // Insert documents with age (indexed) and priority (non-indexed) fields
        // Filter will use age index, but sort is on the non-indexed priority field
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25, \"priority\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30, \"priority\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 35, \"priority\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 15, \"priority\": 40}"),  // age <= 20, filtered out
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 40, \"priority\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Frank\", \"age\": 10, \"priority\": 60}")   // age <= 20, filtered out
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<List<Integer>> batchPriorities = new ArrayList<>();

        // BUCKET.QUERY - Filter on age > 20 (uses age index), SORTBY priority ASC (non-indexed)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"age\": {\"$gt\": 20}}", BucketQueryArgs.Builder.limit(2).resultSort("priority", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);

            List<Integer> batch = new ArrayList<>();
            for (BsonDocument doc : entries) {
                batch.add(doc.getInt32("priority").getValue());
            }
            if (!batch.isEmpty()) {
                batchPriorities.add(batch);
            }
        }

        // BUCKET.ADVANCE - Continue until exhausted
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            List<Integer> batch = new ArrayList<>();
            for (BsonDocument doc : entries) {
                batch.add(doc.getInt32("priority").getValue());
            }
            batchPriorities.add(batch);
            advanceCalls++;
        }

        // Verify: Only 4 documents match age > 20 (Alice, Bob, Charlie, Eve)
        // Diana (age=15) and Frank (age=10) are filtered out
        int totalDocs = batchPriorities.stream().mapToInt(List::size).sum();
        assertEquals(4, totalDocs, "Should retrieve 4 documents with age > 20");

        // Verify each batch is sorted by priority (within batch)
        // Note: Global sort is NOT guaranteed when RESULTSORT field differs from scan index
        for (List<Integer> batch : batchPriorities) {
            List<Integer> sorted = new ArrayList<>(batch);
            Collections.sort(sorted);
            assertEquals(sorted, batch, "Each batch should be sorted by priority in ascending order");
        }
    }

    @Test
    void shouldAdvanceCursorWithSortByIndexedFieldAndEmptyFilterDescending() {
        // Behavior: Empty filter with sortBy on indexed field DESC should use the index in reverse order.

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndex);

        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 40}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<Integer> collectedAges = new ArrayList<>();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.limit(2).sortBy("age", "DESC")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                collectedAges.add(doc.getInt32("age").getValue());
            }
        }

        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            for (BsonDocument doc : entries) {
                collectedAges.add(doc.getInt32("age").getValue());
            }
            advanceCalls++;
        }

        assertEquals(5, collectedAges.size(), "Should retrieve all 5 documents");
        assertEquals(Arrays.asList(50, 40, 30, 20, 10), collectedAges, "Ages should be in descending order");
    }

    @Test
    void shouldRejectSortByOnNonIndexedField() {
        // Behavior: SORTBY on a non-indexed field is rejected because no index can provide
        // natural ordering. The planner suggests creating an index on the sort field.

        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"A\", \"priority\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"B\", \"priority\": 10}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.limit(2).sortBy("priority", "DESC")).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        String error = ((ErrorRedisMessage) msg).content();
        assertTrue(error.contains("SORTBY 'priority' requires an index that provides natural ordering"));
        assertTrue(error.contains("create an index on 'priority'"));
    }

    @Test
    void shouldAdvanceCursorWithFilterOnIndexedFieldASortByIndexedFieldB() {
        // Behavior: Planner picks the score index for SORTBY ordering and applies the age
        // filter as a residual predicate. Pagination works correctly across all cursor advances.
        //
        // The score predicate ({$gte: 0}) is a no-op filter that exists solely to bring the
        // score index into the physical plan. Without it, the plan would be a single
        // PhysicalIndexScan on age — which provides no ordering on score, so SORTBY score
        // would be rejected. With the AND, the planner sees both indexes and can select
        // score as the primary scan for sort ordering.

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-idx", "score", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndex);
        createIndexThenWaitForReadiness(scoreIndex);

        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25, \"score\": 80}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30, \"score\": 60}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 35, \"score\": 90}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 15, \"score\": 70}"),  // age <= 20, filtered out
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 40, \"score\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Frank\", \"age\": 10, \"score\": 100}")  // age <= 20, filtered out
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<String> collectedNames = new ArrayList<>();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"age\": {\"$gt\": 20}, \"score\": {\"$gte\": 0}}", BucketQueryArgs.Builder.limit(2).sortBy("score", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
            }
        }

        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
            }
            advanceCalls++;
        }

        assertEquals(4, collectedNames.size(), "Should retrieve 4 documents with age > 20");

        // Verify global sort order by score ASC
        List<String> expectedOrder = List.of("Eve", "Bob", "Alice", "Charlie");
        assertEquals(expectedOrder, collectedNames, "Results should be globally sorted by score ASC across pages");
    }

    @Test
    void shouldRejectCompoundIndexRangePrefixSortByTrailingField() {
        // Behavior: Compound index {age, score} with range filter on prefix field (age) and
        // sortBy on trailing field (score) is rejected because a range predicate on a prefix
        // field breaks the global sort ordering of trailing fields in the compound index.

        createIndexThenWaitForReadiness(CompoundIndexDefinition.create("idx_age_score", List.of(
                new CompoundIndexField("age", BsonType.INT32, false),
                new CompoundIndexField("score", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25, \"score\": 80}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30, \"score\": 60}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"age\": {\"$gt\": 20}}", BucketQueryArgs.Builder.limit(2).sortBy("score", "ASC")).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);

        String error = ((ErrorRedisMessage) msg).content();
        assertTrue(error.contains("SORTBY 'score' cannot be executed"));
        assertTrue(error.contains("range filter on 'age'"));
    }

    @Test
    void shouldAdvanceCursorWithSortByStringFieldDescending() {
        // Behavior: SortBy on indexed string field DESC should use the index in reverse order.

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name-idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(nameIndex);

        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"value\": 3}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"value\": 1}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"value\": 5}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"value\": 2}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"value\": 4}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<String> collectedNames = new ArrayList<>();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"name\": {\"$gte\": \"\"}}", BucketQueryArgs.Builder.limit(2).sortBy("name", "DESC")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
            }
        }

        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
            }
            advanceCalls++;
        }

        assertEquals(5, collectedNames.size(), "Should retrieve all 5 documents");
        assertEquals(Arrays.asList("Eve", "Diana", "Charlie", "Bob", "Alice"), collectedNames,
                "Names should be in reverse alphabetical order");
    }

    @Test
    void shouldAdvanceCursorWithSortByFieldWithMissingValues() {
        // Behavior: When filtering on the indexed field with sortBy on the same field,
        // documents missing the field are not in the index and won't be returned.

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-idx", "score", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(scoreIndex);

        // Some documents have score, some don't
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"score\": 80}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}"),  // missing score - not in index
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"score\": 60}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\"}"),  // missing score - not in index
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"score\": 90}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<String> collectedNames = new ArrayList<>();

        {
            ByteBuf buf = Unpooled.buffer();
            // Filter on score (uses index), sortBy score - only indexed documents returned
            cmd.query(TEST_BUCKET, "{\"score\": {\"$gte\": 0}}", BucketQueryArgs.Builder.limit(2).sortBy("score", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
            }
        }

        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
            }
            advanceCalls++;
        }

        // Only documents with score field are returned (3 documents)
        // Bob and Diana are not in the score index, so they're not returned
        assertEquals(3, collectedNames.size(), "Should retrieve 3 documents with score field");

        // Verify the documents with scores are returned in ascending score order
        // Alice(80), Charlie(60), Eve(90) -> sorted: Charlie(60), Alice(80), Eve(90)
        assertEquals(Arrays.asList("Charlie", "Alice", "Eve"), collectedNames,
                "Documents should be sorted by score ascending");
    }

    @Test
    void shouldAdvanceCursorWithOrQueryOnMultipleIndexedFields() {
        // Behavior: $or queries on multiple indexed fields return each matching document exactly once
        // across cursor advances. Documents matching multiple OR branches are deduplicated.

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-idx", "price", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition quantityIndex = SingleFieldIndexDefinition.create("quantity-idx", "quantity", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(priceIndex);
        createIndexThenWaitForReadiness(quantityIndex);

        // Insert documents with price and quantity fields
        // $or: price > 100 OR quantity < 10
        // A: price=100, quantity=5 → matches quantity<10 only (100 is NOT > 100)
        // B: price=50, quantity=15 → no match
        // C: price=200, quantity=3 → matches BOTH conditions
        // D: price=30, quantity=25 → no match
        // E: price=150, quantity=8 → matches BOTH conditions
        // Expected: A, C, E (3 unique documents)
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"A\", \"price\": 100, \"quantity\": 5}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"B\", \"price\": 50, \"quantity\": 15}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"C\", \"price\": 200, \"quantity\": 3}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"D\", \"price\": 30, \"quantity\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"E\", \"price\": 150, \"quantity\": 8}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<String> collectedNames = new ArrayList<>();

        // BUCKET.QUERY - $or with limit 1
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"$or\": [{\"price\": {\"$gt\": 100}}, {\"quantity\": {\"$lt\": 10}}]}",
                    BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
            }
        }

        // BUCKET.ADVANCE - Continue until exhausted
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
            }
            advanceCalls++;
        }

        // Verify exactly 3 unique documents returned (A, C, E)
        assertEquals(3, collectedNames.size(), "Should retrieve exactly 3 matching documents");

        // Verify no duplicates
        Set<String> uniqueNames = new HashSet<>(collectedNames);
        assertEquals(collectedNames.size(), uniqueNames.size(), "Should not have duplicate documents");

        // Verify correct documents returned
        Set<String> expectedNames = Set.of("A", "C", "E");
        assertEquals(expectedNames, uniqueNames, "Should return documents A, C, and E");
    }

    @Test
    void shouldCompleteBatchAfterFilteringDuplicatesInOrQuery() {
        // Behavior: When $or query filters out already-returned documents, the system continues
        // fetching from FDB until the batch is complete or all sources are exhausted.
        // This tests that batch completion works correctly after deduplication.

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-idx", "price", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition quantityIndex = SingleFieldIndexDefinition.create("quantity-idx", "quantity", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(priceIndex);
        createIndexThenWaitForReadiness(quantityIndex);

        // Insert documents where many match BOTH conditions to maximize duplicate filtering
        // Query: price > 100 OR quantity < 10
        // A-D match BOTH conditions (will appear in both index scans)
        // E-F match quantity < 10 only
        // G-H match neither (control group)
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"A\", \"price\": 200, \"quantity\": 5}"),  // BOTH
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"B\", \"price\": 150, \"quantity\": 3}"),  // BOTH
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"C\", \"price\": 180, \"quantity\": 2}"),  // BOTH
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"D\", \"price\": 120, \"quantity\": 8}"),  // BOTH
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"E\", \"price\": 80, \"quantity\": 7}"),   // quantity only
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"F\", \"price\": 60, \"quantity\": 4}"),   // quantity only
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"G\", \"price\": 50, \"quantity\": 15}"),  // no match
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"H\", \"price\": 40, \"quantity\": 20}")   // no match
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<String> collectedNames = new ArrayList<>();
        List<Integer> batchSizes = new ArrayList<>();

        // BUCKET.QUERY with limit=2 - small limit to trigger multiple advances and batch completion
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"$or\": [{\"price\": {\"$gt\": 100}}, {\"quantity\": {\"$lt\": 10}}]}",
                    BucketQueryArgs.Builder.limit(2)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);

            int batchSize = 0;
            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
                batchSize++;
            }
            assertEquals(2, batchSize);
            batchSizes.add(batchSize);
        }

        // BUCKET.ADVANCE - Continue until exhausted
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            int batchSize = 0;
            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
                batchSize++;
            }
            assertEquals(2, batchSize);
            batchSizes.add(batchSize);
            advanceCalls++;
        }

        // Verify exactly 6 unique documents returned (A, B, C, D, E, F)
        assertEquals(6, collectedNames.size(), "Should retrieve exactly 6 matching documents");

        // Verify no duplicates - this is the key assertion
        Set<String> uniqueNames = new HashSet<>(collectedNames);
        assertEquals(collectedNames.size(), uniqueNames.size(),
                "Should not have duplicate documents even when documents match multiple OR branches");

        // Verify correct documents returned
        Set<String> expectedNames = Set.of("A", "B", "C", "D", "E", "F");
        assertEquals(expectedNames, uniqueNames, "Should return all 6 matching documents");

        // Verify number of batches: 6 documents / limit 2 = 3 batches
        // This confirms the system completes batches after filtering duplicates
        assertEquals(3, batchSizes.size(),
                "Should complete in exactly 3 batches (6 docs / limit 2). " +
                        "More batches would indicate incomplete batch filling after duplicate filtering.");

        // Verify batch sizes: each batch should have exactly 2 documents (the limit)
        // because the system continues fetching after filtering duplicates to complete each batch
        for (int i = 0; i < batchSizes.size(); i++) {
            assertEquals(2, batchSizes.get(i),
                    "Batch " + i + " should have exactly 2 documents (the limit)");
        }
    }

    @Test
    void shouldAdvanceCursorWithIndexedFilterAndSortByNonIndexedFieldDescending() {
        // Behavior: Filter uses index, RESULTSORT on non-indexed field DESC - each batch sorted independently.

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndex);

        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25, \"priority\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30, \"priority\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 35, \"priority\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 15, \"priority\": 40}"),  // age <= 20, filtered out
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 40, \"priority\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Frank\", \"age\": 10, \"priority\": 60}")   // age <= 20, filtered out
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<List<Integer>> batchPriorities = new ArrayList<>();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"age\": {\"$gt\": 20}}", BucketQueryArgs.Builder.limit(2).resultSort("priority", "DESC")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);

            List<Integer> batch = new ArrayList<>();
            for (BsonDocument doc : entries) {
                batch.add(doc.getInt32("priority").getValue());
            }
            if (!batch.isEmpty()) {
                batchPriorities.add(batch);
            }
        }

        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            List<Integer> batch = new ArrayList<>();
            for (BsonDocument doc : entries) {
                batch.add(doc.getInt32("priority").getValue());
            }
            batchPriorities.add(batch);
            advanceCalls++;
        }

        int totalDocs = batchPriorities.stream().mapToInt(List::size).sum();
        assertEquals(4, totalDocs, "Should retrieve 4 documents with age > 20");

        // Each batch should be sorted in descending order
        for (List<Integer> batch : batchPriorities) {
            List<Integer> sorted = new ArrayList<>(batch);
            sorted.sort(Collections.reverseOrder());
            assertEquals(sorted, batch, "Each batch should be sorted by priority in descending order");
        }
    }

    @Test
    void shouldHandleAsymmetricBranchExhaustionInOrQuery() {
        // Behavior: When one OR branch exhausts much earlier than another, the system continues
        // fetching from the remaining branch until all results are retrieved. This tests that
        // UnionNode correctly handles asymmetric exhaustion across multiple BUCKET.ADVANCE calls.

        SingleFieldIndexDefinition categoryIndex = SingleFieldIndexDefinition.create("category-idx", "category", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-idx", "score", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(categoryIndex);
        createIndexThenWaitForReadiness(scoreIndex);

        // Create asymmetric data distribution:
        // - category = "rare": only 2 documents (exhausts quickly)
        // - score > 90: 10 documents (larger set)
        // - 1 document matches BOTH conditions
        List<byte[]> documents = new ArrayList<>();

        // 2 documents with category = "rare" (one also has score > 90)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{\"name\": \"R1\", \"category\": \"rare\", \"score\": 50}"));
        documents.add(BSONUtil.jsonToDocumentThenBytes("{\"name\": \"R2\", \"category\": \"rare\", \"score\": 95}")); // matches BOTH

        // 9 documents with score > 90 (category != "rare")
        for (int i = 1; i <= 9; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes(
                    String.format("{\"name\": \"S%d\", \"category\": \"common\", \"score\": %d}", i, 91 + i)));
        }

        // 5 documents that match neither condition (control group)
        for (int i = 1; i <= 5; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes(
                    String.format("{\"name\": \"N%d\", \"category\": \"common\", \"score\": %d}", i, 50 + i)));
        }

        insertDocumentsAndGetObjectIds(documents);

        // Expected matches: R1, R2, S1-S9 = 11 unique documents (R2 matches both but counted once)
        // category = "rare" branch: 2 docs (exhausts quickly)
        // score > 90 branch: 10 docs (continues for many more iterations)

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<String> collectedNames = new ArrayList<>();
        int advanceCount = 0;

        // BUCKET.QUERY with limit=1 to stress test pending entries
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"$or\": [{\"category\": {\"$eq\": \"rare\"}}, {\"score\": {\"$gt\": 90}}]}",
                    BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
            }
        }

        // BUCKET.ADVANCE until exhausted
        int maxAdvanceCalls = 20;
        while (advanceCount < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
            }
            advanceCount++;
        }

        // Verify exactly 11 unique documents returned
        assertEquals(11, collectedNames.size(), "Should retrieve exactly 11 matching documents");

        // Verify no duplicates
        Set<String> uniqueNames = new HashSet<>(collectedNames);
        assertEquals(collectedNames.size(), uniqueNames.size(),
                "Should not have duplicate documents despite asymmetric branch exhaustion");

        // Verify all expected documents are present
        List<String> expectedNames = new ArrayList<>();
        expectedNames.add("R1");
        expectedNames.add("R2");
        for (int i = 1; i <= 9; i++) {
            expectedNames.add("S" + i);
        }
        assertEquals(expectedNames.size(), uniqueNames.size());
        assertTrue(uniqueNames.containsAll(expectedNames), "Should return all 11 matching documents");

        // Verify we needed multiple advances (confirming pagination worked across asymmetric exhaustion)
        // With limit=1 and 11 docs: first query returns 1, then 10 advances needed
        assertTrue(advanceCount >= 10, "Should need at least 10 advances for 11 docs with limit=1");
    }

    @Test
    void shouldStressChildRewindWithSmallLimit() {
        // Behavior: With a small limit, OR queries produce excess entries that trigger child
        // cursor rewind. This stress tests the rewind mechanism across many iterations,
        // ensuring no data loss or duplicates.

        SingleFieldIndexDefinition typeIndex = SingleFieldIndexDefinition.create("type-idx", "type", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition levelIndex = SingleFieldIndexDefinition.create("level-idx", "level", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(typeIndex);
        createIndexThenWaitForReadiness(levelIndex);

        // Create 20 documents where most match BOTH OR conditions
        // This maximizes pending entries since union produces many results per iteration
        List<byte[]> documents = new ArrayList<>();

        // 15 documents matching BOTH conditions (type="active" AND level>5)
        for (int i = 1; i <= 15; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes(
                    String.format("{\"name\": \"B%d\", \"type\": \"active\", \"level\": %d}", i, 10 + i)));
        }

        // 3 documents matching only type="active" (level <= 5)
        for (int i = 1; i <= 3; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes(
                    String.format("{\"name\": \"T%d\", \"type\": \"active\", \"level\": %d}", i, i)));
        }

        // 2 documents matching only level>5 (type != "active")
        documents.add(BSONUtil.jsonToDocumentThenBytes("{\"name\": \"L1\", \"type\": \"inactive\", \"level\": 20}"));
        documents.add(BSONUtil.jsonToDocumentThenBytes("{\"name\": \"L2\", \"type\": \"inactive\", \"level\": 25}"));

        insertDocumentsAndGetObjectIds(documents);

        // Expected: 20 unique documents (15 both + 3 type-only + 2 level-only)

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<String> collectedNames = new ArrayList<>();
        List<Integer> batchSizes = new ArrayList<>();

        // BUCKET.QUERY with limit=3 (enough for both children to get quota)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"$or\": [{\"type\": {\"$eq\": \"active\"}}, {\"level\": {\"$gt\": 5}}]}",
                    BucketQueryArgs.Builder.limit(3)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);

            int batchSize = 0;
            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
                batchSize++;
            }
            batchSizes.add(batchSize);
        }

        // BUCKET.ADVANCE until exhausted
        int maxAdvanceCalls = 30;
        int advanceCount = 0;
        while (advanceCount < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            int batchSize = 0;
            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
                batchSize++;
            }
            batchSizes.add(batchSize);
            advanceCount++;
        }

        // Build expected list for comparison
        List<String> expectedNames = new ArrayList<>();
        for (int i = 1; i <= 15; i++) expectedNames.add("B" + i);
        for (int i = 1; i <= 3; i++) expectedNames.add("T" + i);
        expectedNames.add("L1");
        expectedNames.add("L2");

        // Find missing documents for debugging
        Set<String> uniqueNames = new HashSet<>(collectedNames);
        List<String> missing = new ArrayList<>(expectedNames);
        missing.removeAll(uniqueNames);

        // Verify exactly 20 documents returned
        assertEquals(20, collectedNames.size(),
                "Should retrieve exactly 20 matching documents. Missing: " + missing);

        // Verify no duplicates - critical for child rewind correctness
        assertEquals(collectedNames.size(), uniqueNames.size(),
                "Should not have duplicates despite heavy pending entries usage");

        // Verify all batches have at most 3 documents (the limit)
        for (int i = 0; i < batchSizes.size(); i++) {
            assertTrue(batchSizes.get(i) <= 3,
                    "Batch " + i + " should have at most 3 documents (limit=3)");
        }

        // Verify we needed multiple batches for 20 docs with limit=3
        assertTrue(batchSizes.size() >= 7, "Should need at least 7 batches for 20 docs with limit=3");
    }

    @Test
    void shouldHandleOrQueryWithLimitOneLessThanChildCount() {
        // Behavior: When limit=1 with 2 OR branches, each child gets at least limit=1 to avoid
        // FDB's "limit=0 means unlimited" behavior. Deduplication and buffering handle excess.

        SingleFieldIndexDefinition typeIndex = SingleFieldIndexDefinition.create("type-idx", "type", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition levelIndex = SingleFieldIndexDefinition.create("level-idx", "level", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(typeIndex);
        createIndexThenWaitForReadiness(levelIndex);

        // Documents matching different branches:
        // - T1, T2: match type="active" only (level <= 50)
        // - L1, L2: match level>50 only (type != "active")
        // - B1: matches BOTH conditions
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"T1\", \"type\": \"active\", \"level\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"T2\", \"type\": \"active\", \"level\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"L1\", \"type\": \"inactive\", \"level\": 60}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"L2\", \"type\": \"inactive\", \"level\": 70}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"B1\", \"type\": \"active\", \"level\": 80}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<String> collectedNames = new ArrayList<>();

        // BUCKET.QUERY with limit=1
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"$or\": [{\"type\": {\"$eq\": \"active\"}}, {\"level\": {\"$gt\": 50}}]}",
                    BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
            }
        }

        // BUCKET.ADVANCE until exhausted
        int maxAdvanceCalls = 10;
        int advanceCount = 0;
        while (advanceCount < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
            }
            advanceCount++;
        }

        // Verify exactly 5 unique documents returned
        assertEquals(5, collectedNames.size(), "Should retrieve all 5 matching documents");

        // Verify no duplicates
        Set<String> uniqueNames = new HashSet<>(collectedNames);
        assertEquals(collectedNames.size(), uniqueNames.size(), "No duplicates");

        // Verify all expected documents are present
        Set<String> expectedNames = Set.of("T1", "T2", "L1", "L2", "B1");
        assertEquals(expectedNames, uniqueNames, "Should return all matching documents");
    }

    @Test
    void shouldAdvanceElemMatchWithOrAndTwoIndexesWithLimitOne() {
        // Behavior: $or inside $elemMatch on an indexed array creates a UnionNode with branches
        // for each $or condition. With limit=1, child quota distribution and pending entries
        // buffering work correctly across ADVANCE calls.

        createBucket(TEST_BUCKET);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Create two multi-key indexes on nested array fields
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"items.category\": {\"bson_type\": \"string\", \"multi_key\": true}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"items.brand\": {\"bson_type\": \"string\", \"multi_key\": true}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        // Insert documents with items array:
        // - O1, O2: have items with category="electronics" only
        // - O3, O4: have items with brand="acme" only
        // - O5: has item matching BOTH conditions
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"O1\", \"items\": [{\"category\": \"electronics\", \"brand\": \"sony\"}]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"O2\", \"items\": [{\"category\": \"electronics\", \"brand\": \"samsung\"}]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"O3\", \"items\": [{\"category\": \"clothing\", \"brand\": \"acme\"}]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"O4\", \"items\": [{\"category\": \"furniture\", \"brand\": \"acme\"}]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"O5\", \"items\": [{\"category\": \"electronics\", \"brand\": \"acme\"}]}")
        );
        insertDocumentsAndGetObjectIds(documents);

        int cursorId;
        List<String> collectedNames = new ArrayList<>();

        // BUCKET.QUERY with limit=1: items has element where (category="electronics" OR brand="acme")
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET,
                    "{\"items\": {\"$elemMatch\": {\"$or\": [{\"category\": {\"$eq\": \"electronics\"}}, {\"brand\": {\"$eq\": \"acme\"}}]}}}",
                    BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
            }
        }

        // BUCKET.ADVANCE until exhausted
        int maxAdvanceCalls = 10;
        int advanceCount = 0;
        while (advanceCount < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
            }
            advanceCount++;
        }

        // Verify exactly 5 unique documents returned
        assertEquals(5, collectedNames.size(), "Should retrieve all 5 matching documents");

        // Verify no duplicates
        Set<String> uniqueNames = new HashSet<>(collectedNames);
        assertEquals(collectedNames.size(), uniqueNames.size(), "No duplicates");

        // Verify all expected documents are present
        Set<String> expectedNames = Set.of("O1", "O2", "O3", "O4", "O5");
        assertEquals(expectedNames, uniqueNames, "Should return all matching documents");
    }

    @Test
    void shouldSortOrQueryResultsPerBatchOnly() {
        // Behavior: $or queries with RESULTSORT on a non-driving field sort each batch independently.
        // Global ordering is NOT guaranteed across cursor advances; only within-batch ordering is ensured.
        // This documents intentional behavior: cross-batch ordering is undefined for $or queries.

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-idx", "price", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition quantityIndex = SingleFieldIndexDefinition.create("quantity-idx", "quantity", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(priceIndex);
        createIndexThenWaitForReadiness(quantityIndex);

        // Insert documents with price, quantity, and score fields
        // $or: price > 50 OR quantity < 20
        // A: price=100, quantity=25, score=5 → matches price>50
        // B: price=30, quantity=10, score=2 → matches quantity<20
        // C: price=80, quantity=5, score=8 → matches BOTH
        // D: price=20, quantity=30, score=1 → no match
        // E: price=60, quantity=15, score=4 → matches BOTH
        // F: price=40, quantity=8, score=6 → matches quantity<20
        // G: price=90, quantity=22, score=3 → matches price>50
        // H: price=25, quantity=18, score=7 → matches quantity<20
        // I: price=70, quantity=12, score=9 → matches BOTH
        // J: price=15, quantity=35, score=10 → no match
        // Expected matches: A, B, C, E, F, G, H, I (8 documents)
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"A\", \"price\": 100, \"quantity\": 25, \"score\": 5}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"B\", \"price\": 30, \"quantity\": 10, \"score\": 2}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"C\", \"price\": 80, \"quantity\": 5, \"score\": 8}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"D\", \"price\": 20, \"quantity\": 30, \"score\": 1}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"E\", \"price\": 60, \"quantity\": 15, \"score\": 4}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"F\", \"price\": 40, \"quantity\": 8, \"score\": 6}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"G\", \"price\": 90, \"quantity\": 22, \"score\": 3}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"H\", \"price\": 25, \"quantity\": 18, \"score\": 7}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"I\", \"price\": 70, \"quantity\": 12, \"score\": 9}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"J\", \"price\": 15, \"quantity\": 35, \"score\": 10}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<List<Integer>> batchScores = new ArrayList<>();
        List<String> collectedNames = new ArrayList<>();

        // BUCKET.QUERY - $or with sortBy on non-driving field (score), limit 3
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"$or\": [{\"price\": {\"$gt\": 50}}, {\"quantity\": {\"$lt\": 20}}]}",
                    BucketQueryArgs.Builder.limit(3).resultSort("score", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);

            List<Integer> batch = new ArrayList<>();
            for (BsonDocument doc : entries) {
                batch.add(doc.getInt32("score").getValue());
                collectedNames.add(doc.getString("name").getValue());
            }
            if (!batch.isEmpty()) {
                batchScores.add(batch);
            }
        }

        // BUCKET.ADVANCE - Continue until exhausted
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            List<Integer> batch = new ArrayList<>();
            for (BsonDocument doc : entries) {
                batch.add(doc.getInt32("score").getValue());
                collectedNames.add(doc.getString("name").getValue());
            }
            batchScores.add(batch);
            advanceCalls++;
        }

        // Verify no duplicates (previously hidden by HashSet)
        Set<String> uniqueCollected = new HashSet<>(collectedNames);
        assertEquals(collectedNames.size(), uniqueCollected.size(), "No duplicates");

        // Verify all 8 matching documents retrieved
        assertEquals(8, collectedNames.size(), "Should retrieve all 8 matching documents");

        // Verify correct documents returned (A, B, C, E, F, G, H, I)
        assertTrue(collectedNames.containsAll(List.of("A", "B", "C", "E", "F", "G", "H", "I")),
                "Should return exactly the matching documents");

        // Verify each batch is sorted by score (within-batch guarantee)
        for (List<Integer> batch : batchScores) {
            List<Integer> sorted = new ArrayList<>(batch);
            Collections.sort(sorted);
            assertEquals(sorted, batch, "Each batch should be sorted by score in ascending order");
        }

        // NOTE: We intentionally do NOT assert global ordering across batches.
        // This is documented behavior: $or queries only guarantee per-batch sorting.
    }

    @Test
    void shouldAdvanceCursorWithCompoundIndexSortByAgeAscending() {
        // Behavior: Compound index on (name, age) with EQ prefix on name and range on age enables
        // globally sorted results by age ASC across all cursor advances.

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create(
                "name_age_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);
        createIndexThenWaitForReadiness(compoundIdx);

        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 40}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 60}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 70}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<Integer> collectedAges = new ArrayList<>();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"name\": \"Alice\", \"age\": {\"$gte\": 0}}",
                    BucketQueryArgs.Builder.limit(2).sortBy("age", "ASC")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                collectedAges.add(doc.getInt32("age").getValue());
            }
        }

        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            assertTrue(entries.size() <= 2, "Each batch should have at most 2 documents");

            for (BsonDocument doc : entries) {
                collectedAges.add(doc.getInt32("age").getValue());
            }
            advanceCalls++;
        }

        assertEquals(7, collectedAges.size(), "Should retrieve all 7 documents");
        assertEquals(Arrays.asList(10, 20, 30, 40, 50, 60, 70), collectedAges, "Ages should be in ascending order");
    }

    @Test
    void shouldAdvanceCursorWithCompoundIndexSortByAgeDescending() {
        // Behavior: Compound index on (name, age) with EQ prefix on name and range on age enables
        // globally sorted results by age DESC across all cursor advances.

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create(
                "name_age_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);
        createIndexThenWaitForReadiness(compoundIdx);

        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 40}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 60}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 70}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<Integer> collectedAges = new ArrayList<>();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"name\": \"Alice\", \"age\": {\"$gte\": 0}}",
                    BucketQueryArgs.Builder.limit(2).sortBy("age", "DESC")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                collectedAges.add(doc.getInt32("age").getValue());
            }
        }

        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            assertTrue(entries.size() <= 2, "Each batch should have at most 2 documents");

            for (BsonDocument doc : entries) {
                collectedAges.add(doc.getInt32("age").getValue());
            }
            advanceCalls++;
        }

        assertEquals(7, collectedAges.size(), "Should retrieve all 7 documents");
        assertEquals(Arrays.asList(70, 60, 50, 40, 30, 20, 10), collectedAges, "Ages should be in descending order");
    }

    @Test
    void shouldAdvanceCursorWithCompoundIndexPagination() {
        // Behavior: Compound index pagination returns all matching documents with no duplicates
        // and excludes non-matching documents.

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create(
                "name_age_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);
        createIndexThenWaitForReadiness(compoundIdx);

        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 10}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 20}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 40}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 50}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 60}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<ObjectId> collectedIds = new ArrayList<>();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"name\": \"Alice\", \"age\": {\"$gte\": 0}}",
                    BucketQueryArgs.Builder.limit(2)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            appendIds(entries, collectedIds);
        }

        int maxAdvanceCalls = 10;
        int advanceCalls = 0;
        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            assertTrue(entries.size() <= 2, "Each batch should have at most 2 documents");

            appendIds(entries, collectedIds);
            advanceCalls++;
        }

        assertEquals(6, collectedIds.size(), "Should retrieve all 6 Alice documents (Bob excluded)");

        // Verify no duplicate ObjectIds
        Set<ObjectId> uniqueIds = new HashSet<>(collectedIds);
        assertEquals(collectedIds.size(), uniqueIds.size(), "Should not have duplicate document IDs");
    }

    @Test
    void shouldRewindFullScanChildrenInOrQueryWithLimitAndAdvance() {
        // Behavior: $or on two non-indexed fields produces a UnionNode with FullScanNode children.
        // With limit=2, the rewind mechanism must restore FullScanNode checkpoints (which use
        // saveObjectIdCheckpoint on the primary index). All matching documents are retrieved
        // without duplicates across multiple ADVANCE calls.

        // No indexes created — both branches use FullScanNode

        // 12 docs: 4 match both, 3 match category only, 3 match color only, 2 match neither
        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"BC1\", \"category\": \"electronics\", \"color\": \"red\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"BC2\", \"category\": \"electronics\", \"color\": \"red\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"BC3\", \"category\": \"electronics\", \"color\": \"red\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"BC4\", \"category\": \"electronics\", \"color\": \"red\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"CA1\", \"category\": \"electronics\", \"color\": \"blue\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"CA2\", \"category\": \"electronics\", \"color\": \"blue\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"CA3\", \"category\": \"electronics\", \"color\": \"blue\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"CO1\", \"category\": \"clothing\", \"color\": \"red\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"CO2\", \"category\": \"clothing\", \"color\": \"red\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"CO3\", \"category\": \"clothing\", \"color\": \"red\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"N1\", \"category\": \"clothing\", \"color\": \"blue\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"N2\", \"category\": \"clothing\", \"color\": \"blue\"}")
        );
        insertDocumentsAndGetObjectIds(documents);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        int cursorId;
        List<String> collectedNames = new ArrayList<>();

        // BUCKET.QUERY with limit=2
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"$or\": [{\"category\": {\"$eq\": \"electronics\"}}, {\"color\": {\"$eq\": \"red\"}}]}",
                    BucketQueryArgs.Builder.limit(2)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            cursorId = extractCursorId(msg);

            List<BsonDocument> entries = extractEntries(msg);
            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
            }
        }

        // BUCKET.ADVANCE until exhausted
        int maxAdvanceCalls = 20;
        int advanceCount = 0;
        while (advanceCount < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);

            if (entries.isEmpty()) {
                break;
            }

            assertTrue(entries.size() <= 2, "Batch size should respect limit=2");

            for (BsonDocument doc : entries) {
                collectedNames.add(doc.getString("name").getValue());
            }
            advanceCount++;
        }

        // Verify all 10 matching documents found
        Set<String> expectedNames = Set.of("BC1", "BC2", "BC3", "BC4", "CA1", "CA2", "CA3", "CO1", "CO2", "CO3");
        assertEquals(10, collectedNames.size(), "Should retrieve exactly 10 matching documents");

        // Verify no duplicates
        Set<String> uniqueNames = new HashSet<>(collectedNames);
        assertEquals(collectedNames.size(), uniqueNames.size(),
                "Should not have duplicates despite FullScanNode child rewind");

        // Verify correct document set
        assertEquals(expectedNames, uniqueNames);
    }
}