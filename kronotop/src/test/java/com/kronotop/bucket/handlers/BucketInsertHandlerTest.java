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
import com.kronotop.TestUtil;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BsonHelper;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.*;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class BucketInsertHandlerTest extends BaseBucketHandlerTest {

    private static final String VECTOR_BUCKET = "vector-test-bucket";
    private static final String VECTOR_INDEX_JSON = "{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}";

    /**
     * Provides test data for all BqlValue/BsonType combinations for index creation tests
     */
    static Stream<Arguments> indexCreationTestData() {
        return Stream.of(
                Arguments.of("name", BsonType.STRING, "{\"name\": \"John Doe\", \"age\": 30}", "John Doe"),
                Arguments.of("age", BsonType.INT32, "{\"name\": \"John\", \"age\": 25}", 25L),
                Arguments.of("timestamp", BsonType.INT64, "{\"timestamp\": {\"$numberLong\": \"1234567890123\"}}", 1234567890123L),
                Arguments.of("score", BsonType.DOUBLE, "{\"score\": 95.5}", 95.5),
                Arguments.of("active", BsonType.BOOLEAN, "{\"active\": true}", true),
                Arguments.of("created", BsonType.DATE_TIME, "{\"created\": {\"$date\": \"2022-01-01T00:00:00Z\"}}", 1640995200000L)
                //Arguments.of("price", BsonType.DECIMAL128, "{\"price\": {\"$numberDecimal\": \"99.99\"}}", "99.99")
        );
    }

    @BeforeEach
    void setUp() {
        createBucket(TEST_BUCKET);
    }

    @Test
    void shouldInsertSingleDocumentWithOneOffTransaction() {
        // Behavior: Inserting a single document returns an array with one ObjectId.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, TEST_DOCUMENT).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        assertEquals(1, actualMessage.children().size());
        FullBulkStringRedisMessage message = (FullBulkStringRedisMessage) actualMessage.children().getFirst();
        ObjectId objectId = TestUtil.bulkStringToObjectId(message);
        assertNotNull(objectId);
    }

    @Test
    void shouldInsertMultipleDocumentsWithOneOffTransaction() {
        // Behavior: Inserting two documents returns an array with two unique ObjectIds.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"one\": \"two\"}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"three\": \"four\"}")
                ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        assertEquals(2, actualMessage.children().size());
        List<ObjectId> objectIds = TestUtil.extractObjectIds(actualMessage);
        assertEquals(2, objectIds.size());
        assertNotEquals(objectIds.get(0), objectIds.get(1), "ObjectIds should be unique");
    }

    @Test
    void shouldInsertManyDocumentsInSingleCommand() {
        // Behavior: Inserting 10 documents in a single BUCKET.INSERT command returns 10 unique ObjectIds.
        int documentCount = 10;
        List<byte[]> documentList = new ArrayList<>();
        for (int i = 0; i < documentCount; i++) {
            documentList.add(BSONUtil.jsonToDocumentThenBytes(
                    String.format("{\"index\": %d, \"name\": \"doc-%d\"}", i, i)
            ));
        }

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, makeDocumentsArray(documentList)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        assertEquals(documentCount, actualMessage.children().size());

        List<ObjectId> objectIds = TestUtil.extractObjectIds(actualMessage);
        assertEquals(documentCount, objectIds.size());
        assertEquals(documentCount, objectIds.stream().distinct().count(), "All ObjectIds should be unique");
    }

    @Test
    void shouldInsertWithinTransaction() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        // BEGIN
        {
            ByteBuf buf = Unpooled.buffer();
            // Create a new transaction
            cmd.begin().encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        // BUCKET.INSERT <bucket-name> <document> <document>
        {
            ByteBuf buf = Unpooled.buffer();
            BucketCommandBuilder<byte[], byte[]> bucketCommandBuilder = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            byte[][] docs = makeDocumentsArray(
                    List.of(
                            BSONUtil.jsonToDocumentThenBytes("{\"one\": \"two\"}"),
                            BSONUtil.jsonToDocumentThenBytes("{\"three\": \"four\"}")
                    ));
            bucketCommandBuilder.insert(TEST_BUCKET, docs).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);

            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
            assertEquals(2, actualMessage.children().size());

            // ObjectIds have returned
            for (RedisMessage redisMessage : actualMessage.children()) {
                assertInstanceOf(FullBulkStringRedisMessage.class, redisMessage);
                ObjectId objectId = TestUtil.bulkStringToObjectId((FullBulkStringRedisMessage) redisMessage);
                assertNotNull(objectId);
            }
        }

        // COMMIT
        {
            // Commit the changes
            ByteBuf buf = Unpooled.buffer();
            cmd.commit().encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }
    }

    @ParameterizedTest
    @MethodSource("indexCreationTestData")
    void shouldCreateIndexEntriesDuringDocumentInsertion(String fieldName, BsonType bsonType, String jsonDocument, Object expectedIndexValue) {
        // Create an index for the field
        SingleFieldIndexDefinition indexDefinition = SingleFieldIndexDefinition.create(fieldName + "-index", fieldName, bsonType, false, IndexStatus.WAITING);

        // Create bucket metadata and register the index
        createIndexThenWaitForReadiness(indexDefinition);

        // Refresh bucket metadata to include the new index
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert document
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, documentBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(1, actualMessage.children().size());

        // Verify the index entry was created
        Index index = metadata.indexes().getIndex(indexDefinition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist for " + fieldName);
        DirectorySubspace indexSubspace = index.subspace();

        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexedEntries = tr.getRange(begin, end).asList().join();
            assertEquals(1, indexedEntries.size(), "Should have exactly one index entry for " + fieldName);

            KeyValue indexEntry = indexedEntries.get(0);

            // Verify the index key structure
            Tuple unpackedIndex = indexSubspace.unpack(indexEntry.getKey());
            assertEquals((long) IndexSubspaceMagic.ENTRIES.getValue(), unpackedIndex.get(0), "Magic value should match");

            Object actualIndexValue = unpackedIndex.get(1);
            if (expectedIndexValue instanceof byte[]) {
                assertArrayEquals((byte[]) expectedIndexValue, (byte[]) actualIndexValue, "Binary index value should match");
            } else {
                assertEquals(expectedIndexValue, actualIndexValue, fieldName + " index value should match");
            }

            byte[] objectIdBytes = (byte[]) unpackedIndex.get(2);
            assertNotNull(objectIdBytes, "ObjectId bytes should not be null");
            assertEquals(12, objectIdBytes.length, "ObjectId should be 12 bytes");

            // Verify the index entry value contains proper metadata
            IndexEntry decodedEntry = IndexEntry.decode(indexEntry.getValue());
            assertNotNull(decodedEntry.entryMetadata(), "Entry metadata should not be null");
        }
    }

    @Test
    void shouldNotIgnoreMissingFieldsInDocument() {
        // Create indexes for fields that don't exist in the document
        SingleFieldIndexDefinition nameIndexDefinition = SingleFieldIndexDefinition.create("name-index", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);

        createIndexThenWaitForReadiness(nameIndexDefinition, ageIndexDefinition);

        // Refresh bucket metadata to include the new indexes
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert document that only has one of the indexed fields
        String jsonDocument = "{\"name\": \"John Doe\", \"city\": \"New York\"}"; // Missing 'age' field
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, documentBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(1, actualMessage.children().size());

        // Verify only the 'name' index entry was created
        Index nameIndex = metadata.indexes().getIndex("name", IndexSelectionPolicy.READ);
        Index ageIndex = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        assertNotNull(nameIndex, "Name index should exist");
        assertNotNull(ageIndex, "Age index should exist");
        DirectorySubspace nameIndexSubspace = nameIndex.subspace();
        DirectorySubspace ageIndexSubspace = ageIndex.subspace();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Check name index - should have entry
            byte[] namePrefix = nameIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> nameEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(namePrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(namePrefix))
            ).asList().join();
            assertEquals(1, nameEntries.size(), "Should have one entry for name index");

            // Check age index - should have one entry(null)
            byte[] agePrefix = ageIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> ageEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(agePrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(agePrefix))
            ).asList().join();

            assertEquals(1, ageEntries.size(), "Should have an entry(null) for age index when field is missing");
            for (KeyValue entry : ageEntries) {
                Tuple tuple = ageIndexSubspace.unpack(entry.getKey());
                assertNull(tuple.get(1));
            }
        }
    }

    @Test
    void shouldThrowIndexTypeMismatchExceptionWhenStrictTypesEnabled() {
        // Create an index expecting INT32 for 'age' field
        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);

        // Create bucket metadata and register the index
        createIndexThenWaitForReadiness(ageIndexDefinition);

        // Insert document where 'age' is a STRING instead of INT32
        // With strict_types=true (default in test.conf), this should throw IndexTypeMismatchException
        String jsonDocument = "{\"name\": \"John\", \"age\": \"twenty-five\"}"; // 'age' is string, not int32
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, documentBytes).encode(buf);

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
    void shouldThrowIndexTypeMismatchExceptionForInt64WithInt32Index() {
        // Create an index expecting INT32 for 'count' field
        SingleFieldIndexDefinition countIndexDefinition = SingleFieldIndexDefinition.create("count-index", "count", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(countIndexDefinition);

        // Insert document where 'count' is INT64 instead of INT32
        String jsonDocument = "{\"name\": \"Test\", \"count\": {\"$numberLong\": \"999999999999\"}}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, documentBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().startsWith("INDEXTYPE_MISMATCH"),
                "Should return INDEXTYPE_MISMATCH error for INT64 value with INT32 index");
    }

    @Test
    void shouldThrowIndexTypeMismatchExceptionForDoubleWithInt32Index() {
        // Create an index expecting INT32 for 'value' field
        SingleFieldIndexDefinition valueIndexDefinition = SingleFieldIndexDefinition.create("value-index", "value", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(valueIndexDefinition);

        // Insert document where 'value' is DOUBLE instead of INT32
        String jsonDocument = "{\"name\": \"Test\", \"value\": 3.14}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, documentBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().startsWith("INDEXTYPE_MISMATCH"),
                "Should return INDEXTYPE_MISMATCH error for DOUBLE value with INT32 index");
    }

    @Test
    void shouldThrowIndexTypeMismatchExceptionForBooleanWithStringIndex() {
        // Create an index expecting STRING for 'status' field
        SingleFieldIndexDefinition statusIndexDefinition = SingleFieldIndexDefinition.create("status-index", "status", BsonType.STRING, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(statusIndexDefinition);

        // Insert document where 'status' is BOOLEAN instead of STRING
        String jsonDocument = "{\"name\": \"Test\", \"status\": true}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, documentBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().startsWith("INDEXTYPE_MISMATCH"),
                "Should return INDEXTYPE_MISMATCH error for BOOLEAN value with STRING index");
    }

    @Test
    void shouldSucceedWhenValueTypeMatchesIndexType() {
        // Create an index expecting INT32 for 'age' field
        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        // Insert document where 'age' is correctly INT32
        String jsonDocument = "{\"name\": \"John\", \"age\": 25}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, documentBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(1, actualMessage.children().size(), "Insert should succeed when types match");
    }

    @Test
    void shouldCreateMultipleIndexEntriesForSingleDocument() {
        // Create multiple indexes
        SingleFieldIndexDefinition nameIndexDefinition = SingleFieldIndexDefinition.create("name-index", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition activeIndexDefinition = SingleFieldIndexDefinition.create("active-index", "active", BsonType.BOOLEAN, false, IndexStatus.WAITING);

        // Create bucket metadata and register the indexes
        createIndexThenWaitForReadiness(nameIndexDefinition, ageIndexDefinition, activeIndexDefinition);

        // Refresh bucket metadata to include the new indexes
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert document with all indexed fields
        String jsonDocument = "{\"name\": \"Alice\", \"age\": 28, \"active\": true, \"city\": \"Boston\"}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, documentBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(1, actualMessage.children().size());

        // Verify all three index entries were created
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Check name index
            Index nameIndex = metadata.indexes().getIndex("name", IndexSelectionPolicy.READ);
            assertNotNull(nameIndex, "Name index should exist");
            DirectorySubspace nameIndexSubspace = nameIndex.subspace();
            byte[] namePrefix = nameIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> nameEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(namePrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(namePrefix))
            ).asList().join();
            assertEquals(1, nameEntries.size(), "Should have one entry for name index");

            // Check age index
            Index ageIndex = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
            assertNotNull(ageIndex, "Age index should exist");
            DirectorySubspace ageIndexSubspace = ageIndex.subspace();
            byte[] agePrefix = ageIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> ageEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(agePrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(agePrefix))
            ).asList().join();
            assertEquals(1, ageEntries.size(), "Should have one entry for age index");

            // Check active index
            Index activeIndex = metadata.indexes().getIndex("active", IndexSelectionPolicy.READ);
            assertNotNull(activeIndex, "Active index should exist");
            DirectorySubspace activeIndexSubspace = activeIndex.subspace();
            byte[] activePrefix = activeIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> activeEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(activePrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(activePrefix))
            ).asList().join();
            assertEquals(1, activeEntries.size(), "Should have one entry for active index");

            // Verify the actual index values
            Tuple nameIndexTuple = nameIndexSubspace.unpack(nameEntries.get(0).getKey());
            assertEquals("Alice", nameIndexTuple.get(1), "Name index value should match");

            Tuple ageIndexTuple = ageIndexSubspace.unpack(ageEntries.get(0).getKey());
            assertEquals(28L, ageIndexTuple.get(1), "Age index value should match");

            Tuple activeIndexTuple = activeIndexSubspace.unpack(activeEntries.get(0).getKey());
            assertEquals(true, activeIndexTuple.get(1), "Active index value should match");
        }
    }

    @Test
    void shouldUpdateIndexCardinalityAfterInsertion() {
        // Create an INT32 index for 'age' field
        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);

        // Create bucket metadata and register the index
        createIndexThenWaitForReadiness(ageIndexDefinition);

        // Refresh bucket metadata to include the new index
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics ageStats = indexStatistics.get(ageIndexDefinition.id());
            assertNull(ageStats, "Age index statistics should not exist");
        }

        // Insert multiple documents with age field
        String[] documents = {
                "{\"name\": \"Alice\", \"age\": 25}",
                "{\"name\": \"Bob\", \"age\": 30}",
                "{\"name\": \"Charlie\", \"age\": 35}"
        };

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        for (String jsonDocument : documents) {
            ByteBuf buf = Unpooled.buffer();
            byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
            cmd.insert(TEST_BUCKET, documentBytes).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
            assertEquals(1, actualMessage.children().size());
        }

        // Verify cardinality has been updated to 3
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics ageStats = indexStatistics.get(ageIndexDefinition.id());
            assertNotNull(ageStats, "Age index statistics should exist after insertions");
            assertEquals(3L, ageStats.cardinality(), "Cardinality should be 3 after inserting 3 documents");
        }

        // Insert one more document to verify cardinality increments correctly
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"David\", \"age\": 40}");
        cmd.insert(TEST_BUCKET, documentBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        // Verify final cardinality is 4
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics ageStats = indexStatistics.get(ageIndexDefinition.id());
            assertNotNull(ageStats, "Age index statistics should exist after final insertion");
            assertEquals(4L, ageStats.cardinality(), "Final cardinality should be 4 after inserting 4 documents");
        }
    }

    @Test
    void shouldThrowBucketBeingRemovedExceptionWhenInsertingIntoRemovedBucket() {
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

        // Try to insert into the dropped bucket
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, TEST_DOCUMENT).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertEquals("BUCKETBEINGREMOVED Bucket 'test-bucket' is being removed", errorMessage.content());
    }

    @Test
    void shouldRejectInsertWhenNamespaceIsBeingRemovedThenPurged() {
        String testNamespace = UUID.randomUUID().toString();
        String testBucket = "insert-test-bucket";

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        BucketCommandBuilder<byte[], byte[]> bucketCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            // Create namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(testNamespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            // Switch to the namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse(testNamespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            // Insert a document
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.create(testBucket).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            // Insert a document
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.insert(testBucket, TEST_DOCUMENT).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;
            assertEquals(1, actualMessage.children().size());
        }

        {
            // Remove the namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceRemove(testNamespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Wait for the namespace removal event to be processed
        await().atMost(15, TimeUnit.SECONDS).until(() ->
                context.getBucketMetadataCache().get(testNamespace, testBucket) == null
        );

        {
            // Try to insert a document - should fail with NAMESPACEBEINGREMOVED
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.insert(testBucket, TEST_DOCUMENT).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertEquals(
                    String.format("NAMESPACEBEINGREMOVED Namespace '%s' is being removed", testNamespace),
                    actualMessage.content()
            );
        }

        {
            // Purge the namespace - should succeed
            ByteBuf buf = Unpooled.buffer();
            cmd.namespacePurge(testNamespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            // Try to insert a document again - should fail with NOSUCHNAMESPACE
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.insert(testBucket, TEST_DOCUMENT).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertEquals(
                    String.format("NOSUCHNAMESPACE No such namespace: '%s'", testNamespace),
                    actualMessage.content()
            );
        }
    }

    @Test
    void shouldIndexMissingFieldsAsNull() {
        // Create an index for the 'age' field
        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert documents: one without age field, one with actual value
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}")
        );

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, makeDocumentsArray(documents)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(2, actualMessage.children().size());

        // Fetch all index entries
        Index ageIndex = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        assertNotNull(ageIndex, "Age index should exist");
        List<KeyValue> indexEntries = fetchAllIndexedEntries(ageIndex.subspace());

        // Should have 2 entries: one null (missing field), one with value 30
        assertEquals(2, indexEntries.size(), "Should have 2 index entries (null and 30)");

        // Extract indexed values
        List<Object> indexedValues = new ArrayList<>();
        byte[] prefix = ageIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        for (KeyValue kv : indexEntries) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey(), prefix.length, kv.getKey().length - prefix.length);
            indexedValues.add(keyTuple.get(0));
        }

        // Verify we have both null (for missing field) and 30L
        assertTrue(indexedValues.contains(null), "Should have null indexed value for missing field");
        assertTrue(indexedValues.contains(30L), "Should have 30 indexed value");
    }

    @Test
    void shouldIndexExplicitNullValues() {
        // Create an index for the 'age' field
        SingleFieldIndexDefinition ageIndexDefinition = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert documents: one with explicit null, one with actual value
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": null}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}")
        );

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, makeDocumentsArray(documents)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(2, actualMessage.children().size());

        // Fetch all index entries
        Index ageIndex = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        assertNotNull(ageIndex, "Age index should exist");
        List<KeyValue> indexEntries = fetchAllIndexedEntries(ageIndex.subspace());

        // Should have 2 entries: one null, one with value 30
        assertEquals(2, indexEntries.size(), "Should have 2 index entries (null and 30)");

        // Extract indexed values
        List<Object> indexedValues = new ArrayList<>();
        byte[] prefix = ageIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        for (KeyValue kv : indexEntries) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey(), prefix.length, kv.getKey().length - prefix.length);
            indexedValues.add(keyTuple.get(0));
        }

        // Verify we have both null and 30L
        assertTrue(indexedValues.contains(null), "Should have null indexed value for explicit null");
        assertTrue(indexedValues.contains(30L), "Should have 30 indexed value");
    }

    @Test
    void shouldInsertDocumentWithUserProvidedObjectId() {
        // Behavior: Inserting a document with a user-provided _id of type ObjectId should
        // preserve that _id and return it in the response.
        ObjectId providedId = new ObjectId();
        String json = String.format("{\"_id\": {\"$oid\": \"%s\"}, \"name\": \"Alice\"}", providedId.toHexString());

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(json)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(1, actualMessage.children().size());

        ObjectId returnedId = TestUtil.bulkStringToObjectId(
                (FullBulkStringRedisMessage) actualMessage.children().getFirst());
        assertEquals(providedId, returnedId, "Returned ObjectId should match the user-provided one");
    }

    @Test
    void shouldRejectDuplicateUserProvidedObjectId() {
        // Behavior: Inserting a document with an _id that already exists in the primary index
        // should fail with a DUPLICATEKEY error.
        ObjectId providedId = new ObjectId();
        String json = String.format("{\"_id\": {\"$oid\": \"%s\"}, \"name\": \"Alice\"}", providedId.toHexString());

        // The first insert should succeed
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf1 = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(json)).encode(buf1);
        Object msg1 = runCommand(channel, buf1);
        assertInstanceOf(ArrayRedisMessage.class, msg1);

        // Second insert with same _id should fail
        ByteBuf buf2 = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(json)).encode(buf2);
        Object msg2 = runCommand(channel, buf2);
        assertInstanceOf(ErrorRedisMessage.class, msg2);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg2;
        assertTrue(errorMessage.content().startsWith("DUPLICATEKEY"),
                "Should return DUPLICATEKEY error");
        assertTrue(errorMessage.content().contains(providedId.toHexString()),
                "Error message should contain the duplicate ObjectId");
    }

    @Test
    void shouldRejectNonObjectIdType() {
        // Behavior: Inserting a document with an _id that is not an ObjectId type
        // should fail with an error.
        String json = "{\"_id\": \"not-an-objectid\", \"name\": \"Alice\"}";

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(json)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("_id field must be of type ObjectId"),
                "Should return error about _id type");
    }

    @Test
    void shouldRejectDuplicateIdsWithinSameBatch() {
        // Behavior: A batch insert where two documents share the same user-provided _id
        // should fail with a DUPLICATEKEY error.
        ObjectId sharedId = new ObjectId();
        String json1 = String.format("{\"_id\": {\"$oid\": \"%s\"}, \"name\": \"Alice\"}", sharedId.toHexString());
        String json2 = String.format("{\"_id\": {\"$oid\": \"%s\"}, \"name\": \"Bob\"}", sharedId.toHexString());

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(List.of(
                BSONUtil.jsonToDocumentThenBytes(json1),
                BSONUtil.jsonToDocumentThenBytes(json2)
        ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().startsWith("DUPLICATEKEY"),
                "Should return DUPLICATEKEY error for duplicate _id within batch");
    }

    @Test
    void shouldAllowMixOfAutoAndUserProvidedIds() {
        // Behavior: A batch containing documents with and without user-provided _id
        // should succeed, returning the correct ObjectIds for each.
        ObjectId providedId = new ObjectId();
        String jsonWithId = String.format("{\"_id\": {\"$oid\": \"%s\"}, \"name\": \"Alice\"}", providedId.toHexString());
        String jsonWithoutId = "{\"name\": \"Bob\"}";

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(List.of(
                BSONUtil.jsonToDocumentThenBytes(jsonWithId),
                BSONUtil.jsonToDocumentThenBytes(jsonWithoutId)
        ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(2, actualMessage.children().size());

        List<ObjectId> objectIds = TestUtil.extractObjectIds(actualMessage);
        assertEquals(providedId, objectIds.get(0), "First ObjectId should be the user-provided one");
        assertNotEquals(providedId, objectIds.get(1), "Second ObjectId should be auto-generated");
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
    void shouldCreateMultikeyIndexEntriesForArrayOfDocuments() {
        // Create an index on "scores.type" field within the array
        SingleFieldIndexDefinition typeIndexDefinition = SingleFieldIndexDefinition.create("scores-type-index", "scores.type", BsonType.STRING, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(typeIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert document with array of objects: { scores: [ { type: "math", score: 90 }, { type: "english", score: 70 } ] }
        String jsonDocument = "{\"name\": \"Alice\", \"scores\": [{\"type\": \"math\", \"score\": 90}, {\"type\": \"english\", \"score\": 70}]}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, documentBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(1, actualMessage.children().size());

        // Fetch all index entries
        Index typeIndex = metadata.indexes().getIndex("scores.type", IndexSelectionPolicy.READ);
        assertNotNull(typeIndex, "scores.type index should exist");
        List<KeyValue> indexEntries = fetchAllIndexedEntries(typeIndex.subspace());

        // Should have 2 entries: "math" and "english"
        assertEquals(2, indexEntries.size(), "Should have 2 index entries for multikey index");

        // Extract indexed values
        List<Object> indexedValues = new ArrayList<>();
        byte[] prefix = typeIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        for (KeyValue kv : indexEntries) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey(), prefix.length, kv.getKey().length - prefix.length);
            indexedValues.add(keyTuple.get(0));
        }

        assertTrue(indexedValues.contains("math"), "Should have 'math' indexed value");
        assertTrue(indexedValues.contains("english"), "Should have 'english' indexed value");
    }

    @Test
    void shouldDeduplicateMultikeyIndexEntries() {
        // Create an index on "scores.type" field
        SingleFieldIndexDefinition typeIndexDefinition = SingleFieldIndexDefinition.create("scores-type-index", "scores.type", BsonType.STRING, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(typeIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert document with duplicate values in array: { scores: [ { type: "math" }, { type: "math" }, { type: "english" } ] }
        String jsonDocument = "{\"name\": \"Bob\", \"scores\": [{\"type\": \"math\"}, {\"type\": \"math\"}, {\"type\": \"english\"}]}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, documentBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(1, actualMessage.children().size());

        // Fetch all index entries
        Index typeIndex = metadata.indexes().getIndex("scores.type", IndexSelectionPolicy.READ);
        assertNotNull(typeIndex, "scores.type index should exist");
        List<KeyValue> indexEntries = fetchAllIndexedEntries(typeIndex.subspace());

        // Should have only 2 entries due to deduplication: "math" and "english"
        assertEquals(2, indexEntries.size(), "Should have 2 index entries after deduplication");

        // Extract indexed values
        List<Object> indexedValues = new ArrayList<>();
        byte[] prefix = typeIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        for (KeyValue kv : indexEntries) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey(), prefix.length, kv.getKey().length - prefix.length);
            indexedValues.add(keyTuple.get(0));
        }

        assertTrue(indexedValues.contains("math"), "Should have 'math' indexed value");
        assertTrue(indexedValues.contains("english"), "Should have 'english' indexed value");
        assertEquals(2, indexedValues.size(), "Should only have 2 unique values");
    }

    @Test
    void shouldUpdateCardinalityCorrectlyForMultikeyIndex() {
        // Create an index on "tags" field within array
        SingleFieldIndexDefinition tagsIndexDefinition = SingleFieldIndexDefinition.create("tags-index", "items.tag", BsonType.STRING, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(tagsIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert first document with 2 unique tags
        String doc1 = "{\"name\": \"Doc1\", \"items\": [{\"tag\": \"red\"}, {\"tag\": \"blue\"}]}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf1 = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(doc1)).encode(buf1);
        runCommand(channel, buf1);

        // Insert second document with 3 unique tags (one overlapping)
        String doc2 = "{\"name\": \"Doc2\", \"items\": [{\"tag\": \"blue\"}, {\"tag\": \"green\"}, {\"tag\": \"yellow\"}]}";
        ByteBuf buf2 = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(doc2)).encode(buf2);
        runCommand(channel, buf2);

        // Verify cardinality: 2 from doc1 + 3 from doc2 = 5 total index entries
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics tagsStats = indexStatistics.get(tagsIndexDefinition.id());
            assertNotNull(tagsStats, "Tags index statistics should exist");
            assertEquals(5L, tagsStats.cardinality(), "Cardinality should be 5 (2 + 3 entries)");
        }
    }

    @Test
    void shouldDeduplicateAndTrackCardinalityCorrectlyForMultikeyIndex() {
        // Create an index on the "tags.name" field within an array
        SingleFieldIndexDefinition tagsIndexDefinition = SingleFieldIndexDefinition.create("tags-name-index", "tags.name", BsonType.STRING, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(tagsIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        // Insert the first document with duplicates: 4 array elements but only 2 unique values
        // Expected: 2 index entries, cardinality = 2
        String doc1 = "{\"name\": \"Doc1\", \"tags\": [{\"name\": \"java\"}, {\"name\": \"java\"}, {\"name\": \"kotlin\"}, {\"name\": \"java\"}]}";
        ByteBuf buf1 = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(doc1)).encode(buf1);
        runCommand(channel, buf1);

        // Verify after first document: 2 unique index entries
        Index tagsIndex = metadata.indexes().getIndex("tags.name", IndexSelectionPolicy.READ);
        assertNotNull(tagsIndex, "tags.name index should exist");

        List<KeyValue> entriesAfterDoc1 = fetchAllIndexedEntries(tagsIndex.subspace());
        assertEquals(2, entriesAfterDoc1.size(), "Should have 2 index entries after doc1 (deduplicated)");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics stats = indexStatistics.get(tagsIndexDefinition.id());
            assertNotNull(stats, "Index statistics should exist after doc1");
            assertEquals(2L, stats.cardinality(), "Cardinality should be 2 after doc1 (deduplicated)");
        }

        // Insert a second document with duplicates: 5 array elements but only 3 unique values
        // Expected: 3 new index entries, total cardinality = 2 + 3 = 5
        String doc2 = "{\"name\": \"Doc2\", \"tags\": [{\"name\": \"python\"}, {\"name\": \"rust\"}, {\"name\": \"python\"}, {\"name\": \"go\"}, {\"name\": \"rust\"}]}";
        ByteBuf buf2 = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(doc2)).encode(buf2);
        runCommand(channel, buf2);

        // Verify after second document: 5 total index entries (2 + 3)
        List<KeyValue> entriesAfterDoc2 = fetchAllIndexedEntries(tagsIndex.subspace());
        assertEquals(5, entriesAfterDoc2.size(), "Should have 5 index entries after doc2");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics stats = indexStatistics.get(tagsIndexDefinition.id());
            assertNotNull(stats, "Index statistics should exist after doc2");
            assertEquals(5L, stats.cardinality(), "Cardinality should be 5 after doc2 (2 + 3 deduplicated)");
        }

        // Insert a third document with all duplicates of the same value: 3 array elements, 1 unique value
        // Expected: 1 new index entry, total cardinality = 5 + 1 = 6
        String doc3 = "{\"name\": \"Doc3\", \"tags\": [{\"name\": \"scala\"}, {\"name\": \"scala\"}, {\"name\": \"scala\"}]}";
        ByteBuf buf3 = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(doc3)).encode(buf3);
        runCommand(channel, buf3);

        // Verify final state: 6 total index entries
        List<KeyValue> finalEntries = fetchAllIndexedEntries(tagsIndex.subspace());
        assertEquals(6, finalEntries.size(), "Should have 6 index entries total");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics stats = indexStatistics.get(tagsIndexDefinition.id());
            assertNotNull(stats, "Index statistics should exist after doc3");
            assertEquals(6L, stats.cardinality(), "Final cardinality should be 6 (2 + 3 + 1 deduplicated)");
        }

        // Verify all unique values are indexed
        List<Object> indexedValues = new ArrayList<>();
        byte[] prefix = tagsIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        for (KeyValue kv : finalEntries) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey(), prefix.length, kv.getKey().length - prefix.length);
            indexedValues.add(keyTuple.get(0));
        }

        assertTrue(indexedValues.contains("java"), "Should have 'java' indexed");
        assertTrue(indexedValues.contains("kotlin"), "Should have 'kotlin' indexed");
        assertTrue(indexedValues.contains("python"), "Should have 'python' indexed");
        assertTrue(indexedValues.contains("rust"), "Should have 'rust' indexed");
        assertTrue(indexedValues.contains("go"), "Should have 'go' indexed");
        assertTrue(indexedValues.contains("scala"), "Should have 'scala' indexed");
    }

    @Test
    void shouldIndexNullElementsInMultikeyArray() {
        // Behavior: Array with null elements should index ONE null entry (deduplicated)
        // along with non-null values for consistent semantics with single-value indexes.
        SingleFieldIndexDefinition tagsIndexDefinition = SingleFieldIndexDefinition.create("tags-index", "tags.name", BsonType.STRING, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(tagsIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Document: { tags: [{name: "java"}, {name: null}, {name: "kotlin"}, {name: null}] }
        String jsonDocument = "{\"name\": \"Doc1\", \"tags\": [{\"name\": \"java\"}, {\"name\": null}, {\"name\": \"kotlin\"}, {\"name\": null}]}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(jsonDocument)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        // Fetch all index entries
        Index tagsIndex = metadata.indexes().getIndex("tags.name", IndexSelectionPolicy.READ);
        assertNotNull(tagsIndex, "tags.name index should exist");
        List<KeyValue> indexEntries = fetchAllIndexedEntries(tagsIndex.subspace());

        // Should have 3 entries: "java", "kotlin", and null (deduplicated from two nulls)
        assertEquals(3, indexEntries.size(), "Should have 3 index entries: java, kotlin, and one null");

        // Extract indexed values
        List<Object> indexedValues = new ArrayList<>();
        byte[] prefix = tagsIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        for (KeyValue kv : indexEntries) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey(), prefix.length, kv.getKey().length - prefix.length);
            indexedValues.add(keyTuple.get(0));
        }

        assertTrue(indexedValues.contains("java"), "Should have 'java' indexed");
        assertTrue(indexedValues.contains("kotlin"), "Should have 'kotlin' indexed");
        assertTrue(indexedValues.contains(null), "Should have null indexed for null array elements");
    }

    @Test
    void shouldIndexArrayWithOnlyNullElements() {
        // Behavior: Array containing only nulls should create exactly one null index entry.
        SingleFieldIndexDefinition tagsIndexDefinition = SingleFieldIndexDefinition.create("tags-index", "tags.name", BsonType.STRING, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(tagsIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Document: { tags: [{name: null}, {name: null}] }
        String jsonDocument = "{\"name\": \"Doc1\", \"tags\": [{\"name\": null}, {\"name\": null}]}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(jsonDocument)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        // Fetch all index entries
        Index tagsIndex = metadata.indexes().getIndex("tags.name", IndexSelectionPolicy.READ);
        assertNotNull(tagsIndex, "tags.name index should exist");
        List<KeyValue> indexEntries = fetchAllIndexedEntries(tagsIndex.subspace());

        // Should have exactly 1 entry: null (deduplicated)
        assertEquals(1, indexEntries.size(), "Should have exactly 1 index entry (null deduplicated)");

        // Verify the indexed value is null
        byte[] prefix = tagsIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        Tuple keyTuple = Tuple.fromBytes(indexEntries.getFirst().getKey(), prefix.length, indexEntries.getFirst().getKey().length - prefix.length);
        assertNull(keyTuple.get(0), "Indexed value should be null");
    }

    @Test
    void shouldUpdateCardinalityCorrectlyForMultikeyIndexWithNulls() {
        // Behavior: Cardinality tracking should count null entries like other values.
        SingleFieldIndexDefinition tagsIndexDefinition = SingleFieldIndexDefinition.create("tags-index", "tags.name", BsonType.STRING, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(tagsIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        // Insert document with 2 unique values + 2 nulls -> 3 index entries (java, kotlin, null)
        String doc1 = "{\"name\": \"Doc1\", \"tags\": [{\"name\": \"java\"}, {\"name\": null}, {\"name\": \"kotlin\"}, {\"name\": null}]}";
        ByteBuf buf1 = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(doc1)).encode(buf1);
        runCommand(channel, buf1);

        // Verify cardinality is 3 (java, kotlin, null)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics stats = indexStatistics.get(tagsIndexDefinition.id());
            assertNotNull(stats, "Index statistics should exist");
            assertEquals(3L, stats.cardinality(), "Cardinality should be 3 (java, kotlin, null)");
        }
    }

    // --- Compound Index Tests ---

    @Test
    void shouldInsertIntoMultipleShardsWithAutoCommit() {
        // Behavior: Documents inserted into different shards with auto-commit (no explicit transaction)
        // should all be visible when queried.
        BucketCommandBuilder<String, String> bucketCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(bucketCmd, RESPVersion.RESP3);

        // INSERT into shard 1 (SHARD_ID) with auto-commit
        {
            BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            byte[] doc1 = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"shard\": 1}");
            ByteBuf buf = Unpooled.buffer();
            insertCmd.insert(TEST_BUCKET, doc1).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
            assertEquals(1, actualMessage.children().size());
        }

        // INSERT into shard 2 with auto-commit
        {
            BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            byte[] doc2 = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"shard\": 2}");
            ByteBuf buf = Unpooled.buffer();
            insertCmd.insert(TEST_BUCKET, doc2).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
            assertEquals(1, actualMessage.children().size());
        }

        // Verify both documents are queryable via QUERY {} and check their content
        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.query(TEST_BUCKET, "{}").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(2, entries.size(), "Both documents from different shards should be visible");

            // Extract names from returned documents
            List<String> names = new ArrayList<>();
            for (BsonDocument doc : entries) {
                names.add(doc.getString("name").getValue());
            }

            names.sort(Comparator.naturalOrder());
            assertEquals(List.of("Alice", "Bob"), names, "Should have both Alice and Bob documents");
        }
    }

    @Test
    void shouldInsertIntoMultipleShardsWithinTransaction() {
        // Behavior: Documents inserted into different shards within a single transaction
        // should all be visible after commit.
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        BucketCommandBuilder<String, String> bucketCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(bucketCmd, RESPVersion.RESP3);

        // BEGIN
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        // INSERT into shard 1 (SHARD_ID)
        {
            byte[] doc1 = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"shard\": 1}");
            ByteBuf buf = Unpooled.buffer();
            insertCmd.insert(TEST_BUCKET, doc1).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
            assertEquals(1, actualMessage.children().size());
        }

        // INSERT into shard 2
        {
            byte[] doc2 = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"shard\": 2}");
            ByteBuf buf = Unpooled.buffer();
            insertCmd.insert(TEST_BUCKET, doc2).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
            assertEquals(1, actualMessage.children().size());
        }

        // COMMIT
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.commit().encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify both documents are queryable via QUERY {} and check their content
        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.query(TEST_BUCKET, "{}").encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(2, entries.size(), "Both documents from different shards should be visible after commit");

            // Extract names from returned documents
            List<String> names = new ArrayList<>();
            for (BsonDocument doc : entries) {
                names.add(doc.getString("name").getValue());
            }

            names.sort(Comparator.naturalOrder());
            assertEquals(List.of("Alice", "Bob"), names, "Should have both Alice and Bob documents");
        }
    }

    @Test
    void shouldCreateCompoundIndexEntriesDuringInsertion() {
        // Behavior: Inserting a document with fields matching a compound index creates one ENTRIES key
        // with both values in order.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "name_age_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);
        createIndexThenWaitForReadiness(definition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        String jsonDocument = "{\"name\": \"Alice\", \"age\": 30}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(jsonDocument)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("name_age_idx", IndexSelectionPolicy.ALL);
        assertNotNull(compoundIndex);
        List<KeyValue> entries = fetchAllIndexedEntries(compoundIndex.subspace());
        assertEquals(1, entries.size(), "Should have exactly 1 compound index entry");

        // Verify key structure: (ENTRIES, "Alice", 30L, objectIdBytes)
        byte[] prefix = compoundIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        Tuple keyTuple = Tuple.fromBytes(entries.getFirst().getKey(), prefix.length, entries.getFirst().getKey().length - prefix.length);
        assertEquals("Alice", keyTuple.get(0));
        assertEquals(30L, keyTuple.getLong(1));
    }

    @Test
    void shouldCreateCompoundIndexEntriesForMultipleDocuments() {
        // Behavior: Inserting multiple documents creates one compound index entry per document.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "name_age_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);
        createIndexThenWaitForReadiness(definition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 35}")
        ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("name_age_idx", IndexSelectionPolicy.ALL);
        List<KeyValue> entries = fetchAllIndexedEntries(compoundIndex.subspace());
        assertEquals(3, entries.size(), "Should have 3 compound index entries");
    }

    @Test
    void shouldIndexMissingFieldsAsNullInCompoundIndex() {
        // Behavior: When a document is missing a field covered by a compound index, that field is indexed as null.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "name_age_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);
        createIndexThenWaitForReadiness(definition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        String jsonDocument = "{\"name\": \"Alice\"}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(jsonDocument)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("name_age_idx", IndexSelectionPolicy.ALL);
        List<KeyValue> entries = fetchAllIndexedEntries(compoundIndex.subspace());
        assertEquals(1, entries.size(), "Should have 1 compound index entry");

        // Verify key structure: (ENTRIES, "Alice", null, objectIdBytes)
        byte[] prefix = compoundIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        Tuple keyTuple = Tuple.fromBytes(entries.getFirst().getKey(), prefix.length, entries.getFirst().getKey().length - prefix.length);
        assertEquals("Alice", keyTuple.get(0));
        assertNull(keyTuple.get(1), "Missing field should be indexed as null");
    }

    @Test
    void shouldCreateMultiKeyCompoundIndexEntriesWithTopLevelArray() {
        // Behavior: When a compound index has a multi-key field pointing at a top-level array,
        // one entry per unique array element is created.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "name_tags_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("tags", BsonType.STRING, true)
                )
                , IndexStatus.WAITING);
        createIndexThenWaitForReadiness(definition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        String jsonDocument = "{\"name\": \"Alice\", \"tags\": [\"java\", \"go\"]}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(jsonDocument)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("name_tags_idx", IndexSelectionPolicy.ALL);
        List<KeyValue> entries = fetchAllIndexedEntries(compoundIndex.subspace());
        assertEquals(2, entries.size(), "Should have 2 compound index entries for multi-key array");

        // Extract indexed values
        byte[] prefix = compoundIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        List<String> tagValues = new ArrayList<>();
        for (KeyValue kv : entries) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey(), prefix.length, kv.getKey().length - prefix.length);
            assertEquals("Alice", keyTuple.get(0));
            tagValues.add(keyTuple.getString(1));
        }
        assertTrue(tagValues.contains("java"), "Should have 'java' entry");
        assertTrue(tagValues.contains("go"), "Should have 'go' entry");
    }

    @Test
    void shouldCreateMultiKeyCompoundIndexEntriesWithNestedArrayOfObjects() {
        // Behavior: When a compound index has a multi-key field using dot-notation into an array of objects,
        // one entry per unique matched value is created.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "name_scores_type_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("scores.type", BsonType.STRING, true)
                )
                , IndexStatus.WAITING);
        createIndexThenWaitForReadiness(definition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        String jsonDocument = "{\"name\": \"Alice\", \"scores\": [{\"type\": \"math\"}, {\"type\": \"english\"}]}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(jsonDocument)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("name_scores_type_idx", IndexSelectionPolicy.ALL);
        List<KeyValue> entries = fetchAllIndexedEntries(compoundIndex.subspace());
        assertEquals(2, entries.size(), "Should have 2 compound index entries for nested array");

        byte[] prefix = compoundIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        List<String> typeValues = new ArrayList<>();
        for (KeyValue kv : entries) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey(), prefix.length, kv.getKey().length - prefix.length);
            assertEquals("Alice", keyTuple.get(0));
            typeValues.add(keyTuple.getString(1));
        }
        assertTrue(typeValues.contains("math"), "Should have 'math' entry");
        assertTrue(typeValues.contains("english"), "Should have 'english' entry");
    }

    @Test
    void shouldDeduplicateMultiKeyCompoundIndexEntries() {
        // Behavior: Duplicate values in a multi-key array field produce only one compound index entry
        // per unique value.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "name_tags_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("tags", BsonType.STRING, true)
                )
                , IndexStatus.WAITING);
        createIndexThenWaitForReadiness(definition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        String jsonDocument = "{\"name\": \"Bob\", \"tags\": [\"java\", \"java\", \"go\"]}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(jsonDocument)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("name_tags_idx", IndexSelectionPolicy.ALL);
        List<KeyValue> entries = fetchAllIndexedEntries(compoundIndex.subspace());
        assertEquals(2, entries.size(), "Should have 2 entries (not 3) after deduplication");
    }

    // --- Vector Index Tests ---

    @Test
    void shouldThrowIndexTypeMismatchForCompoundIndex() {
        // Behavior: With strict_types=true, inserting a document where a field type doesn't match the
        // compound index field type returns INDEXTYPE_MISMATCH error.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "name_age_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);
        createIndexThenWaitForReadiness(definition);

        String jsonDocument = "{\"name\": \"Alice\", \"age\": \"not-a-number\"}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BSONUtil.jsonToDocumentThenBytes(jsonDocument)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().startsWith("INDEXTYPE_MISMATCH"),
                "Should return INDEXTYPE_MISMATCH error");
    }

    @Test
    void shouldUpdateCardinalityForCompoundIndex() {
        // Behavior: Each compound index entry increments the index cardinality counter.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "name_age_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);
        createIndexThenWaitForReadiness(definition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 35}")
        ));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics stats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(3L, stats.cardinality(), "Cardinality should be 3 after inserting 3 documents");
        }
    }

    private BucketMetadata createBucketWithVectorIndex() {
        createBucket(VECTOR_BUCKET, List.of(TEST_SHARD_ID), VECTOR_INDEX_JSON);
        return getBucketMetadata(VECTOR_BUCKET);
    }

    @Test
    void shouldCreateVectorIndexEntryDuringInsertion() {
        // Behavior: Inserting a document with a vector field into a bucket with a vector index
        // creates one entry in the vector index subspace with the correct vector data.
        BucketMetadata metadata = createBucketWithVectorIndex();

        String jsonDocument = "{\"label\": \"test\", \"embedding\": [0.1, 0.2, 0.3]}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(VECTOR_BUCKET, BSONUtil.jsonToDocumentThenBytes(jsonDocument)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
        assertNotNull(vectorIndex, "Vector index should exist");
        List<KeyValue> entries = fetchAllIndexedEntries(vectorIndex.subspace());
        assertEquals(1, entries.size(), "Should have exactly 1 vector index entry");

        VectorIndexValue decoded = VectorIndexValue.decode(entries.getFirst().getValue());
        assertArrayEquals(new float[]{0.1f, 0.2f, 0.3f}, decoded.vector(), 1e-6f);
    }

    @Test
    void shouldSkipVectorIndexEntryWhenFieldMissing() {
        // Behavior: Inserting a document without the vector field creates no entry in the
        // vector index subspace.
        BucketMetadata metadata = createBucketWithVectorIndex();

        String jsonDocument = "{\"label\": \"test\"}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(VECTOR_BUCKET, BSONUtil.jsonToDocumentThenBytes(jsonDocument)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
        assertNotNull(vectorIndex, "Vector index should exist");
        List<KeyValue> entries = fetchAllIndexedEntries(vectorIndex.subspace());
        assertEquals(0, entries.size(), "Should have no vector index entries when field is missing");
    }

    @Test
    void shouldCreateVectorIndexEntriesForMultipleDocuments() {
        // Behavior: Inserting multiple documents with vector fields creates one vector index
        // entry per document.
        BucketMetadata metadata = createBucketWithVectorIndex();

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"label\": \"a\", \"embedding\": [0.1, 0.2, 0.3]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"label\": \"b\", \"embedding\": [0.4, 0.5, 0.6]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"label\": \"c\", \"embedding\": [0.7, 0.8, 0.9]}")
        ));
        cmd.insert(VECTOR_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
        List<KeyValue> entries = fetchAllIndexedEntries(vectorIndex.subspace());
        assertEquals(3, entries.size(), "Should have 3 vector index entries");
    }

    @Test
    void shouldUpdateVectorIndexCardinalityAfterInsertion() {
        // Behavior: Inserting documents with vector fields increments the vector index
        // cardinality counter correctly.
        BucketMetadata metadata = createBucketWithVectorIndex();

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"label\": \"a\", \"embedding\": [0.1, 0.2, 0.3]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"label\": \"b\", \"embedding\": [0.4, 0.5, 0.6]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"label\": \"c\", \"embedding\": [0.7, 0.8, 0.9]}")
        ));
        cmd.insert(VECTOR_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector("embedding", IndexSelectionPolicy.ALL);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics stats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), vectorIndex.definition().id());
            assertEquals(3L, stats.cardinality(), "Vector index cardinality should be 3");
        }
    }

    // --- Numeric Widening Integration Tests ---

    @Test
    void shouldWidenInt32ValueToInt64Index() {
        // Behavior: INT32 document value is losslessly widened to INT64 when stored in an INT64 index.
        SingleFieldIndexDefinition indexDefinition = SingleFieldIndexDefinition.create(
                "age-index", "age", BsonType.INT64, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(indexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 42}");
        cmd.insert(TEST_BUCKET, documentBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(1, actualMessage.children().size());

        Index index = metadata.indexes().getIndex(indexDefinition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist for age");
        DirectorySubspace indexSubspace = index.subspace();

        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexedEntries = tr.getRange(begin, end).asList().join();
            assertEquals(1, indexedEntries.size(), "Should have exactly one index entry");

            Tuple unpackedIndex = indexSubspace.unpack(indexedEntries.get(0).getKey());
            assertEquals(42L, unpackedIndex.get(1), "INT32 value 42 should be widened to Long 42L in INT64 index");
        }
    }

    @Test
    void shouldWidenInt32ValueToDoubleIndex() {
        // Behavior: INT32 document value is losslessly widened to DOUBLE when stored in a DOUBLE index.
        SingleFieldIndexDefinition indexDefinition = SingleFieldIndexDefinition.create(
                "score-index", "score", BsonType.DOUBLE, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(indexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"score\": 42}");
        cmd.insert(TEST_BUCKET, documentBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(1, actualMessage.children().size());

        Index index = metadata.indexes().getIndex(indexDefinition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist for score");
        DirectorySubspace indexSubspace = index.subspace();

        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexedEntries = tr.getRange(begin, end).asList().join();
            assertEquals(1, indexedEntries.size(), "Should have exactly one index entry");

            Tuple unpackedIndex = indexSubspace.unpack(indexedEntries.get(0).getKey());
            Object actualIndexValue = unpackedIndex.get(1);
            assertInstanceOf(Double.class, actualIndexValue, "Widened value should be stored as Double");
            assertEquals(42.0, actualIndexValue, "INT32 value 42 should be widened to Double 42.0 in DOUBLE index");
        }
    }

    @Test
    void shouldWidenMultipleInt32ValuesToInt64IndexAndTrackCardinality() {
        // Behavior: Multiple INT32 values are widened and indexed correctly, cardinality tracks all widened entries.
        SingleFieldIndexDefinition indexDefinition = SingleFieldIndexDefinition.create(
                "age-index", "age", BsonType.INT64, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(indexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        String[] documents = {
                "{\"name\": \"Alice\", \"age\": 20}",
                "{\"name\": \"Bob\", \"age\": 30}",
                "{\"name\": \"Charlie\", \"age\": 40}"
        };

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        for (String jsonDocument : documents) {
            ByteBuf buf = Unpooled.buffer();
            byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
            cmd.insert(TEST_BUCKET, documentBytes).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
            assertEquals(1, actualMessage.children().size());
        }

        Index index = metadata.indexes().getIndex(indexDefinition.selector(), IndexSelectionPolicy.READ);
        DirectorySubspace indexSubspace = index.subspace();

        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexedEntries = tr.getRange(begin, end).asList().join();
            assertEquals(3, indexedEntries.size(), "Should have 3 index entries for 3 widened INT32 values");

            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics ageStats = indexStatistics.get(indexDefinition.id());
            assertNotNull(ageStats, "Age index statistics should exist after insertions");
            assertEquals(3L, ageStats.cardinality(), "Cardinality should be 3 after inserting 3 widened documents");
        }
    }

    @Test
    void shouldRejectInt64ValueInDoubleIndex() {
        // Behavior: INT64 to DOUBLE widening is forbidden (lossy: 64-bit integers exceed double's 53-bit mantissa).
        SingleFieldIndexDefinition indexDefinition = SingleFieldIndexDefinition.create(
                "value-index", "value", BsonType.DOUBLE, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(indexDefinition);

        String jsonDocument = "{\"name\": \"Test\", \"value\": {\"$numberLong\": \"999999999999\"}}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, documentBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().startsWith("INDEXTYPE_MISMATCH"),
                "Should return INDEXTYPE_MISMATCH error for INT64 value with DOUBLE index (lossy widening forbidden)");
    }

    @Test
    void shouldFindWidenedInt32DocumentViaInt64IndexEqQuery() {
        // Behavior: An INT32 value widened to INT64 at store-time is found by an INT32 $eq query predicate via index lookup.
        SingleFieldIndexDefinition indexDefinition = SingleFieldIndexDefinition.create(
                "age-index", "age", BsonType.INT64, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(indexDefinition);

        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf insertBuf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 42}");
        insertCmd.insert(TEST_BUCKET, documentBytes).encode(insertBuf);

        Object insertMsg = runCommand(channel, insertBuf);
        assertInstanceOf(ArrayRedisMessage.class, insertMsg);

        BucketCommandBuilder<String, String> queryCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(queryCmd, RESPVersion.RESP3);

        ByteBuf queryBuf = Unpooled.buffer();
        queryCmd.query(TEST_BUCKET, "{\"age\": {\"$eq\": 42}}").encode(queryBuf);
        Object queryMsg = runCommand(channel, queryBuf);

        assertInstanceOf(MapRedisMessage.class, queryMsg);
        List<BsonDocument> entries = extractEntries(queryMsg);
        assertEquals(1, entries.size(), "Should find 1 document via INT64 index with INT32 predicate");
        assertEquals(42, BsonHelper.getInteger(entries.get(0), "age"),
                "Returned document should have age = 42");
    }

    @Test
    void shouldFindWidenedInt32DocumentsViaInt64IndexRangeQuery() {
        // Behavior: INT32 values widened to INT64 at store-time are found by an INT32 $gte range query via index range scan.
        SingleFieldIndexDefinition indexDefinition = SingleFieldIndexDefinition.create(
                "age-index", "age", BsonType.INT64, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(indexDefinition);

        String[] documents = {
                "{\"name\": \"Alice\", \"age\": 20}",
                "{\"name\": \"Bob\", \"age\": 30}",
                "{\"name\": \"Charlie\", \"age\": 40}"
        };

        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        for (String jsonDocument : documents) {
            ByteBuf buf = Unpooled.buffer();
            byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
            insertCmd.insert(TEST_BUCKET, documentBytes).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
        }

        BucketCommandBuilder<String, String> queryCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(queryCmd, RESPVersion.RESP3);

        ByteBuf queryBuf = Unpooled.buffer();
        queryCmd.query(TEST_BUCKET, "{\"age\": {\"$gte\": 30}}").encode(queryBuf);
        Object queryMsg = runCommand(channel, queryBuf);

        assertInstanceOf(MapRedisMessage.class, queryMsg);
        List<BsonDocument> entries = extractEntries(queryMsg);
        assertEquals(2, entries.size(), "Should find 2 documents with age >= 30 via INT64 index");
        for (BsonDocument doc : entries) {
            assertTrue(BsonHelper.getInteger(doc, "age") >= 30,
                    "Each returned document should have age >= 30");
        }
    }

    @Test
    void shouldFindDoubleDocumentsViaDoubleIndexWithInt32GtPredicate() {
        // Behavior: DOUBLE values stored in a DOUBLE index are found by an INT32 $gt query predicate via index range scan.
        SingleFieldIndexDefinition indexDefinition = SingleFieldIndexDefinition.create(
                "score-index", "score", BsonType.DOUBLE, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(indexDefinition);

        String[] documents = {
                "{\"name\": \"Alice\", \"score\": 5.5}",
                "{\"name\": \"Bob\", \"score\": 10.0}",
                "{\"name\": \"Charlie\", \"score\": 15.7}",
                "{\"name\": \"Diana\", \"score\": 22.3}"
        };

        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        for (String jsonDocument : documents) {
            ByteBuf buf = Unpooled.buffer();
            byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
            insertCmd.insert(TEST_BUCKET, documentBytes).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
        }

        BucketCommandBuilder<String, String> queryCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(queryCmd, RESPVersion.RESP3);

        ByteBuf queryBuf = Unpooled.buffer();
        queryCmd.query(TEST_BUCKET, "{\"score\": {\"$gt\": 10}}").encode(queryBuf);
        Object queryMsg = runCommand(channel, queryBuf);

        assertInstanceOf(MapRedisMessage.class, queryMsg);
        List<BsonDocument> entries = extractEntries(queryMsg);
        assertEquals(2, entries.size(), "Should find 2 documents with score > 10 via DOUBLE index with INT32 predicate");
        for (BsonDocument doc : entries) {
            assertTrue(BsonHelper.getDouble(doc, "score") > 10.0,
                    "Each returned document should have score > 10");
        }
    }

    @Test
    void shouldFindInt32FieldWithInt64PredicateViaFullScan() {
        // Behavior: PredicateEvaluator cross-type widening finds an INT32 document field when queried with an INT64 predicate during full scan.
        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf insertBuf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 42}");
        insertCmd.insert(TEST_BUCKET, documentBytes).encode(insertBuf);

        Object insertMsg = runCommand(channel, insertBuf);
        assertInstanceOf(ArrayRedisMessage.class, insertMsg);

        BucketCommandBuilder<String, String> queryCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(queryCmd, RESPVersion.RESP3);

        ByteBuf queryBuf = Unpooled.buffer();
        queryCmd.query(TEST_BUCKET, "{\"age\": {\"$eq\": {\"$numberLong\": \"42\"}}}").encode(queryBuf);
        Object queryMsg = runCommand(channel, queryBuf);

        assertInstanceOf(MapRedisMessage.class, queryMsg);
        List<BsonDocument> entries = extractEntries(queryMsg);
        assertEquals(1, entries.size(), "Should find 1 document via full scan with INT64 predicate matching INT32 field");
        assertEquals(42, BsonHelper.getInteger(entries.get(0), "age"),
                "Returned document should have age = 42");
    }
}
