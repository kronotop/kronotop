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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.CachedTimeService;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.*;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.commandbuilder.kronotop.BucketInsertArgs;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.protocol.CommitArgs;
import com.kronotop.protocol.CommitKeyword;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class BucketInsertHandlerTest extends BaseBucketHandlerTest {

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

    @Test
    void shouldInsertSingleDocumentWithOneOffTransaction() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), DOCUMENT).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        assertEquals(1, actualMessage.children().size());
        SimpleStringRedisMessage message = (SimpleStringRedisMessage) actualMessage.children().getFirst();
        assertNotNull(message.content());

        Versionstamp versionstamp = assertDoesNotThrow(() -> VersionstampUtil.base32HexDecode(message.content()));
        assertEquals(0, versionstamp.getUserVersion());
    }

    @Test
    void shouldInsertMultipleDocumentsWithOneOffTransaction() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"one\": \"two\"}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"three\": \"four\"}")
                ));
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        assertEquals(2, actualMessage.children().size());
        for (int i = 0; i < actualMessage.children().size(); i++) {
            SimpleStringRedisMessage message = (SimpleStringRedisMessage) actualMessage.children().get(i);
            assertNotNull(message.content());
        }
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
            bucketCommandBuilder.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), docs).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);

            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
            assertEquals(2, actualMessage.children().size());

            // User versions have returned
            for (RedisMessage redisMessage : actualMessage.children()) {
                assertInstanceOf(IntegerRedisMessage.class, redisMessage);
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

    @Test
    void shouldInsertAndCommitWithFutures() {
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
            bucketCommandBuilder.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), docs).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);

            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
            assertEquals(2, actualMessage.children().size());

            // User versions have returned
            for (RedisMessage redisMessage : actualMessage.children()) {
                assertInstanceOf(IntegerRedisMessage.class, redisMessage);
            }
        }

        // COMMIT
        {
            // Commit the changes
            ByteBuf buf = Unpooled.buffer();
            cmd.commit(CommitArgs.Builder.returning(CommitKeyword.FUTURES)).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);

            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
            assertEquals(1, actualMessage.children().size());
            RedisMessage result = actualMessage.children().getFirst();
            assertInstanceOf(MapRedisMessage.class, result);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) result;

            for (Map.Entry<RedisMessage, RedisMessage> entry : mapRedisMessage.children().entrySet()) {
                assertInstanceOf(IntegerRedisMessage.class, entry.getKey());
                IntegerRedisMessage userVersion = (IntegerRedisMessage) entry.getKey();

                assertInstanceOf(SimpleStringRedisMessage.class, entry.getValue());
                SimpleStringRedisMessage id = (SimpleStringRedisMessage) entry.getValue();

                Versionstamp decodedId = VersionstampUtil.base32HexDecode(id.content());
                assertEquals(decodedId.getUserVersion(), userVersion.value());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("indexCreationTestData")
    void shouldCreateIndexEntriesDuringDocumentInsertion(String fieldName, BsonType bsonType, String jsonDocument, Object expectedIndexValue) {
        // Create an index for the field
        IndexDefinition indexDefinition = IndexDefinition.create(fieldName + "-index", fieldName, bsonType);

        // Create bucket metadata and register the index
        createIndexThenWaitForReadiness(indexDefinition);

        // Refresh bucket metadata to include the new index
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert document
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), documentBytes).encode(buf);

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

            Versionstamp versionstamp = (Versionstamp) unpackedIndex.get(2);
            assertEquals(0, versionstamp.getUserVersion(), "User version should match entry");

            // Verify the index entry value contains proper metadata
            IndexEntry decodedEntry = IndexEntry.decode(indexEntry.getValue());
            assertEquals(SHARD_ID, decodedEntry.shardId(), "Shard ID should match");
            assertNotNull(decodedEntry.entryMetadata(), "Entry metadata should not be null");
        }
    }

    @Test
    void shouldNotIgnoreMissingFieldsInDocument() {
        // Create indexes for fields that don't exist in the document
        IndexDefinition nameIndexDefinition = IndexDefinition.create("name-index", "name", BsonType.STRING);
        IndexDefinition ageIndexDefinition = IndexDefinition.create("age-index", "age", BsonType.INT32);

        createIndexThenWaitForReadiness(nameIndexDefinition, ageIndexDefinition);

        // Refresh bucket metadata to include the new indexes
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert document that only has one of the indexed fields
        String jsonDocument = "{\"name\": \"John Doe\", \"city\": \"New York\"}"; // Missing 'age' field
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), documentBytes).encode(buf);

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
        IndexDefinition ageIndexDefinition = IndexDefinition.create("age-index", "age", BsonType.INT32);

        // Create bucket metadata and register the index
        createIndexThenWaitForReadiness(ageIndexDefinition);

        // Insert document where 'age' is a STRING instead of INT32
        // With strict_types=true (default in test.conf), this should throw IndexTypeMismatchException
        String jsonDocument = "{\"name\": \"John\", \"age\": \"twenty-five\"}"; // 'age' is string, not int32
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), documentBytes).encode(buf);

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
        IndexDefinition countIndexDefinition = IndexDefinition.create("count-index", "count", BsonType.INT32);
        createIndexThenWaitForReadiness(countIndexDefinition);

        // Insert document where 'count' is INT64 instead of INT32
        String jsonDocument = "{\"name\": \"Test\", \"count\": {\"$numberLong\": \"999999999999\"}}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), documentBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().startsWith("INDEXTYPE_MISMATCH"),
                "Should return INDEXTYPE_MISMATCH error for INT64 value with INT32 index");
    }

    @Test
    void shouldThrowIndexTypeMismatchExceptionForDoubleWithInt32Index() {
        // Create an index expecting INT32 for 'value' field
        IndexDefinition valueIndexDefinition = IndexDefinition.create("value-index", "value", BsonType.INT32);
        createIndexThenWaitForReadiness(valueIndexDefinition);

        // Insert document where 'value' is DOUBLE instead of INT32
        String jsonDocument = "{\"name\": \"Test\", \"value\": 3.14}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), documentBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().startsWith("INDEXTYPE_MISMATCH"),
                "Should return INDEXTYPE_MISMATCH error for DOUBLE value with INT32 index");
    }

    @Test
    void shouldThrowIndexTypeMismatchExceptionForBooleanWithStringIndex() {
        // Create an index expecting STRING for 'status' field
        IndexDefinition statusIndexDefinition = IndexDefinition.create("status-index", "status", BsonType.STRING);
        createIndexThenWaitForReadiness(statusIndexDefinition);

        // Insert document where 'status' is BOOLEAN instead of STRING
        String jsonDocument = "{\"name\": \"Test\", \"status\": true}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), documentBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().startsWith("INDEXTYPE_MISMATCH"),
                "Should return INDEXTYPE_MISMATCH error for BOOLEAN value with STRING index");
    }

    @Test
    void shouldSucceedWhenValueTypeMatchesIndexType() {
        // Create an index expecting INT32 for 'age' field
        IndexDefinition ageIndexDefinition = IndexDefinition.create("age-index", "age", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        // Insert document where 'age' is correctly INT32
        String jsonDocument = "{\"name\": \"John\", \"age\": 25}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), documentBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(1, actualMessage.children().size(), "Insert should succeed when types match");
    }

    @Test
    void shouldCreateMultipleIndexEntriesForSingleDocument() {
        // Create multiple indexes
        IndexDefinition nameIndexDefinition = IndexDefinition.create("name-index", "name", BsonType.STRING);
        IndexDefinition ageIndexDefinition = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition activeIndexDefinition = IndexDefinition.create("active-index", "active", BsonType.BOOLEAN);

        // Create bucket metadata and register the indexes
        createIndexThenWaitForReadiness(nameIndexDefinition, ageIndexDefinition, activeIndexDefinition);

        // Refresh bucket metadata to include the new indexes
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert document with all indexed fields
        String jsonDocument = "{\"name\": \"Alice\", \"age\": 28, \"active\": true, \"city\": \"Boston\"}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), documentBytes).encode(buf);

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
        IndexDefinition ageIndexDefinition = IndexDefinition.create("age-index", "age", BsonType.INT32);

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
            cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), documentBytes).encode(buf);

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
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), documentBytes).encode(buf);

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
        insertDocuments(List.of(DOCUMENT));

        // Get the bucket metadata and mark it as removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.openUncached(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            TransactionalContext tx = new TransactionalContext(context, tr);
            BucketMetadataUtil.setRemoved(tx, metadata);
            tr.commit().join();
        }

        // Flush the bucket metadata cache so createOrOpen reads the dropped status
        Runnable cleanup = context.getBucketMetadataCache().createEvictionWorker(
                context.getService(CachedTimeService.NAME), 0);
        cleanup.run();

        // Try to insert into the dropped bucket
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), DOCUMENT).encode(buf);
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
            bucketCmd.insert(testBucket, BucketInsertArgs.Builder.shard(SHARD_ID), DOCUMENT).encode(buf);

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
            bucketCmd.insert(testBucket, BucketInsertArgs.Builder.shard(SHARD_ID), DOCUMENT).encode(buf);

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
            bucketCmd.insert(testBucket, BucketInsertArgs.Builder.shard(SHARD_ID), DOCUMENT).encode(buf);

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
        IndexDefinition ageIndexDefinition = IndexDefinition.create("age-index", "age", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert documents: one without age field, one with actual value
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}")
        );

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), makeDocumentsArray(documents)).encode(buf);

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
        IndexDefinition ageIndexDefinition = IndexDefinition.create("age-index", "age", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert documents: one with explicit null, one with actual value
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": null}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}")
        );

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), makeDocumentsArray(documents)).encode(buf);

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
        IndexDefinition typeIndexDefinition = IndexDefinition.create("scores-type-index", "scores.type", BsonType.STRING);
        createIndexThenWaitForReadiness(typeIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert document with array of objects: { scores: [ { type: "math", score: 90 }, { type: "english", score: 70 } ] }
        String jsonDocument = "{\"name\": \"Alice\", \"scores\": [{\"type\": \"math\", \"score\": 90}, {\"type\": \"english\", \"score\": 70}]}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), documentBytes).encode(buf);

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
        IndexDefinition typeIndexDefinition = IndexDefinition.create("scores-type-index", "scores.type", BsonType.STRING);
        createIndexThenWaitForReadiness(typeIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert document with duplicate values in array: { scores: [ { type: "math" }, { type: "math" }, { type: "english" } ] }
        String jsonDocument = "{\"name\": \"Bob\", \"scores\": [{\"type\": \"math\"}, {\"type\": \"math\"}, {\"type\": \"english\"}]}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), documentBytes).encode(buf);

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
        IndexDefinition tagsIndexDefinition = IndexDefinition.create("tags-index", "items.tag", BsonType.STRING);
        createIndexThenWaitForReadiness(tagsIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert first document with 2 unique tags
        String doc1 = "{\"name\": \"Doc1\", \"items\": [{\"tag\": \"red\"}, {\"tag\": \"blue\"}]}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf1 = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), BSONUtil.jsonToDocumentThenBytes(doc1)).encode(buf1);
        runCommand(channel, buf1);

        // Insert second document with 3 unique tags (one overlapping)
        String doc2 = "{\"name\": \"Doc2\", \"items\": [{\"tag\": \"blue\"}, {\"tag\": \"green\"}, {\"tag\": \"yellow\"}]}";
        ByteBuf buf2 = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), BSONUtil.jsonToDocumentThenBytes(doc2)).encode(buf2);
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
        IndexDefinition tagsIndexDefinition = IndexDefinition.create("tags-name-index", "tags.name", BsonType.STRING);
        createIndexThenWaitForReadiness(tagsIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        // Insert the first document with duplicates: 4 array elements but only 2 unique values
        // Expected: 2 index entries, cardinality = 2
        String doc1 = "{\"name\": \"Doc1\", \"tags\": [{\"name\": \"java\"}, {\"name\": \"java\"}, {\"name\": \"kotlin\"}, {\"name\": \"java\"}]}";
        ByteBuf buf1 = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), BSONUtil.jsonToDocumentThenBytes(doc1)).encode(buf1);
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
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), BSONUtil.jsonToDocumentThenBytes(doc2)).encode(buf2);
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
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), BSONUtil.jsonToDocumentThenBytes(doc3)).encode(buf3);
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
}
