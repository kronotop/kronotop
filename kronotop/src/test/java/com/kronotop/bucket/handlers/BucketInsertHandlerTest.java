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

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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
    void test_insert_single_document_with_oneOff_transaction() {
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
    void test_insert_documents_with_oneOff_transaction() {
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
    void test_insert_within_transaction() {
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
    void test_insert_commit_with_futures() {
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
    void shouldIgnoreTypeMismatchedFields() {
        // Create an index expecting INT32 for 'age' field
        IndexDefinition ageIndexDefinition = IndexDefinition.create("age-index", "age", BsonType.INT32);

        // Create bucket metadata and register the index
        createIndexThenWaitForReadiness(ageIndexDefinition);

        // Refresh bucket metadata to include the new index
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert document where 'age' is a STRING instead of INT32
        String jsonDocument = "{\"name\": \"John\", \"age\": \"twenty-five\"}"; // 'age' is string, not int32
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] documentBytes = BSONUtil.jsonToDocumentThenBytes(jsonDocument);
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), documentBytes).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(1, actualMessage.children().size());

        // Verify no index entry was created due to type mismatch
        Index ageIndex = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        assertNotNull(ageIndex, "Age index should exist");
        DirectorySubspace ageIndexSubspace = ageIndex.subspace();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] agePrefix = ageIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> ageEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(agePrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(agePrefix))
            ).asList().join();
            assertEquals(0, ageEntries.size(), "Should have no entries for age index when field type doesn't match");
        }
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
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace());
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
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace());
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
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace());
            IndexStatistics ageStats = indexStatistics.get(ageIndexDefinition.id());
            assertNotNull(ageStats, "Age index statistics should exist after final insertion");
            assertEquals(4L, ageStats.cardinality(), "Final cardinality should be 4 after inserting 4 documents");
        }
    }
}
