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

package com.kronotop.bucket;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.TestUtil;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.*;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.commandbuilder.kronotop.BucketInsertArgs;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for non-strict type behavior in secondary indexes.
 * When strict_types=false, type mismatches are skipped instead of throwing exceptions.
 */
class NonStrictTypesIndexTest extends BaseBucketHandlerTest {

    @Override
    protected String getConfigFileName() {
        return "test-non-strict-types.conf";
    }

    @Test
    void shouldSkipIndexEntryOnTypeMismatchDuringInsert() {
        // Create an index expecting INT32 for the 'age' field
        IndexDefinition ageIndexDefinition = IndexDefinition.create("age-index", "age", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert two documents: one with the correct type, one with a mismatched type
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),          // INT32 - should be indexed
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": \"thirty\"}")     // STRING - should be skipped
        );

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), makeDocumentsArray(documents)).encode(buf);

        // Insert should succeed without throwing an exception
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(2, actualMessage.children().size(), "Both documents should be inserted");

        // Fetch all index entries
        Index ageIndex = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        assertNotNull(ageIndex, "Age index should exist");
        List<KeyValue> indexEntries = TestUtil.fetchAllIndexedEntries(context, ageIndex.subspace());

        // Should have only 1 entry (Alice with INT32), Bob's STRING value should be skipped
        assertEquals(1, indexEntries.size(), "Should have 1 index entry (type mismatch skipped)");

        // Verify the indexed value is 25 (Alice's age)
        byte[] prefix = ageIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        Tuple keyTuple = Tuple.fromBytes(indexEntries.getFirst().getKey(), prefix.length,
                indexEntries.getFirst().getKey().length - prefix.length);
        assertEquals(25L, keyTuple.get(0), "Indexed value should be Alice's age (25)");
    }

    @Test
    void shouldSkipMultipleMismatchedTypesAndIndexCorrectOnes() {
        // Create an index expecting INT32 for the 'score' field
        IndexDefinition scoreIndexDefinition = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(scoreIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert documents with various types for the 'score' field
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"score\": 100}"),       // INT32 - indexed
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"score\": \"high\"}"),    // STRING - skipped
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"score\": 85}"),     // INT32 - indexed
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"score\": 3.14}"),     // DOUBLE - skipped
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"score\": 90}")          // INT32 - indexed
        );

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), makeDocumentsArray(documents)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(5, actualMessage.children().size(), "All 5 documents should be inserted");

        // Fetch all index entries
        Index scoreIndex = metadata.indexes().getIndex("score", IndexSelectionPolicy.READ);
        assertNotNull(scoreIndex, "Score index should exist");
        List<KeyValue> indexEntries = TestUtil.fetchAllIndexedEntries(context, scoreIndex.subspace());

        // Should have 3 entries (Alice=100, Charlie=85, Eve=90)
        assertEquals(3, indexEntries.size(), "Should have 3 index entries (2 type mismatches skipped)");

        // Extract and verify indexed values
        List<Long> indexedValues = new ArrayList<>();
        byte[] prefix = scoreIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        for (KeyValue kv : indexEntries) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey(), prefix.length, kv.getKey().length - prefix.length);
            indexedValues.add(keyTuple.getLong(0));
        }

        assertTrue(indexedValues.contains(100L), "Index should contain 100");
        assertTrue(indexedValues.contains(85L), "Index should contain 85");
        assertTrue(indexedValues.contains(90L), "Index should contain 90");
    }

    @Test
    void shouldDropIndexEntryOnTypeMismatchDuringUpdate() {
        // Create an index expecting INT32 for the 'age' field
        IndexDefinition ageIndexDefinition = IndexDefinition.create("age-index", "age", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert documents with the correct INT32 type
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}")
        );

        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf insertBuf = Unpooled.buffer();
        insertCmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), makeDocumentsArray(documents)).encode(insertBuf);
        Object insertMsg = runCommand(channel, insertBuf);
        assertInstanceOf(ArrayRedisMessage.class, insertMsg);

        // Verify both entries are indexed initially
        Index ageIndex = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        assertNotNull(ageIndex, "Age index should exist");
        assertEquals(2, TestUtil.fetchAllIndexedEntries(context, ageIndex.subspace()).size(), "Should have 2 index entries initially");

        // Update Alice's age with a STRING value (type mismatch)
        BucketCommandBuilder<String, String> updateCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(updateCmd, RESPVersion.RESP3);

        ByteBuf updateBuf = Unpooled.buffer();
        updateCmd.update(TEST_BUCKET, "{\"name\": {\"$eq\": \"Alice\"}}", "{\"$set\": {\"age\": \"twenty-five\"}}").encode(updateBuf);
        Object updateMsg = runCommand(channel, updateBuf);

        // Update should succeed without throwing an exception
        assertInstanceOf(MapRedisMessage.class, updateMsg);

        // Verify only Bob's entry remains in the index (Alice's entry dropped due to type mismatch)
        List<KeyValue> indexEntries = TestUtil.fetchAllIndexedEntries(context, ageIndex.subspace());
        assertEquals(1, indexEntries.size(), "Should have 1 index entry (Alice's dropped due to type mismatch)");

        // Verify the remaining indexed value is 30 (Bob's age)
        byte[] prefix = ageIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        Tuple keyTuple = Tuple.fromBytes(indexEntries.getFirst().getKey(), prefix.length,
                indexEntries.getFirst().getKey().length - prefix.length);
        assertEquals(30L, keyTuple.get(0), "Remaining indexed value should be Bob's age (30)");
    }

    @Test
    void shouldSkipMismatchedTypesAndCompleteBackgroundIndexBuilding() {
        // Insert documents with STRING values for the 'age' field (no index yet)
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": \"thirty-two\"}"),
                        BSONUtil.jsonToDocumentThenBytes("{\"age\": \"forty\"}")
                ));
        cmd.insert(TEST_BUCKET, BucketInsertArgs.Builder.shard(SHARD_ID), docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(2, actualMessage.children().size());

        // Create an index expecting INT32 for the 'age' field
        IndexDefinition definition = IndexDefinition.create(
                "age-index",
                "age",
                BsonType.INT32
        );

        DirectorySubspace indexSubspace;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            indexSubspace = IndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
            tr.commit().join();
        }

        // Wait for the index to become READY (background building completed)
        waitForIndexReadiness(indexSubspace);

        // Verify no index entries were created (all skipped due to type mismatch)
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.ALL);
        List<KeyValue> indexEntries = TestUtil.fetchAllIndexedEntries(context, index.subspace());
        assertEquals(0, indexEntries.size(), "No index entries should exist (all type mismatches skipped)");
    }
}
