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

package com.kronotop.bucket;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.TestUtil;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.*;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for array_filter operations with strict_types=false configuration.
 * Verifies that index maintenance silently skips type-mismatched values
 * while array_filter matching uses strict type semantics.
 */
class NonStrictTypesArrayFilterTest extends BaseBucketHandlerTest {

    @BeforeEach
    void setUp() {
        createBucket(TEST_BUCKET);
    }

    @Override
    protected String getConfigFileName() {
        return "test-non-strict-types.conf";
    }

    @Test
    void shouldSkipIndexEntryWhenArrayFilterUpdateProducesTypeMismatch() {
        // Behavior: When an array_filter update replaces INT32 elements with STRING values,
        // the index maintenance skips the STRING values (due to strict_types=false) while
        // keeping entries for remaining INT32 elements. Document is updated; index reflects
        // only type-compatible values.

        // Create INT32 index on 'scores'
        SingleFieldIndexDefinition scoresIndexDefinition = SingleFieldIndexDefinition.create("scores-index", "scores", BsonType.INT32, true, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(scoresIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert document with INT32 array: [55, 60, 65]
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"scores\": [55, 60, 65]}")
        );

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, makeDocumentsArray(documents)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(1, actualMessage.children().size(), "Document should be inserted");

        // Verify initial state: 3 index entries (55, 60, 65)
        Index scoresIndex = metadata.indexes().getIndex("scores", IndexSelectionPolicy.READ);
        assertNotNull(scoresIndex, "Scores index should exist");
        List<KeyValue> initialEntries = TestUtil.fetchAllIndexedEntries(context, scoresIndex.subspace());
        assertEquals(3, initialEntries.size(), "Should have 3 index entries initially");

        // Update: change elements <= 60 to STRING "zero" using array_filters
        // Array filter matches INT32 elements 55 and 60, replaces them with STRING "zero"
        // Result: ["zero", "zero", 65]
        BucketCommandBuilder<String, String> updateCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(updateCmd, RESPVersion.RESP3);

        ByteBuf updateBuf = Unpooled.buffer();
        updateCmd.update(
                TEST_BUCKET,
                "{\"name\": {\"$eq\": \"Alice\"}}",
                "{\"$set\": {\"scores.$[low]\": \"zero\"}, \"array_filters\": [{\"low\": {\"$lte\": 60}}]}"
        ).encode(updateBuf);
        Object updateMsg = runCommand(channel, updateBuf);

        // Update should succeed
        assertInstanceOf(MapRedisMessage.class, updateMsg);

        // Verify index state: only 1 entry (65) remains
        // STRING values "zero" are skipped due to strict_types=false on INT32 index
        List<KeyValue> entriesAfterUpdate = TestUtil.fetchAllIndexedEntries(context, scoresIndex.subspace());
        assertEquals(1, entriesAfterUpdate.size(), "Should have 1 index entry (STRING values skipped)");

        // Verify the remaining value is 65
        byte[] prefix = scoresIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        Tuple keyTuple = Tuple.fromBytes(entriesAfterUpdate.getFirst().getKey(), prefix.length,
                entriesAfterUpdate.getFirst().getKey().length - prefix.length);
        assertEquals(65L, keyTuple.get(0), "Remaining indexed value should be 65");
    }

    @Test
    void shouldHandleMultikeyIndexWithMixedTypesAndArrayFilter() {
        // Behavior: With a mixed-type array and an INT32 index, array_filter only matches
        // elements of the same type for comparison. When updating matched INT32 elements to 0,
        // index removes old INT32 entries and adds new ones, while STRING values remain skipped.

        // Create INT32 index on 'values'
        SingleFieldIndexDefinition valuesIndexDefinition = SingleFieldIndexDefinition.create("values-index", "values", BsonType.INT32, true, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(valuesIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert document with mixed-type array: [10, "twenty", 30]
        // Note: Only INT32 values (10, 30) will be indexed; "twenty" is skipped
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"values\": [10, \"twenty\", 30]}")
        );

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, makeDocumentsArray(documents)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        // Verify initial state: 2 index entries (10, 30) - STRING "twenty" skipped
        Index valuesIndex = metadata.indexes().getIndex("values", IndexSelectionPolicy.READ);
        assertNotNull(valuesIndex, "Values index should exist");
        List<KeyValue> initialEntries = TestUtil.fetchAllIndexedEntries(context, valuesIndex.subspace());
        assertEquals(2, initialEntries.size(), "Should have 2 index entries (STRING skipped)");

        Set<Long> initialValues = extractLongValuesFromIndex(valuesIndex.subspace(), initialEntries);
        assertTrue(initialValues.contains(10L), "Index should contain 10 initially");
        assertTrue(initialValues.contains(30L), "Index should contain 30 initially");

        // Update: change elements >= 10 to 0 using array_filters
        // Array filter uses strict type matching - only INT32 elements 10 and 30 match
        // STRING "twenty" does not match the filter (type mismatch in comparison)
        // Result: [0, "twenty", 0]
        BucketCommandBuilder<String, String> updateCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(updateCmd, RESPVersion.RESP3);

        ByteBuf updateBuf = Unpooled.buffer();
        updateCmd.update(
                TEST_BUCKET,
                "{\"name\": {\"$eq\": \"Bob\"}}",
                "{\"$set\": {\"values.$[num]\": 0}, \"array_filters\": [{\"num\": {\"$gte\": 10}}]}"
        ).encode(updateBuf);
        Object updateMsg = runCommand(channel, updateBuf);

        assertInstanceOf(MapRedisMessage.class, updateMsg);

        // Verify index state: 1 entry (0, deduplicated) - STRING still skipped
        List<KeyValue> entriesAfterUpdate = TestUtil.fetchAllIndexedEntries(context, valuesIndex.subspace());
        assertEquals(1, entriesAfterUpdate.size(), "Should have 1 index entry (0, deduplicated)");

        Set<Long> valuesAfter = extractLongValuesFromIndex(valuesIndex.subspace(), entriesAfterUpdate);
        assertTrue(valuesAfter.contains(0L), "Index should contain 0 after update");
        assertFalse(valuesAfter.contains(10L), "Index should NOT contain 10 after update");
        assertFalse(valuesAfter.contains(30L), "Index should NOT contain 30 after update");
    }

    @Test
    void shouldDropAllIndexEntriesWhenArrayFilterUpdatesToMismatchedType() {
        // Behavior: When all indexed field values are updated to a type that doesn't match
        // the index definition, all index entries are dropped. The document is updated but
        // becomes invisible to queries using that index (falls back to collection scan).

        // Create INT32 index on 'items.price'
        SingleFieldIndexDefinition priceIndexDefinition = SingleFieldIndexDefinition.create("price-index", "items.price", BsonType.INT32, true, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(priceIndexDefinition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Insert document with nested array containing INT32 prices
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Order1\", \"items\": [{\"name\": \"A\", \"price\": 10}, {\"name\": \"B\", \"price\": 20}]}")
        );

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(TEST_BUCKET, makeDocumentsArray(documents)).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        // Verify initial state: 2 index entries (10, 20)
        Index priceIndex = metadata.indexes().getIndex("items.price", IndexSelectionPolicy.READ);
        assertNotNull(priceIndex, "Price index should exist");
        List<KeyValue> initialEntries = TestUtil.fetchAllIndexedEntries(context, priceIndex.subspace());
        assertEquals(2, initialEntries.size(), "Should have 2 index entries initially");

        // Update: set all prices to STRING "free" using $[]
        // This replaces all INT32 prices with STRING values
        // Result: [{name: "A", price: "free"}, {name: "B", price: "free"}]
        BucketCommandBuilder<String, String> updateCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(updateCmd, RESPVersion.RESP3);

        ByteBuf updateBuf = Unpooled.buffer();
        updateCmd.update(
                TEST_BUCKET,
                "{\"name\": {\"$eq\": \"Order1\"}}",
                "{\"$set\": {\"items.$[].price\": \"free\"}}"
        ).encode(updateBuf);
        Object updateMsg = runCommand(channel, updateBuf);

        assertInstanceOf(MapRedisMessage.class, updateMsg);

        // Verify index state: 0 entries (all STRING values skipped)
        List<KeyValue> entriesAfterUpdate = TestUtil.fetchAllIndexedEntries(context, priceIndex.subspace());
        assertEquals(0, entriesAfterUpdate.size(), "Should have 0 index entries (all type mismatches skipped)");
    }

    @Test
    void shouldUpsertWithTypeMismatchAndSkipIndexEntry() {
        // Behavior: Upsert with a value that doesn't match the indexed field's expected type
        // succeeds when strict_types=false. The document is created but the mismatched field
        // is not indexed.

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndex);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"name\": \"John\"}", "{\"$set\": {\"age\": \"twenty-five\"}, \"upsert\": true}").encode(buf);
        Object msg = runCommand(channel, buf);

        // Upsert should succeed
        assertInstanceOf(MapRedisMessage.class, msg);

        // Verify the document was created by querying
        buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"name\": \"John\"}").encode(buf);
        msg = runCommand(channel, buf);

        List<BsonDocument> entries = extractEntries(msg);
        assertEquals(1, entries.size(), "Should find the upserted document");

        // Verify document content
        for (BsonDocument document : entries) {
            assertEquals("John", BsonHelper.getString(document, "name"));
            assertEquals("twenty-five", BsonHelper.getString(document, "age"));
        }

        // Verify no index entry was created (type mismatch skipped)
        Index index = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        assertNotNull(index, "Age index should exist");
        List<KeyValue> indexEntries = TestUtil.fetchAllIndexedEntries(context, index.subspace());
        assertEquals(0, indexEntries.size(), "Should have 0 index entries (STRING value skipped for INT32 index)");
    }

    private Set<Long> extractLongValuesFromIndex(DirectorySubspace indexSubspace, List<KeyValue> entries) {
        Set<Long> values = new HashSet<>();
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));

        for (KeyValue kv : entries) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey(), prefix.length, kv.getKey().length - prefix.length);
            Object value = keyTuple.get(0);
            if (value instanceof Long) {
                values.add((Long) value);
            }
        }
        return values;
    }
}
