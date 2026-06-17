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

package com.kronotop.bucket.index;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.CollatorCache;
import com.kronotop.volume.AppendedEntry;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.kronotop.bucket.index.IndexMaintainer.NULL_VALUE;
import static org.junit.jupiter.api.Assertions.*;

class IndexBuilderBackPointerTest extends BaseIndexMaintainerTest {

    static Stream<Arguments> indexValueTestData() {
        ObjectId objectId = new ObjectId();
        return Stream.of(
                Arguments.of("string-index", "name", BsonType.STRING, "John Doe", "John Doe"),
                Arguments.of("int32-index", "age", BsonType.INT32, 25, 25L),
                Arguments.of("int64-index", "timestamp", BsonType.INT64, 1234567890L, 1234567890L),
                Arguments.of("double-index", "score", BsonType.DOUBLE, 95.5, 95.5),
                Arguments.of("boolean-index", "active", BsonType.BOOLEAN, true, true),
                Arguments.of("binary-index", "data", BsonType.BINARY, new byte[]{1, 2, 3, 4}, new byte[]{1, 2, 3, 4}),
                Arguments.of("datetime-index", "created", BsonType.DATE_TIME, 1640995200000L, 1640995200000L),
                // TODO: Enable this when we implement decimal128 indexes - Arguments.of("decimal128-index", "price", BsonType.DECIMAL128, "99.99", "99.99"),
                Arguments.of("objectid-index", "ref_id", BsonType.OBJECT_ID, objectId, objectId.toByteArray()),
                Arguments.of("null-index", "nullable", BsonType.NULL, null, null)
        );
    }

    private AppendedEntry createAppendedEntry(int userVersion) {
        byte[] encodedEntryMetadata = getEncodedEntryMetadata();
        return new AppendedEntry(userVersion, userVersion, null, encodedEntryMetadata);
    }

    BucketMetadata createIndexAndLoadBucketMetadata(String bucket, SingleFieldIndexDefinition... definitions) {
        createIndexThenWaitForReadiness(TEST_NAMESPACE, bucket, definitions);
        return refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
    }

    private void setIndexEntryAndCommit(SingleFieldIndexDefinition definition, BucketMetadata metadata, Object indexValue, ObjectId objectId, AppendedEntry entry) {
        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READWRITE);
        byte[] objectIdBytes = objectId.toByteArray();
        byte[] encodedIndexEntry = new IndexEntry(SHARD_ID, entry.metadataBytes()).encode();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SingleFieldIndexMaintainer.setEntry(tr, index, metadata, indexValue, objectIdBytes, encodedIndexEntry, new CollatorCache());
            tr.commit().join();
        }
    }

    private void verifyBackPointer(DirectorySubspace indexSubspace, Object expectedIndexValue, ObjectId expectedObjectId) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> backPointers = tr.getRange(begin, end).asList().join();
            boolean found = false;

            for (KeyValue kv : backPointers) {
                Tuple unpacked = indexSubspace.unpack(kv.getKey());
                assertEquals((long) IndexSubspaceMagic.BACK_POINTER.getValue(), unpacked.get(0), "Magic value should match");

                byte[] objectIdBytes = (byte[]) unpacked.get(1);
                ObjectId objectId = new ObjectId(objectIdBytes);
                Object indexValue = unpacked.get(2);

                if (objectId.equals(expectedObjectId)) {
                    if (expectedIndexValue instanceof byte[]) {
                        assertArrayEquals((byte[]) expectedIndexValue, (byte[]) indexValue, "Back pointer index value should match");
                    } else if (expectedIndexValue instanceof Integer && indexValue instanceof Long) {
                        assertEquals(((Integer) expectedIndexValue).longValue(), indexValue, "Back pointer index value should match");
                    } else {
                        assertEquals(expectedIndexValue, indexValue, "Back pointer index value should match");
                    }
                    assertArrayEquals(NULL_VALUE, kv.getValue(), "Back pointer value should be NULL_VALUE");
                    found = true;
                    break;
                }
            }
            assertTrue(found, "Back pointer should exist for ObjectId " + expectedObjectId);
        }
    }

    private List<Tuple> getBackPointersForObjectId(DirectorySubspace indexSubspace, ObjectId targetObjectId) {
        List<Tuple> backPointers = new ArrayList<>();
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> allBackPointers = tr.getRange(begin, end).asList().join();

            for (KeyValue kv : allBackPointers) {
                Tuple unpacked = indexSubspace.unpack(kv.getKey());
                byte[] objectIdBytes = (byte[]) unpacked.get(1);
                ObjectId objectId = new ObjectId(objectIdBytes);

                if (objectId.equals(targetObjectId)) {
                    backPointers.add(unpacked);
                }
            }
        }
        return backPointers;
    }

    private List<Object> getAllIndexValuesForObjectId(DirectorySubspace indexSubspace, ObjectId objectId) {
        List<Object> indexValues = new ArrayList<>();
        List<Tuple> backPointers = getBackPointersForObjectId(indexSubspace, objectId);

        for (Tuple backPointer : backPointers) {
            Object indexValue = backPointer.get(2);
            indexValues.add(indexValue);
        }
        return indexValues;
    }

    @ParameterizedTest
    @MethodSource("indexValueTestData")
    void shouldCreateBackPointerWhenSettingIndexEntry(String indexName, String fieldName, BsonType bsonType, Object inputValue, Object expectedStoredValue) {
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(indexName, fieldName, bsonType, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(TEST_BUCKET, definition);
        AppendedEntry entry = createAppendedEntry(0);
        ObjectId objectId = new ObjectId();

        setIndexEntryAndCommit(definition, metadata, inputValue, objectId, entry);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist for " + bsonType);
        DirectorySubspace indexSubspace = index.subspace();

        verifyBackPointer(indexSubspace, expectedStoredValue, objectId);
    }

    @Test
    void shouldCreateBackPointerWithCorrectStructure() {
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("test-index", "name", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(TEST_BUCKET, definition);
        AppendedEntry entry = createAppendedEntry(42);
        String indexValue = "test-value";
        ObjectId objectId = new ObjectId();

        setIndexEntryAndCommit(definition, metadata, indexValue, objectId, entry);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> backPointers = tr.getRange(begin, end).asList().join();
            assertEquals(1, backPointers.size(), "Should have exactly one back pointer");

            KeyValue backPointer = backPointers.getFirst();
            Tuple unpacked = indexSubspace.unpack(backPointer.getKey());

            assertEquals(3, unpacked.size(), "Back pointer tuple should have 3 elements");
            assertEquals((long) IndexSubspaceMagic.BACK_POINTER.getValue(), unpacked.get(0), "First element should be BACK_POINTER magic");

            byte[] objectIdBytes = (byte[]) unpacked.get(1);
            ObjectId actualObjectId = new ObjectId(objectIdBytes);
            assertEquals(objectId, actualObjectId, "Second element should be ObjectId");

            assertEquals(indexValue, unpacked.get(2), "Third element should be index value");
            assertArrayEquals(NULL_VALUE, backPointer.getValue(), "Back pointer value should be NULL_VALUE");
        }
    }

    @Test
    void shouldRetrieveIndexKeysUsingBackPointer() {
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("test-index", "name", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(TEST_BUCKET, definition);

        AppendedEntry entry = createAppendedEntry(100);
        String indexValue = "retrievable-value";
        ObjectId objectId = new ObjectId();

        setIndexEntryAndCommit(definition, metadata, indexValue, objectId, entry);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();
        List<Object> indexValues = getAllIndexValuesForObjectId(indexSubspace, objectId);

        assertEquals(1, indexValues.size(), "Should find one index value");
        assertEquals(indexValue, indexValues.getFirst(), "Index value should match");
    }

    @Test
    void shouldCreateBackPointersForMultipleIndexes() {
        SingleFieldIndexDefinition stringIndex = SingleFieldIndexDefinition.create("string-index", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition intIndex = SingleFieldIndexDefinition.create("int-index", "age", BsonType.INT32, false, IndexStatus.WAITING);

        BucketMetadata metadata = createIndexAndLoadBucketMetadata(TEST_BUCKET, stringIndex, intIndex);

        AppendedEntry entry = createAppendedEntry(200);
        String stringValue = "multi-index-test";
        Integer intValue = 25;
        ObjectId objectId = new ObjectId();

        setIndexEntryAndCommit(stringIndex, metadata, stringValue, objectId, entry);
        setIndexEntryAndCommit(intIndex, metadata, intValue, objectId, entry);

        Index stringIndexObj = metadata.indexes().getIndex(stringIndex.selector(), IndexSelectionPolicy.READ);
        assertNotNull(stringIndexObj, "String index should exist");
        DirectorySubspace stringIndexSubspace = stringIndexObj.subspace();
        Index intIndexObj = metadata.indexes().getIndex(intIndex.selector(), IndexSelectionPolicy.READ);
        assertNotNull(intIndexObj, "Int index should exist");
        DirectorySubspace intIndexSubspace = intIndexObj.subspace();

        verifyBackPointer(stringIndexSubspace, stringValue, objectId);
        verifyBackPointer(intIndexSubspace, intValue, objectId);
    }

    @Test
    void shouldScanBackPointersForObjectId() {
        SingleFieldIndexDefinition stringIndex = SingleFieldIndexDefinition.create("string-index", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition intIndex = SingleFieldIndexDefinition.create("int-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition doubleIndex = SingleFieldIndexDefinition.create("double-index", "score", BsonType.DOUBLE, false, IndexStatus.WAITING);

        BucketMetadata metadata = createIndexAndLoadBucketMetadata(TEST_BUCKET, stringIndex, intIndex, doubleIndex);

        AppendedEntry entry = createAppendedEntry(300);
        String stringValue = "scan-test";
        Integer intValue = 30;
        Double doubleValue = 98.7;
        ObjectId objectId = new ObjectId();

        setIndexEntryAndCommit(stringIndex, metadata, stringValue, objectId, entry);
        setIndexEntryAndCommit(intIndex, metadata, intValue, objectId, entry);
        setIndexEntryAndCommit(doubleIndex, metadata, doubleValue, objectId, entry);

        Index stringIndexObj = metadata.indexes().getIndex(stringIndex.selector(), IndexSelectionPolicy.READ);
        assertNotNull(stringIndexObj, "String index should exist");
        DirectorySubspace stringIndexSubspace = stringIndexObj.subspace();
        Index intIndexObj = metadata.indexes().getIndex(intIndex.selector(), IndexSelectionPolicy.READ);
        assertNotNull(intIndexObj, "Int index should exist");
        DirectorySubspace intIndexSubspace = intIndexObj.subspace();
        Index doubleIndexObj = metadata.indexes().getIndex(doubleIndex.selector(), IndexSelectionPolicy.READ);
        assertNotNull(doubleIndexObj, "Double index should exist");
        DirectorySubspace doubleIndexSubspace = doubleIndexObj.subspace();

        List<Object> stringIndexValues = getAllIndexValuesForObjectId(stringIndexSubspace, objectId);
        List<Object> intIndexValues = getAllIndexValuesForObjectId(intIndexSubspace, objectId);
        List<Object> doubleIndexValues = getAllIndexValuesForObjectId(doubleIndexSubspace, objectId);

        assertEquals(1, stringIndexValues.size(), "String index should have one back pointer");
        assertEquals(1, intIndexValues.size(), "Int index should have one back pointer");
        assertEquals(1, doubleIndexValues.size(), "Double index should have one back pointer");

        assertEquals(stringValue, stringIndexValues.getFirst());
        assertEquals(intValue.longValue(), intIndexValues.get(0));
        assertEquals(doubleValue, doubleIndexValues.get(0));
    }

    @Test
    void shouldHandleMultipleEntriesWithSameObjectId() {
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("multi-entry-index", "value", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(TEST_BUCKET, definition);

        ObjectId objectId = new ObjectId();
        List<String> expectedValues = Arrays.asList("value1", "value2", "value3");

        for (String value : expectedValues) {
            AppendedEntry entry = createAppendedEntry(400);
            setIndexEntryAndCommit(definition, metadata, value, objectId, entry);
        }

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();
        List<Object> retrievedValues = getAllIndexValuesForObjectId(indexSubspace, objectId);

        assertEquals(expectedValues.size(), retrievedValues.size(), "Should retrieve all values for the ObjectId");
        for (String expectedValue : expectedValues) {
            assertTrue(retrievedValues.contains(expectedValue), "Should contain value: " + expectedValue);
        }
    }

    @Test
    void shouldCreateBackPointersForNullValues() {
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("null-index", "nullable", BsonType.NULL, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(TEST_BUCKET, definition);
        AppendedEntry entry = createAppendedEntry(500);
        ObjectId objectId = new ObjectId();

        setIndexEntryAndCommit(definition, metadata, null, objectId, entry);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();
        verifyBackPointer(indexSubspace, null, objectId);
    }
}