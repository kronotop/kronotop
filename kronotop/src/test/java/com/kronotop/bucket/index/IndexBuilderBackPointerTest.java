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

package com.kronotop.bucket.index;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.volume.AppendedEntry;
import com.kronotop.volume.VolumeTestUtil;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class IndexBuilderBackPointerTest extends BaseStandaloneInstanceTest {
    final int SHARD_ID = 1;
    final byte[] NULL_VALUE = new byte[]{0};

    static Stream<Arguments> indexValueTestData() {
        return Stream.of(
                Arguments.of("string-index", "name", BsonType.STRING, "John Doe", "John Doe"),
                Arguments.of("int32-index", "age", BsonType.INT32, 25, 25L),
                Arguments.of("int64-index", "timestamp", BsonType.INT64, 1234567890L, 1234567890L),
                Arguments.of("double-index", "score", BsonType.DOUBLE, 95.5, 95.5),
                Arguments.of("boolean-index", "active", BsonType.BOOLEAN, true, true),
                Arguments.of("binary-index", "data", BsonType.BINARY, new byte[]{1, 2, 3, 4}, new byte[]{1, 2, 3, 4}),
                Arguments.of("datetime-index", "created", BsonType.DATE_TIME, 1640995200000L, 1640995200000L),
                // TODO: Enable this when we implement decimal128 indexes - Arguments.of("decimal128-index", "price", BsonType.DECIMAL128, "99.99", "99.99"),
                Arguments.of("null-index", "nullable", BsonType.NULL, null, null)
        );
    }

    byte[] getEncodedEntryMetadata() {
        return VolumeTestUtil.generateEntryMetadata(1, 1, 0, 1, "test").encode();
    }

    private AppendedEntry createAppendedEntry(int userVersion) {
        byte[] encodedEntryMetadata = getEncodedEntryMetadata();
        return new AppendedEntry(userVersion, userVersion, null, encodedEntryMetadata);
    }

    BucketMetadata createIndexAndLoadBucketMetadata(String bucket, IndexDefinition... definitions) {
        createIndexThenWaitForReadiness(TEST_NAMESPACE, bucket, definitions);
        return refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
    }

    private void setIndexEntryAndCommit(IndexDefinition definition, BucketMetadata metadata, Object indexValue, AppendedEntry entry) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilder.setIndexEntry(tr, definition, SHARD_ID, metadata, indexValue, entry);
            tr.commit().join();
        }
    }

    private void verifyBackPointer(DirectorySubspace indexSubspace, Object expectedIndexValue, int expectedUserVersion) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> backPointers = tr.getRange(begin, end).asList().join();
            boolean found = false;

            for (KeyValue kv : backPointers) {
                Tuple unpacked = indexSubspace.unpack(kv.getKey());
                assertEquals((long) IndexSubspaceMagic.BACK_POINTER.getValue(), unpacked.get(0), "Magic value should match");

                Versionstamp versionstamp = (Versionstamp) unpacked.get(1);
                Object indexValue = unpacked.get(2);

                if (versionstamp.getUserVersion() == expectedUserVersion) {
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
            assertTrue(found, "Back pointer should exist for user version " + expectedUserVersion);
        }
    }

    private List<Tuple> getBackPointersForVersionstamp(DirectorySubspace indexSubspace, int userVersion) {
        List<Tuple> backPointers = new ArrayList<>();
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> allBackPointers = tr.getRange(begin, end).asList().join();

            for (KeyValue kv : allBackPointers) {
                Tuple unpacked = indexSubspace.unpack(kv.getKey());
                Versionstamp versionstamp = (Versionstamp) unpacked.get(1);

                if (versionstamp.getUserVersion() == userVersion) {
                    backPointers.add(unpacked);
                }
            }
        }
        return backPointers;
    }

    private List<Object> getAllIndexValuesForVersionstamp(DirectorySubspace indexSubspace, int userVersion) {
        List<Object> indexValues = new ArrayList<>();
        List<Tuple> backPointers = getBackPointersForVersionstamp(indexSubspace, userVersion);

        for (Tuple backPointer : backPointers) {
            Object indexValue = backPointer.get(2);
            indexValues.add(indexValue);
        }
        return indexValues;
    }

    @ParameterizedTest
    @MethodSource("indexValueTestData")
    void shouldCreateBackPointerWhenSettingIndexEntry(String indexName, String fieldName, BsonType bsonType, Object inputValue, Object expectedStoredValue) {
        IndexDefinition definition = IndexDefinition.create(indexName, fieldName, bsonType);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(TEST_BUCKET, definition);
        AppendedEntry entry = createAppendedEntry(0);

        setIndexEntryAndCommit(definition, metadata, inputValue, entry);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist for " + bsonType);
        DirectorySubspace indexSubspace = index.subspace();

        verifyBackPointer(indexSubspace, expectedStoredValue, 0);
    }

    @Test
    void shouldCreateBackPointerWithCorrectStructure() {
        IndexDefinition definition = IndexDefinition.create("test-index", "name", BsonType.STRING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(TEST_BUCKET, definition);
        AppendedEntry entry = createAppendedEntry(42);
        String indexValue = "test-value";

        setIndexEntryAndCommit(definition, metadata, indexValue, entry);

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

            Versionstamp versionstamp = (Versionstamp) unpacked.get(1);
            assertEquals(42, versionstamp.getUserVersion(), "Second element should be versionstamp with correct user version");

            assertEquals(indexValue, unpacked.get(2), "Third element should be index value");
            assertArrayEquals(NULL_VALUE, backPointer.getValue(), "Back pointer value should be NULL_VALUE");
        }
    }

    @Test
    void shouldRetrieveIndexKeysUsingBackPointer() {
        IndexDefinition definition = IndexDefinition.create("test-index", "name", BsonType.STRING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(TEST_BUCKET, definition);

        AppendedEntry entry = createAppendedEntry(100);
        String indexValue = "retrievable-value";

        setIndexEntryAndCommit(definition, metadata, indexValue, entry);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();
        List<Object> indexValues = getAllIndexValuesForVersionstamp(indexSubspace, 100);

        assertEquals(1, indexValues.size(), "Should find one index value");
        assertEquals(indexValue, indexValues.getFirst(), "Index value should match");
    }

    @Test
    void shouldCreateBackPointersForMultipleIndexes() {
        IndexDefinition stringIndex = IndexDefinition.create("string-index", "name", BsonType.STRING);
        IndexDefinition intIndex = IndexDefinition.create("int-index", "age", BsonType.INT32);

        BucketMetadata metadata = createIndexAndLoadBucketMetadata(TEST_BUCKET, stringIndex, intIndex);

        AppendedEntry entry = createAppendedEntry(200);
        String stringValue = "multi-index-test";
        Integer intValue = 25;

        setIndexEntryAndCommit(stringIndex, metadata, stringValue, entry);
        setIndexEntryAndCommit(intIndex, metadata, intValue, entry);

        Index stringIndexObj = metadata.indexes().getIndex(stringIndex.selector(), IndexSelectionPolicy.READ);
        assertNotNull(stringIndexObj, "String index should exist");
        DirectorySubspace stringIndexSubspace = stringIndexObj.subspace();
        Index intIndexObj = metadata.indexes().getIndex(intIndex.selector(), IndexSelectionPolicy.READ);
        assertNotNull(intIndexObj, "Int index should exist");
        DirectorySubspace intIndexSubspace = intIndexObj.subspace();

        verifyBackPointer(stringIndexSubspace, stringValue, 200);
        verifyBackPointer(intIndexSubspace, intValue, 200);
    }

    @Test
    void shouldScanBackPointersForVersionstamp() {
        IndexDefinition stringIndex = IndexDefinition.create("string-index", "name", BsonType.STRING);
        IndexDefinition intIndex = IndexDefinition.create("int-index", "age", BsonType.INT32);
        IndexDefinition doubleIndex = IndexDefinition.create("double-index", "score", BsonType.DOUBLE);

        BucketMetadata metadata = createIndexAndLoadBucketMetadata(TEST_BUCKET, stringIndex, intIndex, doubleIndex);

        AppendedEntry entry = createAppendedEntry(300);
        String stringValue = "scan-test";
        Integer intValue = 30;
        Double doubleValue = 98.7;

        setIndexEntryAndCommit(stringIndex, metadata, stringValue, entry);
        setIndexEntryAndCommit(intIndex, metadata, intValue, entry);
        setIndexEntryAndCommit(doubleIndex, metadata, doubleValue, entry);

        Index stringIndexObj = metadata.indexes().getIndex(stringIndex.selector(), IndexSelectionPolicy.READ);
        assertNotNull(stringIndexObj, "String index should exist");
        DirectorySubspace stringIndexSubspace = stringIndexObj.subspace();
        Index intIndexObj = metadata.indexes().getIndex(intIndex.selector(), IndexSelectionPolicy.READ);
        assertNotNull(intIndexObj, "Int index should exist");
        DirectorySubspace intIndexSubspace = intIndexObj.subspace();
        Index doubleIndexObj = metadata.indexes().getIndex(doubleIndex.selector(), IndexSelectionPolicy.READ);
        assertNotNull(doubleIndexObj, "Double index should exist");
        DirectorySubspace doubleIndexSubspace = doubleIndexObj.subspace();

        List<Object> stringIndexValues = getAllIndexValuesForVersionstamp(stringIndexSubspace, 300);
        List<Object> intIndexValues = getAllIndexValuesForVersionstamp(intIndexSubspace, 300);
        List<Object> doubleIndexValues = getAllIndexValuesForVersionstamp(doubleIndexSubspace, 300);

        assertEquals(1, stringIndexValues.size(), "String index should have one back pointer");
        assertEquals(1, intIndexValues.size(), "Int index should have one back pointer");
        assertEquals(1, doubleIndexValues.size(), "Double index should have one back pointer");

        assertEquals(stringValue, stringIndexValues.getFirst());
        assertEquals(intValue.longValue(), intIndexValues.get(0));
        assertEquals(doubleValue, doubleIndexValues.get(0));
    }

    @Test
    void shouldHandleMultipleEntriesWithSameVersionstamp() {
        IndexDefinition definition = IndexDefinition.create("multi-entry-index", "value", BsonType.STRING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(TEST_BUCKET, definition);

        int userVersion = 400;
        List<String> expectedValues = Arrays.asList("value1", "value2", "value3");

        for (String value : expectedValues) {
            AppendedEntry entry = createAppendedEntry(userVersion);
            setIndexEntryAndCommit(definition, metadata, value, entry);
        }

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();
        List<Object> retrievedValues = getAllIndexValuesForVersionstamp(indexSubspace, userVersion);

        assertEquals(expectedValues.size(), retrievedValues.size(), "Should retrieve all values for the versionstamp");
        for (String expectedValue : expectedValues) {
            assertTrue(retrievedValues.contains(expectedValue), "Should contain value: " + expectedValue);
        }
    }

    @Test
    void shouldCreateBackPointersForNullValues() {
        IndexDefinition definition = IndexDefinition.create("null-index", "nullable", BsonType.NULL);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(TEST_BUCKET, definition);
        AppendedEntry entry = createAppendedEntry(500);

        setIndexEntryAndCommit(definition, metadata, null, entry);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();
        verifyBackPointer(indexSubspace, null, 500);
    }
}