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
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.TestUtil;
import com.kronotop.volume.AppendedEntry;
import com.kronotop.volume.VolumeTestUtil;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class IndexBuilderTest extends BaseStandaloneInstanceTest {
    final int SHARD_ID = 1;

    /**
     * Provides test data for all BqlValue/BsonType combinations
     */
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

    private AppendedEntry[] getAppendedEntries() {
        AppendedEntry[] entries = new AppendedEntry[3];
        byte[] encodedEntryMetadata = getEncodedEntryMetadata();
        for (int index = 0; index < entries.length; index++) {
            AppendedEntry entry = new AppendedEntry(index, index, null, encodedEntryMetadata);
            entries[index] = entry;
        }
        return entries;
    }

    BucketMetadata createIndexAndLoadBucketMetadata(IndexDefinition definition, String bucket) {
        createIndexThenWaitForReadiness(TEST_NAMESPACE, bucket, definition);
        // Refresh the index registry
        return refreshBucketMetadata(TEST_NAMESPACE, bucket);
    }

    /**
     * Helper method to set an index entry and commit the transaction
     */
    private void setIndexEntryAndCommit(IndexDefinition definition, BucketMetadata metadata, Object indexValue, AppendedEntry entry) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilder.setIndexEntry(tr, definition, SHARD_ID, metadata, indexValue, entry);
            tr.commit().join();
        }
    }

    /**
     * Helper method to verify index entry structure and content
     */
    private void verifyIndexEntry(DirectorySubspace indexSubspace, Object expectedIndexValue, byte[] expectedEntryMetadata) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexedEntries = tr.getRange(begin, end).asList().join();
            assertEquals(1, indexedEntries.size(), "Should have exactly one index entry");

            KeyValue indexEntry = indexedEntries.get(0);

            // Verify the index key structure
            Tuple unpackedIndex = indexSubspace.unpack(indexEntry.getKey());
            assertEquals((long) IndexSubspaceMagic.ENTRIES.getValue(), unpackedIndex.get(0), "Magic value should match");

            Object actualIndexValue = unpackedIndex.get(1);
            if (expectedIndexValue instanceof byte[]) {
                assertArrayEquals((byte[]) expectedIndexValue, (byte[]) actualIndexValue, "Binary index value should match");
            } else {
                assertEquals(expectedIndexValue, actualIndexValue, "Index value should match");
            }

            Versionstamp versionstamp = (Versionstamp) unpackedIndex.get(2);
            assertEquals(0, versionstamp.getUserVersion(), "User version should match entry");

            // Verify the index entry value
            IndexEntry decodedEntry = IndexEntry.decode(indexEntry.getValue());
            assertEquals(SHARD_ID, decodedEntry.shardId(), "Shard ID should match");
            assertArrayEquals(expectedEntryMetadata, decodedEntry.entryMetadata(), "Entry metadata should match");
        }
    }

    @ParameterizedTest
    @MethodSource("indexValueTestData")
    void shouldSetIndexValueForAllBsonTypes(String indexName, String fieldName, BsonType bsonType, Object inputValue, Object expectedStoredValue) {
        IndexDefinition definition = IndexDefinition.create(indexName, fieldName, bsonType);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();

        setIndexEntryAndCommit(definition, metadata, inputValue, entries[0]);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist for " + bsonType);
        DirectorySubspace indexSubspace = index.subspace();

        byte[] expectedEntryMetadata = getEncodedEntryMetadata();
        verifyIndexEntry(indexSubspace, expectedStoredValue, expectedEntryMetadata);

        // Verify cardinality was incremented
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(1, statistics.cardinality(), "Index cardinality should be 1 after setting one entry for " + bsonType);
        }
    }

    @Test
    void shouldSetDefaultIDIndex() {
        AppendedEntry[] entries = getAppendedEntries();
        assertDoesNotThrow(() -> {
            BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexBuilder.setPrimaryIndexEntry(tr, SHARD_ID, metadata, entries);
                tr.commit().join();
            }

            // Verify cardinality was set correctly
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), DefaultIndexDefinition.ID.id());
                assertEquals(entries.length, statistics.cardinality(), "Primary index cardinality should equal number of entries set");
            }
        });
    }

    @Test
    void shouldReadEntriesFromIdIndex() {
        AppendedEntry[] entries = getAppendedEntries();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilder.setPrimaryIndexEntry(tr, SHARD_ID, metadata, entries);
            tr.commit().join();
        }

        Index idIndex = metadata.indexes().getIndex(DefaultIndexDefinition.ID.selector(), IndexSelectionPolicy.READ);
        assertNotNull(idIndex, "Index should exist");
        DirectorySubspace idIndexSubspace = idIndex.subspace();
        byte[] prefix = idIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        byte[] expectedEntryMetadata = getEncodedEntryMetadata();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexedEntries = tr.getRange(begin, end).asList().join();
            assertEquals(3, indexedEntries.size());

            for (int i = 0; i < indexedEntries.size(); i++) {
                KeyValue entry = indexedEntries.get(i);

                Tuple unpackedIndex = idIndexSubspace.unpack(entry.getKey());
                Versionstamp key = (Versionstamp) unpackedIndex.get(1);
                assertEquals(i, key.getUserVersion());

                IndexEntry indexEntry = IndexEntry.decode(entry.getValue());
                assertEquals(SHARD_ID, indexEntry.shardId());
                assertArrayEquals(expectedEntryMetadata, indexEntry.entryMetadata());
            }
        }
    }

    @Test
    void shouldDropIndexEntry() {
        IndexDefinition definition = IndexDefinition.create("test-index", "name", BsonType.STRING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();
        String indexValue = "test-value";

        // First set the index entry
        setIndexEntryAndCommit(definition, metadata, indexValue, entries[0]);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();
        DirectorySubspace metadataSubspace = metadata.subspace();

        // Get the actual versionstamp from the committed index entry
        Versionstamp actualVersionstamp = null;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] indexPrefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> beforeEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(indexPrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(indexPrefix))
            ).asList().join();
            assertEquals(1, beforeEntries.size(), "Should have one entry before dropping");

            // Extract the versionstamp from the index entry key
            KeyValue indexEntry = beforeEntries.get(0);
            Tuple unpackedIndex = indexSubspace.unpack(indexEntry.getKey());
            actualVersionstamp = (Versionstamp) unpackedIndex.get(2); // versionstamp is at position 2

            byte[] backPointerPrefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
            List<KeyValue> beforeBackPointers = tr.getRange(
                    KeySelector.firstGreaterOrEqual(backPointerPrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(backPointerPrefix))
            ).asList().join();
            assertEquals(1, beforeBackPointers.size(), "Should have one back pointer before dropping");
        }

        assertNotNull(actualVersionstamp, "Should have found the versionstamp");

        // Drop the index entry using the actual versionstamp
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilder.dropIndexEntry(tr, actualVersionstamp, definition, indexSubspace, metadataSubspace);
            tr.commit().join();
        }

        // Verify the entry and back pointer are removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] indexPrefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> afterEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(indexPrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(indexPrefix))
            ).asList().join();
            assertEquals(0, afterEntries.size(), "Should have no entries after dropping");

            byte[] backPointerPrefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
            List<KeyValue> afterBackPointers = tr.getRange(
                    KeySelector.firstGreaterOrEqual(backPointerPrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(backPointerPrefix))
            ).asList().join();
            assertEquals(0, afterBackPointers.size(), "Should have no back pointers after dropping");

            // Verify the cardinality was decremented
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(0, statistics.cardinality(), "Index cardinality should be 0 after dropping the entry");
        }
    }

    @ParameterizedTest
    @MethodSource("indexValueTestData")
    void shouldDropIndexEntryForAllBsonTypes(String indexName, String fieldName, BsonType bsonType, Object inputValue, Object expectedStoredValue) {
        IndexDefinition definition = IndexDefinition.create(indexName, fieldName, bsonType);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();

        // Set the index entry
        setIndexEntryAndCommit(definition, metadata, inputValue, entries[0]);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();
        DirectorySubspace metadataSubspace = metadata.subspace();

        // Retrieve the actual versionstamp from the committed index entry
        Versionstamp actualVersionstamp;
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(1, indexEntries.size(), "Should have one index entry before dropping");
            Tuple unpacked = indexSubspace.unpack(indexEntries.get(0).getKey());
            actualVersionstamp = (Versionstamp) unpacked.get(2);
        }

        // Drop the index entry
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilder.dropIndexEntry(tr, actualVersionstamp, definition, indexSubspace, metadataSubspace);
            tr.commit().join();
        }

        // Verify both index entry and back pointer are removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] indexPrefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> indexEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(indexPrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(indexPrefix))
            ).asList().join();
            assertEquals(0, indexEntries.size(), "Should have no index entries after dropping for " + bsonType);

            byte[] backPointerPrefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
            List<KeyValue> backPointers = tr.getRange(
                    KeySelector.firstGreaterOrEqual(backPointerPrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(backPointerPrefix))
            ).asList().join();
            assertEquals(0, backPointers.size(), "Should have no back pointers after dropping for " + bsonType);

            // Verify the cardinality was decremented
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(0, statistics.cardinality(), "Index cardinality should be 0 after dropping the entry for " + bsonType);
        }
    }

    @Test
    void shouldDropMultipleIndexEntriesForSameVersionstamp() {
        IndexDefinition stringIndex = IndexDefinition.create("string-index", "name", BsonType.STRING);
        IndexDefinition intIndex = IndexDefinition.create("int-index", "age", BsonType.INT32);

        createIndexThenWaitForReadiness(stringIndex, intIndex);
        // Refresh metadata
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        AppendedEntry[] entries = getAppendedEntries();
        String stringValue = "multi-drop-test";
        Integer intValue = 42;

        // Set entries for both indexes with the same versionstamp
        setIndexEntryAndCommit(stringIndex, metadata, stringValue, entries[0]);
        setIndexEntryAndCommit(intIndex, metadata, intValue, entries[0]);

        Index stringIndexObj = metadata.indexes().getIndex(stringIndex.selector(), IndexSelectionPolicy.READ);
        assertNotNull(stringIndexObj, "String index should exist");
        DirectorySubspace stringIndexSubspace = stringIndexObj.subspace();
        Index intIndexObj = metadata.indexes().getIndex(intIndex.selector(), IndexSelectionPolicy.READ);
        assertNotNull(intIndexObj, "Int index should exist");
        DirectorySubspace intIndexSubspace = intIndexObj.subspace();
        DirectorySubspace metadataSubspace = metadata.subspace();

        // Verify both entries exist
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] stringPrefix = stringIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> stringEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(stringPrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(stringPrefix))
            ).asList().join();
            assertEquals(1, stringEntries.size(), "Should have string index entry");

            byte[] intPrefix = intIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> intEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(intPrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(intPrefix))
            ).asList().join();
            assertEquals(1, intEntries.size(), "Should have int index entry");
        }

        // Retrieve actual versionstamps from both indexes
        Versionstamp stringVersionstamp, intVersionstamp;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] stringPrefix = stringIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> stringEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(stringPrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(stringPrefix))
            ).asList().join();
            assertEquals(1, stringEntries.size(), "Should have string index entry");
            Tuple stringUnpacked = stringIndexSubspace.unpack(stringEntries.get(0).getKey());
            stringVersionstamp = (Versionstamp) stringUnpacked.get(2);

            byte[] intPrefix = intIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> intEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(intPrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(intPrefix))
            ).asList().join();
            assertEquals(1, intEntries.size(), "Should have int index entry");
            Tuple intUnpacked = intIndexSubspace.unpack(intEntries.get(0).getKey());
            intVersionstamp = (Versionstamp) intUnpacked.get(2);
        }

        // Drop entries from the string index
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilder.dropIndexEntry(tr, stringVersionstamp, stringIndex, stringIndexSubspace, metadataSubspace);
            tr.commit().join();
        }

        // Drop entries from the int index
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilder.dropIndexEntry(tr, intVersionstamp, intIndex, intIndexSubspace, metadataSubspace);
            tr.commit().join();
        }

        // Verify all entries are removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] stringPrefix = stringIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> stringEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(stringPrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(stringPrefix))
            ).asList().join();
            assertEquals(0, stringEntries.size(), "Should have no string index entries");

            byte[] intPrefix = intIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> intEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(intPrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(intPrefix))
            ).asList().join();
            assertEquals(0, intEntries.size(), "Should have no int index entries");

            // Verify cardinality was decremented for both indexes
            IndexStatistics stringStats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), stringIndex.id());
            assertEquals(0, stringStats.cardinality(), "String index cardinality should be 0 after dropping the entry");

            IndexStatistics intStats = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), intIndex.id());
            assertEquals(0, intStats.cardinality(), "Int index cardinality should be 0 after dropping the entry");
        }
    }

    @Test
    void shouldHandleDropIndexEntryForNonExistentVersionstamp() {
        IndexDefinition definition = IndexDefinition.create("test-index", "name", BsonType.STRING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();
        DirectorySubspace metadataSubspace = metadata.subspace();

        // Try to drop entry for non-existent versionstamp
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Versionstamp nonExistentVersionstamp = Versionstamp.complete(new byte[10], 999);
            assertDoesNotThrow(() -> {
                IndexBuilder.dropIndexEntry(tr, nonExistentVersionstamp, definition, indexSubspace, metadataSubspace);
                tr.commit().join();
            }, "Should not throw exception for non-existent versionstamp");
        }

        // Verify cardinality remains 0 (no entry was actually dropped)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(0, statistics.cardinality(), "Index cardinality should remain 0 when dropping non-existent entry");
        }
    }

    @Test
    void shouldDropOnlySpecificVersionstampEntries() {
        IndexDefinition definition = IndexDefinition.create("test-index", "name", BsonType.STRING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();

        // Set multiple entries with different versionstamps
        setIndexEntryAndCommit(definition, metadata, "value1", entries[0]);
        setIndexEntryAndCommit(definition, metadata, "value2", entries[1]);
        setIndexEntryAndCommit(definition, metadata, "value3", entries[2]);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();
        DirectorySubspace metadataSubspace = metadata.subspace();

        // Verify all entries exist
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> allEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(prefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix))
            ).asList().join();
            assertEquals(3, allEntries.size(), "Should have 3 entries initially");
        }

        // Retrieve the actual versionstamp from the middle entry
        Versionstamp actualVersionstamp;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> allEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(prefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix))
            ).asList().join();
            assertEquals(3, allEntries.size(), "Should have 3 entries initially");

            // Find the entry with userVersion matching entries[1]
            actualVersionstamp = null;
            for (KeyValue kv : allEntries) {
                Tuple unpacked = indexSubspace.unpack(kv.getKey());
                Versionstamp versionstamp = (Versionstamp) unpacked.get(2);
                if (versionstamp.getUserVersion() == entries[1].userVersion()) {
                    actualVersionstamp = versionstamp;
                    break;
                }
            }
            assertNotNull(actualVersionstamp, "Should find middle entry versionstamp");
        }

        // Drop only the middle entry
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilder.dropIndexEntry(tr, actualVersionstamp, definition, indexSubspace, metadataSubspace);
            tr.commit().join();
        }

        // Verify only one entry was removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> remainingEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(prefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix))
            ).asList().join();
            assertEquals(2, remainingEntries.size(), "Should have 2 entries remaining");

            // Verify the correct entries remain (entries[0] and entries[2])
            boolean foundEntry0 = false, foundEntry2 = false;
            for (KeyValue kv : remainingEntries) {
                Tuple unpacked = indexSubspace.unpack(kv.getKey());
                Versionstamp versionstamp = (Versionstamp) unpacked.get(2);
                if (versionstamp.getUserVersion() == entries[0].userVersion()) {
                    foundEntry0 = true;
                } else if (versionstamp.getUserVersion() == entries[2].userVersion()) {
                    foundEntry2 = true;
                }
            }
            assertTrue(foundEntry0, "Entry 0 should still exist");
            assertTrue(foundEntry2, "Entry 2 should still exist");

            // Verify cardinality was decremented from 3 to 2
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(2, statistics.cardinality(), "Index cardinality should be 2 after dropping 1 out of 3 entries");
        }
    }

    @Test
    void shouldUpdateEntryMetadata() {
        IndexDefinition definition = IndexDefinition.create("test-index", "name", BsonType.STRING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();
        AppendedEntry entry = entries[0];
        String indexValue = "update-test";

        setIndexEntryAndCommit(definition, metadata, indexValue, entry);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();

        // Get the actual versionstamp from the committed entry
        Versionstamp actualVersionstamp;
        byte[] originalMetadata;
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(1, indexEntries.size(), "Should have one index entry");

            Tuple unpacked = indexSubspace.unpack(indexEntries.get(0).getKey());
            actualVersionstamp = (Versionstamp) unpacked.get(2);
            originalMetadata = indexEntries.get(0).getValue();
        }

        // Create new metadata
        byte[] newMetadata = VolumeTestUtil.generateEntryMetadata(1, 2, 1, 2, "updated").encode();
        assertFalse(Arrays.equals(originalMetadata, newMetadata), "New metadata should be different from original");

        // Update entry metadata
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilder.updateEntryMetadata(tr, actualVersionstamp, newMetadata, indexSubspace);
            tr.commit().join();
        }

        // Verify metadata was updated
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(1, indexEntries.size(), "Should still have one index entry");

            byte[] updatedMetadata = indexEntries.get(0).getValue();
            assertArrayEquals(newMetadata, updatedMetadata, "Metadata should be updated");
            assertFalse(Arrays.equals(originalMetadata, updatedMetadata), "Updated metadata should differ from original");
        }
    }

    @Test
    void shouldUpdateEntryMetadataForMultipleIndexValues() {
        IndexDefinition definition = IndexDefinition.create("multi-value-index", "tag", BsonType.STRING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);

        AppendedEntry[] entries = getAppendedEntries();
        AppendedEntry entry = entries[1];
        String[] indexValues = {"tag1", "tag2", "tag3"};

        // Set multiple index entries with the same versionstamp (simulating a document with multiple tags)
        for (String value : indexValues) {
            setIndexEntryAndCommit(definition, metadata, value, entry);
        }

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();

        // Get the actual versionstamp from one of the committed entries
        Versionstamp actualVersionstamp;
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(indexValues.length, indexEntries.size(), "Should have entries for each index value");

            Tuple unpacked = indexSubspace.unpack(indexEntries.get(0).getKey());
            actualVersionstamp = (Versionstamp) unpacked.get(2);
        }

        // Create new metadata
        byte[] newMetadata = VolumeTestUtil.generateEntryMetadata(1, 3, 2, 3, "multi-updated").encode();

        // Update entry metadata for all entries with this versionstamp
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilder.updateEntryMetadata(tr, actualVersionstamp, newMetadata, indexSubspace);
            tr.commit().join();
        }

        // Verify all entries with the same versionstamp have updated metadata
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(indexValues.length, indexEntries.size(), "Should still have all index entries");

            for (KeyValue kv : indexEntries) {
                Tuple unpacked = indexSubspace.unpack(kv.getKey());
                Versionstamp entryVersionstamp = (Versionstamp) unpacked.get(2);

                if (entryVersionstamp.equals(actualVersionstamp)) {
                    assertArrayEquals(newMetadata, kv.getValue(), "Entry metadata should be updated for versionstamp");
                }
            }
        }
    }

    @Test
    void shouldHandleUpdateEntryMetadataForNonExistentVersionstamp() {
        IndexDefinition definition = IndexDefinition.create("empty-index", "value", BsonType.STRING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();
        Versionstamp nonExistentVersionstamp = Versionstamp.complete(new byte[10], 999);
        byte[] newMetadata = VolumeTestUtil.generateEntryMetadata(1, 1, 0, 1, "test").encode();

        // Should not throw exception for a non-existent versionstamp
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> {
                IndexBuilder.updateEntryMetadata(tr, nonExistentVersionstamp, newMetadata, indexSubspace);
                tr.commit().join();
            }, "Should not throw exception for non-existent versionstamp");
        }

        // Verify no entries were created
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(0, indexEntries.size(), "Index should remain empty");
        }
    }

    @ParameterizedTest
    @MethodSource("indexValueTestData")
    void shouldUpdateEntryMetadataForAllBsonTypes(String indexName, String fieldName, BsonType bsonType, Object inputValue, Object expectedStoredValue) {
        IndexDefinition definition = IndexDefinition.create(indexName, fieldName, bsonType);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();
        AppendedEntry entry = entries[0];

        setIndexEntryAndCommit(definition, metadata, inputValue, entry);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();

        // Get the actual versionstamp
        Versionstamp actualVersionstamp;
        byte[] originalMetadata;
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(1, indexEntries.size(), "Should have one index entry for " + bsonType);

            Tuple unpacked = indexSubspace.unpack(indexEntries.get(0).getKey());
            actualVersionstamp = (Versionstamp) unpacked.get(2);
            originalMetadata = indexEntries.get(0).getValue();
        }

        // Create new metadata specific to this BSON type test
        byte[] newMetadata = VolumeTestUtil.generateEntryMetadata(1, 5, 4, 5, "updated-" + bsonType.name().toLowerCase()).encode();
        assertFalse(Arrays.equals(originalMetadata, newMetadata), "New metadata should differ from original for " + bsonType);

        // Update entry metadata
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilder.updateEntryMetadata(tr, actualVersionstamp, newMetadata, indexSubspace);
            tr.commit().join();
        }

        // Verify metadata was updated
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(1, indexEntries.size(), "Should still have one index entry for " + bsonType);

            byte[] updatedMetadata = indexEntries.get(0).getValue();
            assertArrayEquals(newMetadata, updatedMetadata, "Metadata should be updated for " + bsonType);
        }
    }

    @Test
    void shouldDropPrimaryIndexEntry() {
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();

        // First set the ID index entries
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilder.setPrimaryIndexEntry(tr, SHARD_ID, metadata, entries);
            tr.commit().join();
        }

        Index primaryIndex = metadata.indexes().getIndex(DefaultIndexDefinition.ID.selector(), IndexSelectionPolicy.READ);
        assertNotNull(primaryIndex, "Primary index should exist");
        DirectorySubspace primaryIndexSubspace = primaryIndex.subspace();

        // Get the actual versionstamp from one of the committed entries
        Versionstamp actualVersionstamp;
        byte[] prefix = primaryIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(entries.length, indexEntries.size(), "Should have entries for all appended entries");

            Tuple unpacked = primaryIndexSubspace.unpack(indexEntries.get(0).getKey());
            actualVersionstamp = (Versionstamp) unpacked.get(1); // For ID index, versionstamp is at position 1
        }

        // Drop primary index entry
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilder.dropPrimaryIndexEntry(tr, actualVersionstamp, metadata);
            tr.commit().join();
        }

        // Verify the primary index entry was dropped
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(entries.length - 1, indexEntries.size(), "Should have one less entry after dropping");

            // Verify the specific entry was removed
            boolean found = false;
            for (KeyValue kv : indexEntries) {
                Tuple unpacked = primaryIndexSubspace.unpack(kv.getKey());
                Versionstamp entryVersionstamp = (Versionstamp) unpacked.get(1);
                if (entryVersionstamp.equals(actualVersionstamp)) {
                    found = true;
                    break;
                }
            }
            assertFalse(found, "Dropped versionstamp should not be found in remaining entries");

            // Verify cardinality was decremented
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), DefaultIndexDefinition.ID.id());
            assertEquals(entries.length - 1, statistics.cardinality(), "Primary index cardinality should be decremented by 1");
        }
    }

    @Test
    void shouldUpdatePrimaryIndex() {
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();

        // First set the primary index entries
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilder.setPrimaryIndexEntry(tr, SHARD_ID, metadata, entries);
            tr.commit().join();
        }

        Index primaryIndex = metadata.indexes().getIndex(DefaultIndexDefinition.ID.selector(), IndexSelectionPolicy.READ);
        assertNotNull(primaryIndex, "Primary index should exist");
        DirectorySubspace primaryIndexSubspace = primaryIndex.subspace();

        // Get the actual versionstamp from one of the committed entries
        Versionstamp actualVersionstamp;
        byte[] originalIndexEntry;
        byte[] prefix = primaryIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(entries.length, indexEntries.size(), "Should have entries for all appended entries");

            Tuple unpacked = primaryIndexSubspace.unpack(indexEntries.get(0).getKey());
            actualVersionstamp = (Versionstamp) unpacked.get(1); // For ID index, versionstamp is at position 1
            originalIndexEntry = indexEntries.get(0).getValue();
        }

        // Create new metadata for update
        byte[] newMetadata = VolumeTestUtil.generateEntryMetadata(1, 2, 1, 2, "updated-primary").encode();
        assertFalse(Arrays.equals(originalIndexEntry, newMetadata), "New metadata should be different from original");

        // Update primary index entry metadata
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilder.updatePrimaryIndex(tr, actualVersionstamp, metadata, SHARD_ID, newMetadata);
            tr.commit().join();
        }

        // Verify the primary index entry metadata was updated
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(entries.length, indexEntries.size(), "Should still have same number of entries");

            // Find the updated entry and verify its metadata
            boolean found = false;
            for (KeyValue kv : indexEntries) {
                Tuple unpacked = primaryIndexSubspace.unpack(kv.getKey());
                Versionstamp entryVersionstamp = (Versionstamp) unpacked.get(1);
                if (entryVersionstamp.equals(actualVersionstamp)) {
                    IndexEntry indexEntry = new IndexEntry(SHARD_ID, newMetadata);
                    assertArrayEquals(indexEntry.encode(), kv.getValue(), "Entry metadata should be updated");
                    assertFalse(Arrays.equals(originalIndexEntry, kv.getValue()), "Updated metadata should differ from original");
                    found = true;
                    break;
                }
            }
            assertTrue(found, "Updated entry should be found in primary index");
        }
    }

    @Test
    void shouldSetIndexEntryByVersionstamp() {
        IndexDefinition definition = IndexDefinition.create("test-index", "name", BsonType.STRING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);

        String indexValue = "test-value";
        int userVersion = 42;
        Versionstamp versionstamp = TestUtil.generateVersionstamp(userVersion);
        byte[] entryMetadata = getEncodedEntryMetadata();

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();

        IndexEntryContainer container = new IndexEntryContainer(
                metadata,
                indexValue,
                definition,
                indexSubspace,
                SHARD_ID,
                entryMetadata
        );

        // Set index entry using versionstamp
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilder.setIndexEntryByVersionstamp(tr, versionstamp, container);
            tr.commit().join();
        }

        // Verify the index entry was created correctly
        byte[] entryPrefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector entryBegin = KeySelector.firstGreaterOrEqual(entryPrefix);
        KeySelector entryEnd = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(entryPrefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(entryBegin, entryEnd).asList().join();
            assertEquals(1, indexEntries.size(), "Should have exactly one index entry");

            KeyValue entry = indexEntries.get(0);
            Tuple unpackedKey = indexSubspace.unpack(entry.getKey());

            assertEquals((long) IndexSubspaceMagic.ENTRIES.getValue(), unpackedKey.get(0), "Magic value should match");
            assertEquals(indexValue, unpackedKey.get(1), "Index value should match");

            Versionstamp actualVersionstamp = (Versionstamp) unpackedKey.get(2);
            assertEquals(userVersion, actualVersionstamp.getUserVersion(), "User version should match");

            IndexEntry indexEntry = IndexEntry.decode(entry.getValue());
            assertEquals(SHARD_ID, indexEntry.shardId(), "Shard ID should match");
            assertArrayEquals(entryMetadata, indexEntry.entryMetadata(), "Entry metadata should match");
        }

        // Verify the back pointer was created correctly
        byte[] backPointerPrefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
        KeySelector backPointerBegin = KeySelector.firstGreaterOrEqual(backPointerPrefix);
        KeySelector backPointerEnd = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(backPointerPrefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> backPointers = tr.getRange(backPointerBegin, backPointerEnd).asList().join();
            assertEquals(1, backPointers.size(), "Should have exactly one back pointer");

            KeyValue backPointer = backPointers.get(0);
            Tuple unpackedBackPointer = indexSubspace.unpack(backPointer.getKey());

            assertEquals((long) IndexSubspaceMagic.BACK_POINTER.getValue(), unpackedBackPointer.get(0), "Back pointer magic value should match");

            Versionstamp backPointerVersionstamp = (Versionstamp) unpackedBackPointer.get(1);
            assertEquals(userVersion, backPointerVersionstamp.getUserVersion(), "Back pointer versionstamp should match");
            assertEquals(indexValue, unpackedBackPointer.get(2), "Back pointer index value should match");

            assertArrayEquals(IndexBuilder.NULL_VALUE, backPointer.getValue(), "Back pointer value should be NULL_VALUE");
        }

        // Verify the cardinality was updated
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(1, statistics.cardinality(), "Index cardinality should be 1 after adding one entry");
        }
    }
}