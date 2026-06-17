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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.TestUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.CollatorCache;
import com.kronotop.volume.AppendedEntry;
import com.kronotop.volume.VolumeTestUtil;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class SingleFieldIndexMaintainerTest extends BaseIndexMaintainerTest {

    /**
     * Provides test data for all BqlValue/BsonType combinations
     */
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

    BucketMetadata createIndexAndLoadBucketMetadata(SingleFieldIndexDefinition definition, String bucket) {
        createIndexThenWaitForReadiness(TEST_NAMESPACE, bucket, definition);
        // Refresh the index registry
        return refreshBucketMetadata(TEST_NAMESPACE, bucket);
    }

    /**
     * Helper method to set an index entry and commit the transaction
     */
    void setIndexEntryAndCommit(SingleFieldIndexDefinition definition, BucketMetadata metadata, Object indexValue, ObjectId objectId, AppendedEntry entry) {
        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READWRITE);
        byte[] objectIdBytes = objectId.toByteArray();
        byte[] encodedIndexEntry = new IndexEntry(SHARD_ID, entry.metadataBytes()).encode();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SingleFieldIndexMaintainer.setEntry(tr, index, metadata, indexValue, objectIdBytes, encodedIndexEntry, new CollatorCache());
            tr.commit().join();
        }
    }

    /**
     * Helper method to verify index entry structure and content
     */
    void verifyIndexEntry(DirectorySubspace indexSubspace, Object expectedIndexValue, ObjectId expectedObjectId, byte[] expectedEntryMetadata) {
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

            // Verify ObjectId in index key
            byte[] objectIdBytes = (byte[]) unpackedIndex.get(2);
            ObjectId actualObjectId = new ObjectId(objectIdBytes);
            assertEquals(expectedObjectId, actualObjectId, "ObjectId should match");

            // Verify the index entry value
            IndexEntry decodedEntry = IndexEntry.decode(indexEntry.getValue());
            assertEquals(SHARD_ID, decodedEntry.shardId(), "Shard ID should match");
            assertArrayEquals(expectedEntryMetadata, decodedEntry.entryMetadata(), "Entry metadata should match");
        }
    }

    void insertIndexEntryAndCommit(SingleFieldIndexDefinition definition, BucketMetadata metadata,
                                   Object indexValue, ObjectId objectId, int shardId, byte[] entry) {
        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READWRITE);
        byte[] objectIdBytes = objectId.toByteArray();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SingleFieldIndexMaintainer.insertEntry(tr, index, metadata,
                    objectIdBytes, indexValue, shardId, entry, new CollatorCache());
            tr.commit().join();
        }
    }

    @ParameterizedTest
    @MethodSource("indexValueTestData")
    void shouldSetSingleFieldIndexValueForAllBsonTypes(String indexName, String fieldName, BsonType bsonType, Object inputValue, Object expectedStoredValue) {
        // Behavior: Verifies that setEntry creates an index entry and back pointer for each supported BSON type.
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(indexName, fieldName, bsonType, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();
        ObjectId objectId = new ObjectId();

        setIndexEntryAndCommit(definition, metadata, inputValue, objectId, entries[0]);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist for " + bsonType);
        DirectorySubspace indexSubspace = index.subspace();

        byte[] expectedEntryMetadata = getEncodedEntryMetadata();
        verifyIndexEntry(indexSubspace, expectedStoredValue, objectId, expectedEntryMetadata);

        // Verify cardinality was incremented
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(1, statistics.cardinality(), "Index cardinality should be 1 after setting one entry for " + bsonType);
        }

        // Verify the back pointer was created
        byte[] backPointerPrefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> backPointers = tr.getRange(
                    KeySelector.firstGreaterOrEqual(backPointerPrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(backPointerPrefix))
            ).asList().join();
            assertEquals(1, backPointers.size(), "Should have exactly one back pointer for " + bsonType);
        }
    }

    @Test
    void shouldDropSingleFieldIndexEntry() {
        // Behavior: Verifies that dropEntry removes the index entry, back pointer, and decrements cardinality.
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("test-index", "name", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();
        String indexValue = "test-value";
        ObjectId objectId = new ObjectId();

        // First set the index entry
        setIndexEntryAndCommit(definition, metadata, indexValue, objectId, entries[0]);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();
        DirectorySubspace metadataSubspace = metadata.subspace();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] indexPrefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            List<KeyValue> beforeEntries = tr.getRange(
                    KeySelector.firstGreaterOrEqual(indexPrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(indexPrefix))
            ).asList().join();
            assertEquals(1, beforeEntries.size(), "Should have one entry before dropping");

            byte[] backPointerPrefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
            List<KeyValue> beforeBackPointers = tr.getRange(
                    KeySelector.firstGreaterOrEqual(backPointerPrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(backPointerPrefix))
            ).asList().join();
            assertEquals(1, beforeBackPointers.size(), "Should have one back pointer before dropping");
        }

        // Drop the index entry using the ObjectId
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SingleFieldIndexMaintainer.dropEntry(tr, objectId.toByteArray(), definition, indexSubspace, metadataSubspace);
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
    void shouldDropSingleFieldIndexEntryForAllBsonTypes(String indexName, String fieldName, BsonType bsonType, Object inputValue) {
        // Behavior: Verifies that dropEntry correctly removes index entries and back pointers for each supported BSON type.
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(indexName, fieldName, bsonType, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();
        ObjectId objectId = new ObjectId();

        // Set the index entry
        setIndexEntryAndCommit(definition, metadata, inputValue, objectId, entries[0]);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();
        DirectorySubspace metadataSubspace = metadata.subspace();

        // Verify entry exists before dropping
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(1, indexEntries.size(), "Should have one index entry before dropping");
        }

        // Drop the index entry
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SingleFieldIndexMaintainer.dropEntry(tr, objectId.toByteArray(), definition, indexSubspace, metadataSubspace);
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
    void shouldDropMultipleSingleFieldIndexEntriesForSameObjectId() {
        // Behavior: Verifies that dropEntry removes entries from multiple indexes independently for the same ObjectId.
        SingleFieldIndexDefinition stringIndex = SingleFieldIndexDefinition.create("string-index", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition intIndex = SingleFieldIndexDefinition.create("int-index", "age", BsonType.INT32, false, IndexStatus.WAITING);

        createIndexThenWaitForReadiness(stringIndex, intIndex);
        // Refresh metadata
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        AppendedEntry[] entries = getAppendedEntries();
        String stringValue = "multi-drop-test";
        Integer intValue = 42;
        ObjectId objectId = new ObjectId();

        // Set entries for both indexes with the same ObjectId
        setIndexEntryAndCommit(stringIndex, metadata, stringValue, objectId, entries[0]);
        setIndexEntryAndCommit(intIndex, metadata, intValue, objectId, entries[0]);

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

        // Drop entries from the string index
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SingleFieldIndexMaintainer.dropEntry(tr, objectId.toByteArray(), stringIndex, stringIndexSubspace, metadataSubspace);
            tr.commit().join();
        }

        // Drop entries from the int index
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SingleFieldIndexMaintainer.dropEntry(tr, objectId.toByteArray(), intIndex, intIndexSubspace, metadataSubspace);
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
    void shouldHandleDropSingleFieldIndexEntryForNonExistentObjectId() {
        // Behavior: Verifies that dropEntry does not throw for a non-existent ObjectId and cardinality remains unchanged.
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("test-index", "name", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();
        DirectorySubspace metadataSubspace = metadata.subspace();

        // Try to drop entry for non-existent ObjectId
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ObjectId nonExistentObjectId = new ObjectId();
            assertDoesNotThrow(() -> {
                SingleFieldIndexMaintainer.dropEntry(tr, nonExistentObjectId.toByteArray(), definition, indexSubspace, metadataSubspace);
                tr.commit().join();
            }, "Should not throw exception for non-existent ObjectId");
        }

        // Verify cardinality remains 0 (no entry was actually dropped)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(0, statistics.cardinality(), "Index cardinality should remain 0 when dropping non-existent entry");
        }
    }

    @Test
    void shouldDropOnlySpecificObjectIdEntries() {
        // Behavior: Verifies that dropEntry removes only the targeted ObjectId's entries, leaving other entries intact.
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("test-index", "name", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();
        ObjectId objectId0 = new ObjectId();
        ObjectId objectId1 = new ObjectId();
        ObjectId objectId2 = new ObjectId();

        // Set multiple entries with different ObjectIds
        setIndexEntryAndCommit(definition, metadata, "value1", objectId0, entries[0]);
        setIndexEntryAndCommit(definition, metadata, "value2", objectId1, entries[1]);
        setIndexEntryAndCommit(definition, metadata, "value3", objectId2, entries[2]);

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

        // Drop only the middle entry
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SingleFieldIndexMaintainer.dropEntry(tr, objectId1.toByteArray(), definition, indexSubspace, metadataSubspace);
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

            // Verify the correct entries remain (objectId0 and objectId2)
            boolean foundEntry0 = false, foundEntry2 = false;
            for (KeyValue kv : remainingEntries) {
                Tuple unpacked = indexSubspace.unpack(kv.getKey());
                byte[] objectIdBytes = (byte[]) unpacked.get(2);
                ObjectId objectId = new ObjectId(objectIdBytes);
                if (objectId.equals(objectId0)) {
                    foundEntry0 = true;
                } else if (objectId.equals(objectId2)) {
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
    void shouldUpdateIndexEntry() {
        // Behavior: Verifies that updateIndexEntry replaces entry metadata without changing the indexed key.
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("test-index", "name", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();
        AppendedEntry entry = entries[0];
        String indexValue = "update-test";
        ObjectId objectId = new ObjectId();

        setIndexEntryAndCommit(definition, metadata, indexValue, objectId, entry);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();

        // Get the original entry metadata from the committed entry
        byte[] originalEntryMetadata;
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(1, indexEntries.size(), "Should have one index entry");
            IndexEntry originalIndexEntry = IndexEntry.decode(indexEntries.get(0).getValue());
            originalEntryMetadata = originalIndexEntry.entryMetadata();
        }

        // Create new metadata
        byte[] newEntryMetadata = VolumeTestUtil.generateEntryMetadata(1, 2, 1, 2, "updated").encode();
        assertFalse(Arrays.equals(originalEntryMetadata, newEntryMetadata), "New metadata should be different from original");

        // Update entry metadata
        IndexEntry newIndexEntry = new IndexEntry(SHARD_ID, newEntryMetadata);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SingleFieldIndexMaintainer.updateIndexEntry(tr, objectId.toByteArray(), newIndexEntry.encode(), indexSubspace);
            tr.commit().join();
        }

        // Verify metadata was updated
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(1, indexEntries.size(), "Should still have one index entry");

            IndexEntry updatedIndexEntry = IndexEntry.decode(indexEntries.get(0).getValue());
            assertEquals(SHARD_ID, updatedIndexEntry.shardId(), "Shard ID should match");
            assertArrayEquals(newEntryMetadata, updatedIndexEntry.entryMetadata(), "Entry metadata should be updated");
        }
    }

    @Test
    void shouldUpdateIndexEntryForMultipleIndexValues() {
        // Behavior: Verifies that updateIndexEntry updates all index entries sharing the same ObjectId across multiple index values.
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("multi-value-index", "tag", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);

        AppendedEntry[] entries = getAppendedEntries();
        AppendedEntry entry = entries[1];
        String[] indexValues = {"tag1", "tag2", "tag3"};
        ObjectId objectId = new ObjectId();

        // Set multiple index entries with the same ObjectId (simulating a document with multiple tags)
        for (String value : indexValues) {
            setIndexEntryAndCommit(definition, metadata, value, objectId, entry);
        }

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();

        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(indexValues.length, indexEntries.size(), "Should have entries for each index value");
        }

        // Create new metadata
        byte[] newEntryMetadata = VolumeTestUtil.generateEntryMetadata(1, 3, 2, 3, "multi-updated").encode();

        // Update entry metadata for all entries with this ObjectId
        IndexEntry newIndexEntry = new IndexEntry(SHARD_ID, newEntryMetadata);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SingleFieldIndexMaintainer.updateIndexEntry(tr, objectId.toByteArray(), newIndexEntry.encode(), indexSubspace);
            tr.commit().join();
        }

        // Verify all entries with the same ObjectId have updated metadata
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(indexValues.length, indexEntries.size(), "Should still have all index entries");

            for (KeyValue kv : indexEntries) {
                Tuple unpacked = indexSubspace.unpack(kv.getKey());
                byte[] entryObjectIdBytes = (byte[]) unpacked.get(2);
                ObjectId entryObjectId = new ObjectId(entryObjectIdBytes);

                if (entryObjectId.equals(objectId)) {
                    IndexEntry decodedEntry = IndexEntry.decode(kv.getValue());
                    assertEquals(SHARD_ID, decodedEntry.shardId(), "Shard ID should match");
                    assertArrayEquals(newEntryMetadata, decodedEntry.entryMetadata(), "Entry metadata should be updated for ObjectId");
                }
            }
        }
    }

    @Test
    void shouldHandleUpdateIndexEntryForNonExistentObjectId() {
        // Behavior: Verifies that updateIndexEntry does not throw or create entries for a non-existent ObjectId.
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("empty-index", "value", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();
        ObjectId nonExistentObjectId = new ObjectId();
        byte[] newEntryMetadata = VolumeTestUtil.generateEntryMetadata(1, 1, 0, 1, "test").encode();
        IndexEntry newIndexEntry = new IndexEntry(SHARD_ID, newEntryMetadata);

        // Should not throw exception for a non-existent ObjectId
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> {
                SingleFieldIndexMaintainer.updateIndexEntry(tr, nonExistentObjectId.toByteArray(), newIndexEntry.encode(), indexSubspace);
                tr.commit().join();
            }, "Should not throw exception for non-existent ObjectId");
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
    void shouldUpdateIndexEntryForAllBsonTypes(String indexName, String fieldName, BsonType bsonType, Object inputValue) {
        // Behavior: Verifies that updateIndexEntry correctly updates entry metadata for each supported BSON type.
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(indexName, fieldName, bsonType, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();
        AppendedEntry entry = entries[0];
        ObjectId objectId = new ObjectId();

        setIndexEntryAndCommit(definition, metadata, inputValue, objectId, entry);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();

        // Get the original entry metadata
        byte[] originalEntryMetadata;
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(1, indexEntries.size(), "Should have one index entry for " + bsonType);
            IndexEntry originalIndexEntry = IndexEntry.decode(indexEntries.get(0).getValue());
            originalEntryMetadata = originalIndexEntry.entryMetadata();
        }

        // Create new metadata specific to this BSON type test
        byte[] newEntryMetadata = VolumeTestUtil.generateEntryMetadata(1, 5, 4, 5, "updated-" + bsonType.name().toLowerCase()).encode();
        assertFalse(Arrays.equals(originalEntryMetadata, newEntryMetadata), "New metadata should differ from original for " + bsonType);

        // Update entry metadata
        IndexEntry newIndexEntry = new IndexEntry(SHARD_ID, newEntryMetadata);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SingleFieldIndexMaintainer.updateIndexEntry(tr, objectId.toByteArray(), newIndexEntry.encode(), indexSubspace);
            tr.commit().join();
        }

        // Verify metadata was updated
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(1, indexEntries.size(), "Should still have one index entry for " + bsonType);

            IndexEntry updatedIndexEntry = IndexEntry.decode(indexEntries.get(0).getValue());
            assertEquals(SHARD_ID, updatedIndexEntry.shardId(), "Shard ID should match for " + bsonType);
            assertArrayEquals(newEntryMetadata, updatedIndexEntry.entryMetadata(), "Entry metadata should be updated for " + bsonType);
        }
    }

    @Test
    void shouldSetSingleFieldIndexEntryByObjectId() {
        // Behavior: Verifies that setEntryByObjectId creates an index entry, back pointer,
        // and cardinality update.
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("test-index", "name", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);

        String indexValue = "test-value";
        ObjectId objectId = new ObjectId();
        byte[] entryMetadata = getEncodedEntryMetadata();
        Versionstamp versionstamp = TestUtil.generateVersionstamp(1);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();

        IndexEntryContainer container = new IndexEntryContainer(
                metadata,
                indexValue,
                definition,
                indexSubspace,
                SHARD_ID,
                entryMetadata,
                versionstamp
        );

        // Set the index entry using ObjectId
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SingleFieldIndexMaintainer.setEntryByObjectId(tr, objectId.toByteArray(), container, new CollatorCache());
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

            byte[] actualObjectIdBytes = (byte[]) unpackedKey.get(2);
            ObjectId actualObjectId = new ObjectId(actualObjectIdBytes);
            assertEquals(objectId, actualObjectId, "ObjectId should match");

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

            byte[] backPointerObjectIdBytes = (byte[]) unpackedBackPointer.get(1);
            ObjectId backPointerObjectId = new ObjectId(backPointerObjectIdBytes);
            assertEquals(objectId, backPointerObjectId, "Back pointer ObjectId should match");
            assertEquals(indexValue, unpackedBackPointer.get(2), "Back pointer index value should match");

            assertArrayEquals(IndexMaintainer.NULL_VALUE, backPointer.getValue(), "Back pointer value should be NULL_VALUE");
        }

        // Verify the cardinality was updated
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(1, statistics.cardinality(), "Index cardinality should be 1 after adding one entry");
        }
    }

    @Test
    void shouldSetSingleFieldIndexEntryByObjectIdWithObjectIdValue() {
        // Behavior: Verifies that setEntryByObjectId correctly converts an ObjectId index value
        // to its byte array representation when creating the index entry and back pointer.
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("test-index", "ref_id", BsonType.OBJECT_ID, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);

        ObjectId indexValue = new ObjectId();
        ObjectId objectId = new ObjectId();
        byte[] entryMetadata = getEncodedEntryMetadata();
        Versionstamp versionstamp = TestUtil.generateVersionstamp(1);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();

        IndexEntryContainer container = new IndexEntryContainer(
                metadata,
                indexValue,
                definition,
                indexSubspace,
                SHARD_ID,
                entryMetadata,
                versionstamp
        );

        // Set the index entry using ObjectId
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SingleFieldIndexMaintainer.setEntryByObjectId(tr, objectId.toByteArray(), container, new CollatorCache());
            tr.commit().join();
        }

        // Verify the index entry was created correctly with ObjectId converted to byte[]
        byte[] entryPrefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector entryBegin = KeySelector.firstGreaterOrEqual(entryPrefix);
        KeySelector entryEnd = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(entryPrefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(entryBegin, entryEnd).asList().join();
            assertEquals(1, indexEntries.size(), "Should have exactly one index entry");

            KeyValue entry = indexEntries.get(0);
            Tuple unpackedKey = indexSubspace.unpack(entry.getKey());

            assertEquals((long) IndexSubspaceMagic.ENTRIES.getValue(), unpackedKey.get(0), "Magic value should match");
            assertArrayEquals(indexValue.toByteArray(), (byte[]) unpackedKey.get(1), "Index value should be ObjectId bytes");

            byte[] actualObjectIdBytes = (byte[]) unpackedKey.get(2);
            ObjectId actualObjectId = new ObjectId(actualObjectIdBytes);
            assertEquals(objectId, actualObjectId, "ObjectId should match");

            IndexEntry indexEntry = IndexEntry.decode(entry.getValue());
            assertEquals(SHARD_ID, indexEntry.shardId(), "Shard ID should match");
            assertArrayEquals(entryMetadata, indexEntry.entryMetadata(), "Entry metadata should match");
        }

        // Verify the back pointer was created with ObjectId bytes as the index value
        byte[] backPointerPrefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
        KeySelector backPointerBegin = KeySelector.firstGreaterOrEqual(backPointerPrefix);
        KeySelector backPointerEnd = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(backPointerPrefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> backPointers = tr.getRange(backPointerBegin, backPointerEnd).asList().join();
            assertEquals(1, backPointers.size(), "Should have exactly one back pointer");

            KeyValue backPointer = backPointers.get(0);
            Tuple unpackedBackPointer = indexSubspace.unpack(backPointer.getKey());

            assertEquals((long) IndexSubspaceMagic.BACK_POINTER.getValue(), unpackedBackPointer.get(0), "Back pointer magic value should match");

            byte[] backPointerObjectIdBytes = (byte[]) unpackedBackPointer.get(1);
            ObjectId backPointerObjectId = new ObjectId(backPointerObjectIdBytes);
            assertEquals(objectId, backPointerObjectId, "Back pointer ObjectId should match");
            assertArrayEquals(indexValue.toByteArray(), (byte[]) unpackedBackPointer.get(2), "Back pointer index value should be ObjectId bytes");

            assertArrayEquals(IndexMaintainer.NULL_VALUE, backPointer.getValue(), "Back pointer value should be NULL_VALUE");
        }

        // Verify the cardinality was updated
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(1, statistics.cardinality(), "Index cardinality should be 1 after adding one entry");
        }
    }

    @Test
    void shouldInsertSingleFieldIndexEntry() {
        // Behavior: Verifies that insertEntry creates an index entry, back pointer, and cardinality
        // update using direct tr.set() instead of atomic mutation.
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("test-index", "name", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);

        String indexValue = "insert-test";
        ObjectId objectId = new ObjectId();
        byte[] entryMetadata = getEncodedEntryMetadata();

        insertIndexEntryAndCommit(definition, metadata, indexValue, objectId, SHARD_ID, entryMetadata);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist");
        DirectorySubspace indexSubspace = index.subspace();

        // Verify the index entry
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

            byte[] actualObjectIdBytes = (byte[]) unpackedKey.get(2);
            ObjectId actualObjectId = new ObjectId(actualObjectIdBytes);
            assertEquals(objectId, actualObjectId, "ObjectId should match");

            IndexEntry indexEntry = IndexEntry.decode(entry.getValue());
            assertEquals(SHARD_ID, indexEntry.shardId(), "Shard ID should match");
            assertArrayEquals(entryMetadata, indexEntry.entryMetadata(), "Entry metadata should match");
        }

        // Verify the back pointer
        byte[] backPointerPrefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
        KeySelector backPointerBegin = KeySelector.firstGreaterOrEqual(backPointerPrefix);
        KeySelector backPointerEnd = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(backPointerPrefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> backPointers = tr.getRange(backPointerBegin, backPointerEnd).asList().join();
            assertEquals(1, backPointers.size(), "Should have exactly one back pointer");

            KeyValue backPointer = backPointers.get(0);
            Tuple unpackedBackPointer = indexSubspace.unpack(backPointer.getKey());

            assertEquals((long) IndexSubspaceMagic.BACK_POINTER.getValue(), unpackedBackPointer.get(0), "Back pointer magic value should match");

            byte[] backPointerObjectIdBytes = (byte[]) unpackedBackPointer.get(1);
            ObjectId backPointerObjectId = new ObjectId(backPointerObjectIdBytes);
            assertEquals(objectId, backPointerObjectId, "Back pointer ObjectId should match");
            assertEquals(indexValue, unpackedBackPointer.get(2), "Back pointer index value should match");

            assertArrayEquals(IndexMaintainer.NULL_VALUE, backPointer.getValue(), "Back pointer value should be NULL_VALUE");
        }

        // Verify cardinality
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(1, statistics.cardinality(), "Index cardinality should be 1 after inserting one entry");
        }
    }

    @ParameterizedTest
    @MethodSource("indexValueTestData")
    void shouldInsertSingleFieldIndexEntryForAllBsonTypes(String indexName, String fieldName, BsonType bsonType, Object inputValue, Object expectedStoredValue) {
        // Behavior: Verifies that insertEntry creates correct index entries for each supported BSON type.
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create(indexName, fieldName, bsonType, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, TEST_BUCKET);

        ObjectId objectId = new ObjectId();
        byte[] entryMetadata = getEncodedEntryMetadata();

        insertIndexEntryAndCommit(definition, metadata, inputValue, objectId, SHARD_ID, entryMetadata);

        Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.READ);
        assertNotNull(index, "Index should exist for " + bsonType);
        DirectorySubspace indexSubspace = index.subspace();

        verifyIndexEntry(indexSubspace, expectedStoredValue, objectId, entryMetadata);

        // Verify cardinality
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(1, statistics.cardinality(), "Index cardinality should be 1 after inserting one entry for " + bsonType);
        }

        // Verify the back pointer was created
        byte[] backPointerPrefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> backPointers = tr.getRange(
                    KeySelector.firstGreaterOrEqual(backPointerPrefix),
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(backPointerPrefix))
            ).asList().join();
            assertEquals(1, backPointers.size(), "Should have exactly one back pointer for " + bsonType);
        }
    }
}