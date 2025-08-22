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
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.volume.AppendedEntry;
import com.kronotop.volume.VolumeTestUtil;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class IndexBuilderTest extends BaseStandaloneInstanceTest {
    final int SHARD_ID = 1;
    final String testBucketName = "test-bucket";

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
                Arguments.of("decimal128-index", "price", BsonType.DECIMAL128, "99.99", "99.99"),
                Arguments.of("null-index", "nullable", BsonType.NULL, null, null)
        );
    }

    byte[] getEncodedEntryMetadata() {
        return VolumeTestUtil.generateEntryMetadata(1, 0, 1, "test").encode().array();
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
        BucketMetadata metadata = getBucketMetadata(bucket);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace indexSubspace = IndexUtil.create(tr, metadata.subspace(), definition);
            assertNotNull(indexSubspace);
            tr.commit().join();
        }
        // Refresh the index registry
        return getBucketMetadata(TEST_BUCKET_NAME);
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
        IndexDefinition definition = IndexDefinition.create(indexName, fieldName, bsonType, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexAndLoadBucketMetadata(definition, testBucketName);
        AppendedEntry[] entries = getAppendedEntries();

        setIndexEntryAndCommit(definition, metadata, inputValue, entries[0]);

        DirectorySubspace indexSubspace = metadata.indexes().getSubspace(definition.selector());
        assertNotNull(indexSubspace, "Index subspace should exist for " + bsonType);

        byte[] expectedEntryMetadata = getEncodedEntryMetadata();
        verifyIndexEntry(indexSubspace, expectedStoredValue, expectedEntryMetadata);
    }

    @Test
    void shouldSetDefaultIDIndex() {
        AppendedEntry[] entries = getAppendedEntries();
        assertDoesNotThrow(() -> {
            BucketMetadata metadata = getBucketMetadata(testBucketName);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexBuilder.setIDIndexEntry(tr, SHARD_ID, metadata, entries);
                tr.commit().join();
            }
        });
    }

    @Test
    void shouldReadEntriesFromIdIndex() {
        AppendedEntry[] entries = getAppendedEntries();
        BucketMetadata metadata = getBucketMetadata(testBucketName);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilder.setIDIndexEntry(tr, SHARD_ID, metadata, entries);
            tr.commit().join();
        }

        DirectorySubspace idIndexSubspace = metadata.indexes().getSubspace(DefaultIndexDefinition.ID.selector());
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
}