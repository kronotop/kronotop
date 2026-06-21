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
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.*;
import com.kronotop.volume.AppendedEntry;
import com.kronotop.volume.VolumeTestUtil;
import org.bson.*;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class CompoundIndexMaintainerTest extends BaseIndexMaintainerTest {

    private static final List<CompoundIndexField> NAME_AGE_FIELDS = List.of(
            new CompoundIndexField("name", BsonType.STRING, false),
            new CompoundIndexField("age", BsonType.INT32, false)
    );

    private static final List<CompoundIndexField> NAME_TAGS_FIELDS = List.of(
            new CompoundIndexField("name", BsonType.STRING, false),
            new CompoundIndexField("tags", BsonType.STRING, true)
    );

    static Stream<Arguments> compoundTypePairs() {
        ObjectId oid = new ObjectId();
        return Stream.of(
                Arguments.of("STRING+INT32", BsonType.STRING, BsonType.INT32, "John", 25, "John", 25L),
                Arguments.of("STRING+DOUBLE", BsonType.STRING, BsonType.DOUBLE, "John", 95.5, "John", 95.5),
                Arguments.of("STRING+BOOLEAN", BsonType.STRING, BsonType.BOOLEAN, "John", true, "John", true),
                Arguments.of("INT64+DATE_TIME", BsonType.INT64, BsonType.DATE_TIME, 100L, 1640995200000L, 100L, 1640995200000L),
                Arguments.of("STRING+OBJECT_ID", BsonType.STRING, BsonType.OBJECT_ID, "John", oid, "John", oid.toByteArray())
        );
    }

    BucketMetadata createCompoundIndexAndLoadBucketMetadata(CompoundIndexDefinition definition, String bucket) {
        createBucket(bucket);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            CompoundIndexUtil.create(tx, TEST_NAMESPACE, bucket, definition);
            tr.commit().join();
        }
        return refreshBucketMetadata(TEST_NAMESPACE, bucket);
    }

    void setCompoundIndexEntryAndCommit(CompoundIndex compoundIndex, BucketMetadata metadata,
                                        List<Object> fieldValues, ObjectId objectId, AppendedEntry entry) {
        byte[] objectIdBytes = objectId.toByteArray();
        byte[] encodedIndexEntry = new IndexEntry(SHARD_ID, entry.metadataBytes()).encode();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            CompoundIndexMaintainer.setEntry(tr, compoundIndex, metadata, fieldValues, objectIdBytes, encodedIndexEntry, new CollatorCache());
            tr.commit().join();
        }
    }

    List<KeyValue> getEntries(DirectorySubspace indexSubspace) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return tr.getRange(begin, end).asList().join();
        }
    }

    List<KeyValue> getBackPointers(DirectorySubspace indexSubspace) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return tr.getRange(begin, end).asList().join();
        }
    }

    // --- setEntry tests ---

    @Test
    void shouldSetCompoundIndexEntry() {
        // Behavior: setEntry creates an ENTRIES key with field values in order plus ObjectId,
        // a BACK_POINTER, and increments cardinality.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);
        assertNotNull(compoundIndex);

        AppendedEntry[] entries = getAppendedEntries();
        ObjectId objectId = new ObjectId();
        List<Object> fieldValues = List.of("John", 25);

        setCompoundIndexEntryAndCommit(compoundIndex, metadata, fieldValues, objectId, entries[0]);

        DirectorySubspace indexSubspace = compoundIndex.subspace();

        // Verify ENTRIES
        List<KeyValue> indexEntries = getEntries(indexSubspace);
        assertEquals(1, indexEntries.size());
        Tuple unpacked = indexSubspace.unpack(indexEntries.get(0).getKey());
        assertEquals((long) IndexSubspaceMagic.ENTRIES.getValue(), unpacked.get(0));
        assertEquals("John", unpacked.get(1));
        assertEquals(25L, unpacked.get(2));
        byte[] actualOid = (byte[]) unpacked.get(3);
        assertEquals(objectId, new ObjectId(actualOid));

        IndexEntry decoded = IndexEntry.decode(indexEntries.get(0).getValue());
        assertEquals(SHARD_ID, decoded.shardId());
        assertArrayEquals(getEncodedEntryMetadata(), decoded.entryMetadata());

        // Verify BACK_POINTER
        List<KeyValue> backPointers = getBackPointers(indexSubspace);
        assertEquals(1, backPointers.size());
        Tuple bpUnpacked = indexSubspace.unpack(backPointers.get(0).getKey());
        assertEquals((long) IndexSubspaceMagic.BACK_POINTER.getValue(), bpUnpacked.get(0));
        assertArrayEquals(objectId.toByteArray(), (byte[]) bpUnpacked.get(1));
        assertEquals("John", bpUnpacked.get(2));
        assertEquals(25L, bpUnpacked.get(3));
        assertArrayEquals(IndexMaintainer.NULL_VALUE, backPointers.get(0).getValue());

        // Verify cardinality
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(1, statistics.cardinality());
        }
    }

    @Test
    void shouldSetCompoundIndexEntryWithNullField() {
        // Behavior: When a field value is null (missing field), the entry stores null at that position.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        AppendedEntry[] entries = getAppendedEntries();
        ObjectId objectId = new ObjectId();
        List<Object> fieldValues = new java.util.ArrayList<>();
        fieldValues.add("John");
        fieldValues.add(null);

        setCompoundIndexEntryAndCommit(compoundIndex, metadata, fieldValues, objectId, entries[0]);

        List<KeyValue> indexEntries = getEntries(compoundIndex.subspace());
        assertEquals(1, indexEntries.size());
        Tuple unpacked = compoundIndex.subspace().unpack(indexEntries.get(0).getKey());
        assertEquals("John", unpacked.get(1));
        assertNull(unpacked.get(2));
    }

    // --- setEntry BSON type coverage ---

    @Test
    void shouldSetMultipleEntriesForSameIndex() {
        // Behavior: Multiple documents can be indexed, cardinality reflects the total count.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        AppendedEntry[] entries = getAppendedEntries();

        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("Alice", 20), new ObjectId(), entries[0]);
        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("Bob", 30), new ObjectId(), entries[1]);
        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("Charlie", 40), new ObjectId(), entries[2]);

        assertEquals(3, getEntries(compoundIndex.subspace()).size());
        assertEquals(3, getBackPointers(compoundIndex.subspace()).size());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(3, statistics.cardinality());
        }
    }

    @ParameterizedTest
    @MethodSource("compoundTypePairs")
    void shouldSetCompoundIndexEntryForVariousTypes(String label, BsonType type1, BsonType type2,
                                                    Object val1, Object val2,
                                                    Object expectedVal1, Object expectedVal2) {
        // Behavior: Compound index entries work with all supported BSON type combinations.
        List<CompoundIndexField> fields = List.of(
                new CompoundIndexField("field1", type1, false),
                new CompoundIndexField("field2", type2, false)
        );
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("type_pair_idx_" + label, fields, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        AppendedEntry[] entries = getAppendedEntries();
        ObjectId objectId = new ObjectId();
        List<Object> fieldValues = new java.util.ArrayList<>();
        fieldValues.add(val1);
        fieldValues.add(val2);

        setCompoundIndexEntryAndCommit(compoundIndex, metadata, fieldValues, objectId, entries[0]);

        List<KeyValue> indexEntries = getEntries(compoundIndex.subspace());
        assertEquals(1, indexEntries.size());
        Tuple unpacked = compoundIndex.subspace().unpack(indexEntries.get(0).getKey());

        if (expectedVal1 instanceof byte[]) {
            assertArrayEquals((byte[]) expectedVal1, (byte[]) unpacked.get(1));
        } else {
            assertEquals(expectedVal1, unpacked.get(1));
        }
        if (expectedVal2 instanceof byte[]) {
            assertArrayEquals((byte[]) expectedVal2, (byte[]) unpacked.get(2));
        } else {
            assertEquals(expectedVal2, unpacked.get(2));
        }
    }

    // --- insertEntry tests ---

    @Test
    void shouldInsertCompoundIndexEntry() {
        // Behavior: insertEntry creates the ENTRIES key, back pointer, and cardinality for a document.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        ObjectId objectId = new ObjectId();
        byte[] entryMetadata = getEncodedEntryMetadata();
        List<Object> fieldValues = List.of("John", 25);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            CompoundIndexMaintainer.insertEntry(tr, compoundIndex, metadata,
                    objectId.toByteArray(), fieldValues, SHARD_ID, entryMetadata, new CollatorCache());
            tr.commit().join();
        }

        DirectorySubspace indexSubspace = compoundIndex.subspace();

        // Verify ENTRIES key structure
        List<KeyValue> indexEntries = getEntries(indexSubspace);
        assertEquals(1, indexEntries.size(), "Should have exactly one index entry");

        Tuple unpacked = indexSubspace.unpack(indexEntries.get(0).getKey());
        assertEquals((long) IndexSubspaceMagic.ENTRIES.getValue(), unpacked.get(0), "Magic value should match");
        assertEquals("John", unpacked.get(1), "First field value should match");
        assertEquals(25L, unpacked.get(2), "Second field value should match");
        byte[] actualOid = (byte[]) unpacked.get(3);
        assertEquals(objectId, new ObjectId(actualOid), "ObjectId should match");

        // Verify entry value (IndexEntry)
        IndexEntry decoded = IndexEntry.decode(indexEntries.get(0).getValue());
        assertEquals(SHARD_ID, decoded.shardId(), "Shard ID should match");
        assertArrayEquals(entryMetadata, decoded.entryMetadata(), "Entry metadata should match");

        // Verify BACK_POINTER key structure
        List<KeyValue> backPointers = getBackPointers(indexSubspace);
        assertEquals(1, backPointers.size(), "Should have exactly one back pointer");

        Tuple bpUnpacked = indexSubspace.unpack(backPointers.get(0).getKey());
        assertEquals((long) IndexSubspaceMagic.BACK_POINTER.getValue(), bpUnpacked.get(0), "Back pointer magic value should match");
        assertArrayEquals(objectId.toByteArray(), (byte[]) bpUnpacked.get(1), "Back pointer ObjectId should match");
        assertEquals("John", bpUnpacked.get(2), "Back pointer first field value should match");
        assertEquals(25L, bpUnpacked.get(3), "Back pointer second field value should match");
        assertArrayEquals(IndexMaintainer.NULL_VALUE, backPointers.get(0).getValue(), "Back pointer value should be NULL_VALUE");

        // Verify cardinality
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(1, statistics.cardinality());
        }
    }

    // --- dropEntry tests ---

    @Test
    void shouldDropCompoundIndexEntry() {
        // Behavior: dropEntry removes all ENTRIES and BACK_POINTERs for a document,
        // and decrements cardinality.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        AppendedEntry[] entries = getAppendedEntries();
        ObjectId objectId = new ObjectId();
        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("John", 25), objectId, entries[0]);

        DirectorySubspace indexSubspace = compoundIndex.subspace();

        // Verify entry exists
        assertEquals(1, getEntries(indexSubspace).size());
        assertEquals(1, getBackPointers(indexSubspace).size());

        // Drop the entry
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            CompoundIndexMaintainer.dropEntry(tr, objectId.toByteArray(), definition, indexSubspace, metadata.subspace());
            tr.commit().join();
        }

        // Verify everything is removed
        assertEquals(0, getEntries(indexSubspace).size());
        assertEquals(0, getBackPointers(indexSubspace).size());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(0, statistics.cardinality());
        }
    }

    @Test
    void shouldDropCompoundIndexEntryWithMultipleEntries() {
        // Behavior: Dropping one document's entry leaves other documents' entries intact.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        AppendedEntry[] entries = getAppendedEntries();
        ObjectId objectId1 = new ObjectId();
        ObjectId objectId2 = new ObjectId();

        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("Alice", 20), objectId1, entries[0]);
        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("Bob", 30), objectId2, entries[1]);

        DirectorySubspace indexSubspace = compoundIndex.subspace();
        assertEquals(2, getEntries(indexSubspace).size());

        // Drop only objectId1
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            CompoundIndexMaintainer.dropEntry(tr, objectId1.toByteArray(), definition, indexSubspace, metadata.subspace());
            tr.commit().join();
        }

        // Verify the remaining entry belongs to objectId2 with correct field values
        List<KeyValue> remainingEntries = getEntries(indexSubspace);
        assertEquals(1, remainingEntries.size());
        Tuple unpacked = indexSubspace.unpack(remainingEntries.get(0).getKey());
        byte[] remainingOidBytes = (byte[]) unpacked.get(3);
        assertEquals(objectId2, new ObjectId(remainingOidBytes), "Remaining entry should belong to objectId2");
        assertEquals("Bob", unpacked.get(1), "Remaining entry should have Bob's name");
        assertEquals(30L, unpacked.get(2), "Remaining entry should have Bob's age");

        List<KeyValue> remainingBackPointers = getBackPointers(indexSubspace);
        assertEquals(1, remainingBackPointers.size());
        Tuple bpUnpacked = indexSubspace.unpack(remainingBackPointers.get(0).getKey());
        assertArrayEquals(objectId2.toByteArray(), (byte[]) bpUnpacked.get(1), "Remaining back pointer should belong to objectId2");
        assertEquals("Bob", bpUnpacked.get(2));
        assertEquals(30L, bpUnpacked.get(3));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(1, statistics.cardinality());
        }
    }

    @Test
    void shouldDropNonExistentObjectIdWithoutError() {
        // Behavior: Dropping a non-existent ObjectId is a no-op and doesn't affect cardinality.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        DirectorySubspace indexSubspace = compoundIndex.subspace();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ObjectId nonExistent = new ObjectId();
            assertDoesNotThrow(() -> {
                CompoundIndexMaintainer.dropEntry(tr, nonExistent.toByteArray(), definition, indexSubspace, metadata.subspace());
                tr.commit().join();
            });
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(0, statistics.cardinality());
        }
    }

    @Test
    void shouldDropCompoundIndexEntryWithMultiKeyField() {
        // Behavior: dropEntry removes all ENTRIES and BACK_POINTERs generated from a multi-key array field,
        // and decrements cardinality by the total count of entries removed.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_tags_idx", NAME_TAGS_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        AppendedEntry[] entries = getAppendedEntries();
        ObjectId objectId = new ObjectId();

        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("John", "a"), objectId, entries[0]);
        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("John", "b"), objectId, entries[0]);
        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("John", "c"), objectId, entries[0]);

        DirectorySubspace indexSubspace = compoundIndex.subspace();

        assertEquals(3, getEntries(indexSubspace).size());
        assertEquals(3, getBackPointers(indexSubspace).size());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(3, statistics.cardinality());
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            CompoundIndexMaintainer.dropEntry(tr, objectId.toByteArray(), definition, indexSubspace, metadata.subspace());
            tr.commit().join();
        }

        assertEquals(0, getEntries(indexSubspace).size());
        assertEquals(0, getBackPointers(indexSubspace).size());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(0, statistics.cardinality());
        }
    }

    // --- updateIndexEntry tests ---

    @Test
    void shouldUpdateCompoundIndexEntryMetadata() {
        // Behavior: updateIndexEntry replaces the IndexEntry value for all entries of a document
        // without changing keys.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        AppendedEntry[] entries = getAppendedEntries();
        ObjectId objectId = new ObjectId();
        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("John", 25), objectId, entries[0]);

        DirectorySubspace indexSubspace = compoundIndex.subspace();

        // Get the original key for later comparison
        List<KeyValue> before = getEntries(indexSubspace);
        assertEquals(1, before.size());
        byte[] originalKey = before.get(0).getKey();

        // Create new metadata
        byte[] newEntryMetadata = VolumeTestUtil.generateEntryMetadata(1, 2, 1, 2, "updated").encode();
        byte[] newMetadata = new IndexEntry(SHARD_ID + 1, newEntryMetadata).encode();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            CompoundIndexMaintainer.updateIndexEntry(tr, objectId.toByteArray(), newMetadata, indexSubspace, definition.fields().size());
            tr.commit().join();
        }

        List<KeyValue> after = getEntries(indexSubspace);
        assertEquals(1, after.size());
        assertArrayEquals(originalKey, after.get(0).getKey());
        IndexEntry updatedIndexEntry = IndexEntry.decode(after.get(0).getValue());
        assertEquals(SHARD_ID + 1, updatedIndexEntry.shardId(), "Shard ID should match");
        assertArrayEquals(newEntryMetadata, updatedIndexEntry.entryMetadata(), "Entry metadata should be updated");
    }

    @Test
    void shouldUpdateMetadataForNonExistentObjectIdWithoutError() {
        // Behavior: Updating metadata for a non-existent ObjectId is a no-op.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        DirectorySubspace indexSubspace = compoundIndex.subspace();
        byte[] newMetadata = new IndexEntry(SHARD_ID, getEncodedEntryMetadata()).encode();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> {
                CompoundIndexMaintainer.updateIndexEntry(tr, new ObjectId().toByteArray(), newMetadata, indexSubspace, definition.fields().size());
                tr.commit().join();
            });
        }

        assertEquals(0, getEntries(indexSubspace).size());
    }

    @Test
    void shouldUpdateCompoundIndexEntryMetadataWithMultiKeyField() {
        // Behavior: updateIndexEntry updates all entries generated from a multi-key array field
        // for the same ObjectId, replacing metadata while preserving keys.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_tags_idx", NAME_TAGS_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        AppendedEntry[] entries = getAppendedEntries();
        ObjectId objectId = new ObjectId();

        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("John", "a"), objectId, entries[0]);
        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("John", "b"), objectId, entries[0]);
        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("John", "c"), objectId, entries[0]);

        DirectorySubspace indexSubspace = compoundIndex.subspace();

        List<KeyValue> before = getEntries(indexSubspace);
        assertEquals(3, before.size());
        byte[][] originalKeys = new byte[3][];
        for (int i = 0; i < 3; i++) {
            originalKeys[i] = before.get(i).getKey();
        }

        byte[] newEntryMetadata = VolumeTestUtil.generateEntryMetadata(1, 2, 1, 2, "updated").encode();
        byte[] newMetadata = new IndexEntry(SHARD_ID + 1, newEntryMetadata).encode();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            CompoundIndexMaintainer.updateIndexEntry(tr, objectId.toByteArray(), newMetadata, indexSubspace, definition.fields().size());
            tr.commit().join();
        }

        List<KeyValue> after = getEntries(indexSubspace);
        assertEquals(3, after.size());

        for (int i = 0; i < 3; i++) {
            assertArrayEquals(originalKeys[i], after.get(i).getKey(), "Key should be unchanged for entry " + i);
            IndexEntry updatedIndexEntry = IndexEntry.decode(after.get(i).getValue());
            assertEquals(SHARD_ID + 1, updatedIndexEntry.shardId(), "Shard ID should match for entry " + i);
            assertArrayEquals(newEntryMetadata, updatedIndexEntry.entryMetadata(), "Entry metadata should be updated for entry " + i);
        }
    }

    // --- extractFieldValues (BsonDocument) tests ---

    @Test
    void shouldExtractFieldValuesFromDocument() {
        // Behavior: extractFieldValues returns a single combination list for non-multikey compound indexes.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("John"))
                .append("age", new BsonInt32(25));

        List<List<Object>> result = CompoundIndexMaintainer.extractFieldValues(compoundIndex, document, false);
        assertEquals(1, result.size());
        assertEquals("John", result.get(0).get(0));
        assertEquals(25, result.get(0).get(1));
    }

    @Test
    void shouldExtractFieldValuesRegardlessOfDocumentFieldOrder() {
        // Behavior: Field values are extracted in the index definition order, not document field order.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        // Document has fields in reverse order compared to index definition
        BsonDocument document = new BsonDocument()
                .append("age", new BsonInt32(25))
                .append("name", new BsonString("John"));

        List<List<Object>> result = CompoundIndexMaintainer.extractFieldValues(compoundIndex, document, false);
        assertEquals(1, result.size());
        // Index definition order is (name, age), so name must be first
        assertEquals("John", result.get(0).get(0));
        assertEquals(25, result.get(0).get(1));
    }

    @Test
    void shouldExtractFieldValuesWithMissingField() {
        // Behavior: Missing fields produce null values in the combination.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("John"));

        List<List<Object>> result = CompoundIndexMaintainer.extractFieldValues(compoundIndex, document, false);
        assertEquals(1, result.size());
        assertEquals("John", result.get(0).get(0));
        assertNull(result.get(0).get(1));
    }

    @Test
    void shouldExtractFieldValuesWithNullField() {
        // Behavior: Explicit BsonNull produces null in the combination.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("John"))
                .append("age", BsonNull.VALUE);

        List<List<Object>> result = CompoundIndexMaintainer.extractFieldValues(compoundIndex, document, false);
        assertEquals(1, result.size());
        assertEquals("John", result.get(0).get(0));
        assertNull(result.get(0).get(1));
    }

    @Test
    void shouldExtractFieldValuesWithMultiKeyArray() {
        // Behavior: A multikey field containing an array expands into multiple combinations.
        List<CompoundIndexField> fields = List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("tags", BsonType.STRING, true)
        );
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_tags_idx", fields, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("John"))
                .append("tags", new BsonArray(List.of(
                        new BsonString("a"), new BsonString("b"), new BsonString("c")
                )));

        List<List<Object>> result = CompoundIndexMaintainer.extractFieldValues(compoundIndex, document, false);
        assertEquals(3, result.size());

        // All combinations should have "John" as the first field
        for (List<Object> combo : result) {
            assertEquals("John", combo.get(0));
        }
        // Verify all tag values present
        List<Object> tagValues = result.stream().map(combo -> combo.get(1)).toList();
        assertTrue(tagValues.contains("a"));
        assertTrue(tagValues.contains("b"));
        assertTrue(tagValues.contains("c"));
    }

    @Test
    void shouldExtractFieldValuesWithMultiKeyArrayContainingNulls() {
        // Behavior: Null values in multikey arrays are deduplicated into a single null entry.
        List<CompoundIndexField> fields = List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("tags", BsonType.STRING, true)
        );
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_tags_idx", fields, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("John"))
                .append("tags", new BsonArray(List.of(
                        new BsonString("a"), BsonNull.VALUE, BsonNull.VALUE, new BsonString("b")
                )));

        List<List<Object>> result = CompoundIndexMaintainer.extractFieldValues(compoundIndex, document, false);
        assertEquals(3, result.size()); // a, b, null (deduplicated)
    }

    @Test
    void shouldExtractFieldValuesWithEmptyMultiKeyArray() {
        // Behavior: An empty multikey array returns an empty list (index skipped).
        List<CompoundIndexField> fields = List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("tags", BsonType.STRING, true)
        );
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_tags_idx", fields, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("John"))
                .append("tags", new BsonArray());

        List<List<Object>> result = CompoundIndexMaintainer.extractFieldValues(compoundIndex, document, false);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldDeduplicateMultiKeyArrayValues() {
        // Behavior: Duplicate values in a multikey array are deduplicated via LinkedHashSet,
        // producing only unique combinations.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_tags_idx", NAME_TAGS_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("John"))
                .append("tags", new BsonArray(List.of(
                        new BsonString("a"), new BsonString("b"), new BsonString("a")
                )));

        List<List<Object>> result = CompoundIndexMaintainer.extractFieldValues(compoundIndex, document, false);
        assertEquals(2, result.size());

        List<Object> tagValues = result.stream().map(combo -> combo.get(1)).toList();
        assertTrue(tagValues.contains("a"));
        assertTrue(tagValues.contains("b"));
    }

    @Test
    void shouldReturnEmptyOnTypeMismatchNonStrict() {
        // Behavior: When strict types are off and a field value doesn't match the expected type,
        // the index is skipped (an empty list returned).
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        BsonDocument document = new BsonDocument()
                .append("name", new BsonInt32(123)) // INT32 instead of STRING
                .append("age", new BsonInt32(25));

        List<List<Object>> result = CompoundIndexMaintainer.extractFieldValues(compoundIndex, document, false);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldThrowOnTypeMismatchStrict() {
        // Behavior: When strict types are on and a field value doesn't match, IndexTypeMismatchException is thrown.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        BsonDocument document = new BsonDocument()
                .append("name", new BsonInt32(123)) // INT32 instead of STRING
                .append("age", new BsonInt32(25));

        assertThrows(IndexTypeMismatchException.class, () ->
                CompoundIndexMaintainer.extractFieldValues(compoundIndex, document, true));
    }

    @Test
    void shouldThrowOnTypeMismatchInMultiKeyArrayStrict() {
        // Behavior: When strict types are on and a multi-key array element doesn't match the expected type,
        // IndexTypeMismatchException is thrown.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_tags_idx", NAME_TAGS_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("John"))
                .append("tags", new BsonArray(List.of(
                        new BsonString("a"), new BsonInt32(123), new BsonString("b")
                )));

        assertThrows(IndexTypeMismatchException.class, () ->
                CompoundIndexMaintainer.extractFieldValues(compoundIndex, document, true));
    }

    @Test
    void shouldExtractFieldValuesWithAllFieldsMissing() {
        // Behavior: When all fields are missing from the document, a single combination with all null values is returned.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        BsonDocument document = new BsonDocument();

        List<List<Object>> result = CompoundIndexMaintainer.extractFieldValues(compoundIndex, document, false);
        assertEquals(1, result.size());
        assertNull(result.get(0).get(0));
        assertNull(result.get(0).get(1));
    }

    // --- extractFieldValues (ByteBuffer) tests ---

    @Test
    void shouldExtractFieldValuesFromByteBuffer() {
        // Behavior: ByteBuffer variant produces the same results as BsonDocument variant
        // and rewinds the buffer after each field.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("John"))
                .append("age", new BsonInt32(25));

        List<List<Object>> fromDoc = CompoundIndexMaintainer.extractFieldValues(compoundIndex, document, false);

        ByteBuffer buffer = BSONUtil.toByteBuffer(document);
        List<List<Object>> fromBuf = CompoundIndexMaintainer.extractFieldValues(compoundIndex, buffer, false);

        assertEquals(fromDoc.size(), fromBuf.size());
        for (int i = 0; i < fromDoc.size(); i++) {
            assertEquals(fromDoc.get(i), fromBuf.get(i));
        }
    }

    @Test
    void shouldExtractFieldValuesFromByteBufferWithMultiKeyArray() {
        // Behavior: ByteBuffer variant with a multi-key array produces the same combinations as BsonDocument variant.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_tags_idx", NAME_TAGS_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("John"))
                .append("tags", new BsonArray(List.of(
                        new BsonString("a"), new BsonString("b"), new BsonString("c")
                )));

        List<List<Object>> fromDoc = CompoundIndexMaintainer.extractFieldValues(compoundIndex, document, false);

        ByteBuffer buffer = BSONUtil.toByteBuffer(document);
        List<List<Object>> fromBuf = CompoundIndexMaintainer.extractFieldValues(compoundIndex, buffer, false);

        assertEquals(fromDoc.size(), fromBuf.size());
        for (int i = 0; i < fromDoc.size(); i++) {
            assertEquals(fromDoc.get(i), fromBuf.get(i));
        }
    }

    @Test
    void shouldExtractFieldValuesFromByteBufferWithMissingField() {
        // Behavior: ByteBuffer variant with a missing field produces the same null-containing combination as BsonDocument variant.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("John"));

        List<List<Object>> fromDoc = CompoundIndexMaintainer.extractFieldValues(compoundIndex, document, false);

        ByteBuffer buffer = BSONUtil.toByteBuffer(document);
        List<List<Object>> fromBuf = CompoundIndexMaintainer.extractFieldValues(compoundIndex, buffer, false);

        assertEquals(fromDoc.size(), fromBuf.size());
        for (int i = 0; i < fromDoc.size(); i++) {
            assertEquals(fromDoc.get(i), fromBuf.get(i));
        }
    }

    @Test
    void shouldReturnEmptyOnTypeMismatchFromByteBuffer() {
        // Behavior: ByteBuffer variant returns an empty list on type mismatch with strictTypes=false.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        BsonDocument document = new BsonDocument()
                .append("name", new BsonInt32(123))
                .append("age", new BsonInt32(25));

        ByteBuffer buffer = BSONUtil.toByteBuffer(document);
        List<List<Object>> result = CompoundIndexMaintainer.extractFieldValues(compoundIndex, buffer, false);
        assertTrue(result.isEmpty());
    }

    // --- Three-field compound index test ---

    @Test
    void shouldHandleThreeFieldCompoundIndex() {
        // Behavior: Compound index with 3 fields creates correct ENTRIES and BACK_POINTER key structures
        // with all field values in the proper positions.
        List<CompoundIndexField> threeFields = List.of(
                new CompoundIndexField("first", BsonType.STRING, false),
                new CompoundIndexField("second", BsonType.INT32, false),
                new CompoundIndexField("third", BsonType.BOOLEAN, false)
        );
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("three_field_idx", threeFields, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        AppendedEntry[] entries = getAppendedEntries();
        ObjectId objectId = new ObjectId();
        List<Object> fieldValues = List.of("Alice", 42, true);

        setCompoundIndexEntryAndCommit(compoundIndex, metadata, fieldValues, objectId, entries[0]);

        DirectorySubspace indexSubspace = compoundIndex.subspace();

        // Verify ENTRIES key structure: (ENTRIES, "Alice", 42L, true, objectIdBytes)
        List<KeyValue> indexEntries = getEntries(indexSubspace);
        assertEquals(1, indexEntries.size());
        Tuple unpacked = indexSubspace.unpack(indexEntries.get(0).getKey());
        assertEquals((long) IndexSubspaceMagic.ENTRIES.getValue(), unpacked.get(0));
        assertEquals("Alice", unpacked.get(1));
        assertEquals(42L, unpacked.get(2));
        assertEquals(true, unpacked.get(3));
        assertEquals(objectId, new ObjectId((byte[]) unpacked.get(4)));

        // Verify entry value
        IndexEntry decoded = IndexEntry.decode(indexEntries.get(0).getValue());
        assertEquals(SHARD_ID, decoded.shardId());
        assertArrayEquals(getEncodedEntryMetadata(), decoded.entryMetadata());

        // Verify BACK_POINTER key structure: (BACK_POINTER, objectIdBytes, "Alice", 42L, true)
        List<KeyValue> backPointers = getBackPointers(indexSubspace);
        assertEquals(1, backPointers.size());
        Tuple bpUnpacked = indexSubspace.unpack(backPointers.get(0).getKey());
        assertEquals((long) IndexSubspaceMagic.BACK_POINTER.getValue(), bpUnpacked.get(0));
        assertArrayEquals(objectId.toByteArray(), (byte[]) bpUnpacked.get(1));
        assertEquals("Alice", bpUnpacked.get(2));
        assertEquals(42L, bpUnpacked.get(3));
        assertEquals(true, bpUnpacked.get(4));

        // Verify cardinality
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
            assertEquals(1, statistics.cardinality());
        }
    }

    // --- findIndexedObjectIds tests ---

    private Set<ObjectId> findIndexedObjectIds(DirectorySubspace indexSubspace, ObjectId... objectIds) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<byte[]> ids = Arrays.stream(objectIds).map(ObjectId::toByteArray).toList();
            return IndexMaintainer.findIndexedObjectIds(tr, indexSubspace, ids);
        }
    }

    @Test
    void shouldFindIndexedObjectIdsForListedIds() {
        // Behavior: Verifies that findIndexedObjectIds returns every listed ObjectId that has a back
        // pointer in a compound index.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        AppendedEntry[] entries = getAppendedEntries();
        ObjectId id0 = new ObjectId();
        ObjectId id1 = new ObjectId();
        ObjectId id2 = new ObjectId();
        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("Alice", 20), id0, entries[0]);
        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("Bob", 30), id1, entries[1]);
        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("Carol", 40), id2, entries[2]);

        Set<ObjectId> found = findIndexedObjectIds(compoundIndex.subspace(), id0, id1, id2);

        assertEquals(Set.of(id0, id1, id2), found);
    }

    @Test
    void shouldReturnOnlyIndexedIdsFromList() {
        // Behavior: Verifies that findIndexedObjectIds returns only the listed ObjectIds that are
        // actually indexed, excluding ones with no back pointer.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        AppendedEntry[] entries = getAppendedEntries();
        ObjectId indexed0 = new ObjectId();
        ObjectId notIndexed = new ObjectId();
        ObjectId indexed1 = new ObjectId();
        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("Alice", 20), indexed0, entries[0]);
        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("Bob", 30), indexed1, entries[1]);

        Set<ObjectId> found = findIndexedObjectIds(compoundIndex.subspace(), indexed0, notIndexed, indexed1);

        assertEquals(Set.of(indexed0, indexed1), found);
    }

    @Test
    void shouldDeduplicateObjectIdWithMultipleBackPointers() {
        // Behavior: Verifies that findIndexedObjectIds returns a single ObjectId even when a multi-key
        // field produced multiple back pointers for that document.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_tags_idx", NAME_TAGS_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        AppendedEntry[] entries = getAppendedEntries();
        ObjectId objectId = new ObjectId();
        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("John", "a"), objectId, entries[0]);
        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("John", "b"), objectId, entries[0]);
        setCompoundIndexEntryAndCommit(compoundIndex, metadata, List.of("John", "c"), objectId, entries[0]);

        assertEquals(3, getBackPointers(compoundIndex.subspace()).size());

        Set<ObjectId> found = findIndexedObjectIds(compoundIndex.subspace(), objectId);

        assertEquals(Set.of(objectId), found);
    }

    @Test
    void shouldReturnEmptyWhenNoObjectIdsIndexed() {
        // Behavior: Verifies that findIndexedObjectIds returns an empty set when none of the listed
        // ObjectIds are indexed.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("name_age_idx", NAME_AGE_FIELDS, IndexStatus.WAITING);
        BucketMetadata metadata = createCompoundIndexAndLoadBucketMetadata(definition, TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(definition.name(), IndexSelectionPolicy.ALL);

        Set<ObjectId> found = findIndexedObjectIds(compoundIndex.subspace(), new ObjectId(), new ObjectId());

        assertTrue(found.isEmpty());
    }
}
