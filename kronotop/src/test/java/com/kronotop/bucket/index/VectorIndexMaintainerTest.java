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
import com.kronotop.KronotopException;
import com.kronotop.TestUtil;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.volume.AppendedEntry;
import com.kronotop.volume.VolumeTestUtil;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonNull;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VectorIndexMaintainerTest extends BaseIndexMaintainerTest {

    private static final String SELECTOR = "embedding";
    private static final int DIMENSIONS = 3;
    private static final float[] TEST_VECTOR = {0.1f, 0.2f, 0.3f};

    private BucketMetadata createVectorIndexAndLoadBucketMetadata() {
        createBucket(TEST_BUCKET);
        String name = VectorIndexNameGenerator.generate(SELECTOR, DIMENSIONS, DistanceFunction.COSINE);
        VectorIndexDefinition definition = VectorIndexDefinition.create(name, SELECTOR, DIMENSIONS, DistanceFunction.COSINE, IndexStatus.WAITING);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            VectorIndexUtil.create(tx, getBucketMetadata(TEST_BUCKET), definition);
            tr.commit().join();
        }
        return refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
    }

    private List<KeyValue> getEntries(DirectorySubspace indexSubspace) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return tr.getRange(begin, end).asList().join();
        }
    }

    @Test
    void shouldSetEntry() {
        // Behavior: setEntry creates an ENTRIES key with ObjectId, stores VectorIndexValue
        // with encoded index entry and vector, and increments cardinality.
        BucketMetadata metadata = createVectorIndexAndLoadBucketMetadata();
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector(SELECTOR, IndexSelectionPolicy.ALL);
        assertNotNull(vectorIndex);

        AppendedEntry[] entries = getAppendedEntries();
        AppendedEntry entry = entries[0];
        ObjectId objectId = new ObjectId();
        byte[] objectIdBytes = objectId.toByteArray();
        byte[] encodedIndexEntry = new IndexEntry(SHARD_ID, entry.metadataBytes()).encode();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setEntry(tr, vectorIndex, metadata, objectIdBytes, encodedIndexEntry, TEST_VECTOR);
            tr.commit().join();
        }

        // Verify entry exists
        List<KeyValue> indexEntries = getEntries(vectorIndex.subspace());
        assertEquals(1, indexEntries.size());

        // Verify entry value can be decoded
        VectorIndexValue value = VectorIndexValue.decode(indexEntries.get(0).getValue());
        assertEquals(SHARD_ID, value.indexEntry().shardId());
        assertArrayEquals(entry.metadataBytes(), value.indexEntry().entryMetadata());
        assertArrayEquals(TEST_VECTOR, value.vector());

        // Verify cardinality
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), vectorIndex.definition().id());
            assertEquals(1, statistics.cardinality());
        }
    }

    @Test
    void shouldInsertEntry() {
        // Behavior: insertEntry creates an ENTRIES key with ObjectId, stores VectorIndexValue
        // with encoded index entry and vector, and increments cardinality.
        BucketMetadata metadata = createVectorIndexAndLoadBucketMetadata();
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector(SELECTOR, IndexSelectionPolicy.ALL);
        assertNotNull(vectorIndex);

        ObjectId objectId = new ObjectId();
        byte[] objectIdBytes = objectId.toByteArray();
        byte[] entryMetadata = getEncodedEntryMetadata();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.insertEntry(tr, vectorIndex, metadata,
                    objectIdBytes, SHARD_ID, entryMetadata, TEST_VECTOR);
            tr.commit().join();
        }

        List<KeyValue> indexEntries = getEntries(vectorIndex.subspace());
        assertEquals(1, indexEntries.size(), "Should have exactly one index entry");

        Tuple unpacked = vectorIndex.subspace().unpack(indexEntries.get(0).getKey());
        assertEquals((long) IndexSubspaceMagic.ENTRIES.getValue(), unpacked.get(0), "Magic value should match");
        byte[] actualObjectIdBytes = (byte[]) unpacked.get(1);
        assertEquals(objectId, new ObjectId(actualObjectIdBytes), "ObjectId should match");

        VectorIndexValue value = VectorIndexValue.decode(indexEntries.get(0).getValue());
        assertEquals(SHARD_ID, value.indexEntry().shardId(), "Shard ID should match");
        assertArrayEquals(entryMetadata, value.indexEntry().entryMetadata(), "Entry metadata should match");
        assertArrayEquals(TEST_VECTOR, value.vector(), "Vector should match");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), vectorIndex.definition().id());
            assertEquals(1, statistics.cardinality(), "Index cardinality should be 1 after inserting one entry");
        }
    }

    @Test
    void shouldReadEntry() {
        // Behavior: After setting a vector index entry, the stored ObjectId key and VectorIndexValue
        // can be read back correctly.
        BucketMetadata metadata = createVectorIndexAndLoadBucketMetadata();
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector(SELECTOR, IndexSelectionPolicy.ALL);

        AppendedEntry[] entries = getAppendedEntries();
        ObjectId[] objectIds = new ObjectId[entries.length];

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < entries.length; i++) {
                objectIds[i] = new ObjectId();
                byte[] encodedIndexEntry = new IndexEntry(SHARD_ID, entries[i].metadataBytes()).encode();
                float[] vector = {i * 0.1f, i * 0.2f, i * 0.3f};
                VectorIndexMaintainer.setEntry(tr, vectorIndex, metadata, objectIds[i].toByteArray(), encodedIndexEntry, vector);
            }
            tr.commit().join();
        }

        List<KeyValue> indexEntries = getEntries(vectorIndex.subspace());
        assertEquals(entries.length, indexEntries.size());

        for (KeyValue kv : indexEntries) {
            Tuple unpacked = vectorIndex.subspace().unpack(kv.getKey());
            byte[] objectIdBytes = (byte[]) unpacked.get(1);
            ObjectId objectId = new ObjectId(objectIdBytes);

            boolean found = false;
            for (ObjectId expected : objectIds) {
                if (expected.equals(objectId)) {
                    found = true;
                    break;
                }
            }
            assertTrue(found, "ObjectId should be one of the inserted ones");

            VectorIndexValue value = VectorIndexValue.decode(kv.getValue());
            assertEquals(SHARD_ID, value.indexEntry().shardId());
        }
    }

    @Test
    void shouldUpdateIndexEntry() {
        // Behavior: updateIndexEntry replaces the shardId and entryMetadata
        // of an existing vector index entry while preserving the original vector data.
        BucketMetadata metadata = createVectorIndexAndLoadBucketMetadata();
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector(SELECTOR, IndexSelectionPolicy.ALL);
        assertNotNull(vectorIndex);

        AppendedEntry[] entries = getAppendedEntries();
        AppendedEntry entry = entries[0];
        ObjectId objectId = new ObjectId();
        byte[] objectIdBytes = objectId.toByteArray();
        byte[] encodedIndexEntry = new IndexEntry(SHARD_ID, entry.metadataBytes()).encode();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setEntry(tr, vectorIndex, metadata, objectIdBytes, encodedIndexEntry, TEST_VECTOR);
            tr.commit().join();
        }

        List<KeyValue> beforeEntries = getEntries(vectorIndex.subspace());
        assertEquals(1, beforeEntries.size());
        byte[] originalKey = beforeEntries.get(0).getKey();

        int newShardId = SHARD_ID + 1;
        byte[] newEntryMetadata = VolumeTestUtil.generateEntryMetadata(1, 2, 1, 2, "updated").encode();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.updateIndexEntry(tr, objectIdBytes, vectorIndex, newShardId, newEntryMetadata);
            tr.commit().join();
        }

        List<KeyValue> afterEntries = getEntries(vectorIndex.subspace());
        assertEquals(1, afterEntries.size(), "Should still have exactly one index entry");
        assertArrayEquals(originalKey, afterEntries.get(0).getKey(), "Key should be unchanged");

        VectorIndexValue updatedValue = VectorIndexValue.decode(afterEntries.get(0).getValue());
        assertEquals(newShardId, updatedValue.indexEntry().shardId(), "Shard ID should be updated");
        assertArrayEquals(newEntryMetadata, updatedValue.indexEntry().entryMetadata(), "Entry metadata should be updated");
        assertArrayEquals(TEST_VECTOR, updatedValue.vector(), "Vector should be preserved after update");
    }

    @Test
    void shouldUpdateNonExistentObjectIdWithoutError() {
        // Behavior: updateIndexEntry does not throw or create entries when called with a non-existent ObjectId.
        BucketMetadata metadata = createVectorIndexAndLoadBucketMetadata();
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector(SELECTOR, IndexSelectionPolicy.ALL);
        assertNotNull(vectorIndex);

        ObjectId nonExistentObjectId = new ObjectId();
        byte[] newEntryMetadata = VolumeTestUtil.generateEntryMetadata(1, 1, 0, 1, "test").encode();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> {
                VectorIndexMaintainer.updateIndexEntry(tr, nonExistentObjectId.toByteArray(),
                        vectorIndex, SHARD_ID, newEntryMetadata);
                tr.commit().join();
            }, "Should not throw exception for non-existent ObjectId");
        }

        List<KeyValue> indexEntries = getEntries(vectorIndex.subspace());
        assertEquals(0, indexEntries.size(), "Index should remain empty");
    }

    @Test
    void shouldDropEntry() {
        // Behavior: dropEntry removes the ENTRIES key and decrements cardinality.
        BucketMetadata metadata = createVectorIndexAndLoadBucketMetadata();
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector(SELECTOR, IndexSelectionPolicy.ALL);

        AppendedEntry[] entries = getAppendedEntries();
        ObjectId[] objectIds = new ObjectId[entries.length];

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < entries.length; i++) {
                objectIds[i] = new ObjectId();
                byte[] encodedIndexEntry = new IndexEntry(SHARD_ID, entries[i].metadataBytes()).encode();
                VectorIndexMaintainer.setEntry(tr, vectorIndex, metadata, objectIds[i].toByteArray(), encodedIndexEntry, TEST_VECTOR);
            }
            tr.commit().join();
        }

        // Drop the first entry
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.dropEntry(tr, objectIds[0].toByteArray(), vectorIndex, metadata);
            tr.commit().join();
        }

        // Verify one fewer entry
        List<KeyValue> indexEntries = getEntries(vectorIndex.subspace());
        assertEquals(entries.length - 1, indexEntries.size());

        // Verify the dropped ObjectId is gone
        for (KeyValue kv : indexEntries) {
            Tuple unpacked = vectorIndex.subspace().unpack(kv.getKey());
            byte[] objectIdBytes = (byte[]) unpacked.get(1);
            ObjectId objectId = new ObjectId(objectIdBytes);
            assertNotEquals(objectIds[0], objectId);
        }

        // Verify cardinality
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), vectorIndex.definition().id());
            assertEquals(entries.length - 1, statistics.cardinality());
        }
    }

    @Test
    void shouldDropEntryForNonExistentObjectId() {
        // Behavior: dropEntry does not throw for a non-existent ObjectId.
        BucketMetadata metadata = createVectorIndexAndLoadBucketMetadata();
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector(SELECTOR, IndexSelectionPolicy.ALL);
        assertNotNull(vectorIndex);

        ObjectId nonExistentObjectId = new ObjectId();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> {
                VectorIndexMaintainer.dropEntry(tr, nonExistentObjectId.toByteArray(), vectorIndex, metadata);
                tr.commit().join();
            }, "Should not throw exception for non-existent ObjectId");
        }

        List<KeyValue> indexEntries = getEntries(vectorIndex.subspace());
        assertEquals(0, indexEntries.size(), "Index should remain empty");
    }

    @Test
    void shouldEncodeDecodeRoundTrip() {
        // Behavior: VectorIndexValue encode/decode preserves IndexEntry fields and vector data exactly.
        byte[] entryMetadata = VolumeTestUtil.generateEntryMetadata(1, 1, 0, 1, "test").encode();
        byte[] encodedIndexEntry = new IndexEntry(SHARD_ID, entryMetadata).encode();
        float[] vector = {1.0f, -2.5f, 3.14159f, 0.0f};

        byte[] encoded = VectorIndexValue.encode(encodedIndexEntry, vector);
        VectorIndexValue decoded = VectorIndexValue.decode(encoded);

        assertEquals(SHARD_ID, decoded.indexEntry().shardId());
        assertArrayEquals(entryMetadata, decoded.indexEntry().entryMetadata());
        assertArrayEquals(vector, decoded.vector());
    }

    @Test
    void shouldRejectVectorWithOverflowingComponent() {
        // Behavior: extractVector rejects a document whose vector component overflows float range.
        String name = VectorIndexNameGenerator.generate(SELECTOR, DIMENSIONS, DistanceFunction.COSINE);
        VectorIndexDefinition definition = VectorIndexDefinition.create(name, SELECTOR, DIMENSIONS, DistanceFunction.COSINE, IndexStatus.WAITING);
        BsonDocument document = new BsonDocument();
        BsonArray array = new BsonArray();
        array.add(new BsonDouble(0.1));
        array.add(new BsonDouble(Double.MAX_VALUE));
        array.add(new BsonDouble(0.3));
        document.put(SELECTOR, array);

        KronotopException ex = assertThrows(KronotopException.class, () ->
                VectorIndexMaintainer.extractVector(definition, document));
        assertTrue(ex.getMessage().contains("overflows float range"));
    }

    private List<KeyValue> getMutationLogEntries(DirectorySubspace indexSubspace) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.MUTATION_LOG.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return tr.getRange(begin, end).asList().join();
        }
    }

    @Test
    void shouldSetMutationLog() {
        // Behavior: setMutationLog records an INSERT mutation log entry with the correct marker,
        // objectIdBytes, and vector payload.
        BucketMetadata metadata = createVectorIndexAndLoadBucketMetadata();
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector(SELECTOR, IndexSelectionPolicy.ALL);

        AppendedEntry[] entries = getAppendedEntries();
        AppendedEntry entry = entries[0];
        ObjectId objectId = new ObjectId();
        byte[] objectIdBytes = objectId.toByteArray();
        byte[] encodedIndexEntry = new IndexEntry(SHARD_ID, entry.metadataBytes()).encode();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setMutationLog(tr, vectorIndex.subspace(), MutationLogMarker.INSERT, objectIdBytes, encodedIndexEntry, TEST_VECTOR, entry.userVersion());
            tr.commit().join();
        }

        List<KeyValue> logEntries = getMutationLogEntries(vectorIndex.subspace());
        assertEquals(1, logEntries.size());

        MutationLogValue decoded = MutationLogValue.decode(logEntries.get(0).getValue());
        assertEquals(MutationLogMarker.INSERT, decoded.marker());
        assertArrayEquals(objectIdBytes, decoded.objectIdBytes());
        assertNotNull(decoded.vectorPayload());
        assertArrayEquals(TEST_VECTOR, decoded.vectorPayload().vector());
        assertEquals(SHARD_ID, decoded.vectorPayload().indexEntry().shardId());
        assertArrayEquals(entry.metadataBytes(), decoded.vectorPayload().indexEntry().entryMetadata());
    }

    @Test
    void shouldUpdateMutationLog() {
        // Behavior: updateMutationLog records an UPDATE mutation log entry with the correct marker,
        // objectIdBytes, and vector payload.
        BucketMetadata metadata = createVectorIndexAndLoadBucketMetadata();
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector(SELECTOR, IndexSelectionPolicy.ALL);

        AppendedEntry[] entries = getAppendedEntries();
        AppendedEntry entry = entries[0];
        ObjectId objectId = new ObjectId();
        byte[] objectIdBytes = objectId.toByteArray();
        byte[] encodedIndexEntry = new IndexEntry(SHARD_ID, entry.metadataBytes()).encode();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setMutationLog(tr, vectorIndex.subspace(), MutationLogMarker.UPDATE, objectIdBytes, encodedIndexEntry, TEST_VECTOR, entry.userVersion());
            tr.commit().join();
        }

        List<KeyValue> logEntries = getMutationLogEntries(vectorIndex.subspace());
        assertEquals(1, logEntries.size());

        MutationLogValue decoded = MutationLogValue.decode(logEntries.get(0).getValue());
        assertEquals(MutationLogMarker.UPDATE, decoded.marker());
        assertArrayEquals(objectIdBytes, decoded.objectIdBytes());
        assertNotNull(decoded.vectorPayload());
        assertArrayEquals(TEST_VECTOR, decoded.vectorPayload().vector());
        assertEquals(SHARD_ID, decoded.vectorPayload().indexEntry().shardId());
        assertArrayEquals(entry.metadataBytes(), decoded.vectorPayload().indexEntry().entryMetadata());
    }

    @Test
    void shouldDeleteMutationLog() {
        // Behavior: deleteMutationLog records a DELETE mutation log entry with the correct marker,
        // objectIdBytes, and no vector payload.
        BucketMetadata metadata = createVectorIndexAndLoadBucketMetadata();
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector(SELECTOR, IndexSelectionPolicy.ALL);

        AppendedEntry[] entries = getAppendedEntries();
        AppendedEntry entry = entries[0];
        ObjectId objectId = new ObjectId();
        byte[] objectIdBytes = objectId.toByteArray();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.deleteMutationLog(tr, vectorIndex.subspace(), objectIdBytes, entry.userVersion());
            tr.commit().join();
        }

        List<KeyValue> logEntries = getMutationLogEntries(vectorIndex.subspace());
        assertEquals(1, logEntries.size());

        MutationLogValue decoded = MutationLogValue.decode(logEntries.get(0).getValue());
        assertEquals(MutationLogMarker.DELETE, decoded.marker());
        assertArrayEquals(objectIdBytes, decoded.objectIdBytes());
        assertNull(decoded.vectorPayload());
    }

    @Test
    void shouldRejectVectorWithNaNComponent() {
        // Behavior: extractVector rejects a document whose vector component is NaN.
        String name = VectorIndexNameGenerator.generate(SELECTOR, DIMENSIONS, DistanceFunction.COSINE);
        VectorIndexDefinition definition = VectorIndexDefinition.create(name, SELECTOR, DIMENSIONS, DistanceFunction.COSINE, IndexStatus.WAITING);
        BsonDocument document = new BsonDocument();
        BsonArray array = new BsonArray();
        array.add(new BsonDouble(0.1));
        array.add(new BsonDouble(Double.NaN));
        array.add(new BsonDouble(0.3));
        document.put(SELECTOR, array);

        KronotopException ex = assertThrows(KronotopException.class, () ->
                VectorIndexMaintainer.extractVector(definition, document));
        assertTrue(ex.getMessage().contains("overflows float range"));
    }

    @Test
    void shouldReturnNullForMissingVectorField() {
        // Behavior: extractVector returns null when the selector field is missing from the document
        // or when the field value is BsonNull.
        String name = VectorIndexNameGenerator.generate(SELECTOR, DIMENSIONS, DistanceFunction.COSINE);
        VectorIndexDefinition definition = VectorIndexDefinition.create(name, SELECTOR, DIMENSIONS, DistanceFunction.COSINE, IndexStatus.WAITING);

        BsonDocument missingFieldDoc = new BsonDocument();
        assertNull(VectorIndexMaintainer.extractVector(definition, missingFieldDoc),
                "Should return null when vector field is missing");

        BsonDocument nullFieldDoc = new BsonDocument();
        nullFieldDoc.put(SELECTOR, BsonNull.VALUE);
        assertNull(VectorIndexMaintainer.extractVector(definition, nullFieldDoc),
                "Should return null when vector field is BsonNull");
    }
}
