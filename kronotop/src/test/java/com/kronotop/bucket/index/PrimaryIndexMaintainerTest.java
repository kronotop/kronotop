/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.index;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.volume.AppendedEntry;
import com.kronotop.volume.VolumeTestUtil;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class PrimaryIndexMaintainerTest extends BaseIndexMaintainerTest {

    @BeforeEach
    public void setUp() {
        createBucket(TEST_BUCKET);
    }

    @Test
    void shouldSetDefaultPrimaryIndex() {
        AppendedEntry[] entries = getAppendedEntries();
        assertDoesNotThrow(() -> {
            BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                for (AppendedEntry entry : entries) {
                    ObjectId objectId = new ObjectId();
                    PrimaryIndexMaintainer.setEntry(
                            tr, metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE), metadata, objectId.toByteArray(), new IndexEntry(SHARD_ID, entry.metadataBytes()).encode(), entry.userVersion()
                    );
                }
                tr.commit().join();
            }

            // Verify cardinality was set correctly
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
                IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), primaryIndex.definition().id());
                assertEquals(entries.length, statistics.cardinality(), "Primary index cardinality should equal number of entries set");
            }
        });
    }

    @Test
    void shouldReadEntriesFromPrimaryIndex() {
        AppendedEntry[] entries = getAppendedEntries();
        ObjectId[] objectIds = new ObjectId[entries.length];
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < entries.length; i++) {
                objectIds[i] = new ObjectId();
                PrimaryIndexMaintainer.setEntry(
                        tr, metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE), metadata, objectIds[i].toByteArray(), new IndexEntry(SHARD_ID, entries[i].metadataBytes()).encode(), entries[i].userVersion()
                );
            }
            tr.commit().join();
        }

        Index idIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        assertNotNull(idIndex, "Index should exist");
        DirectorySubspace idIndexSubspace = idIndex.subspace();
        byte[] prefix = idIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        byte[] expectedEntryMetadata = getEncodedEntryMetadata();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexedEntries = tr.getRange(begin, end).asList().join();
            assertEquals(3, indexedEntries.size());

            for (KeyValue entry : indexedEntries) {
                Tuple unpackedIndex = idIndexSubspace.unpack(entry.getKey());
                byte[] objectIdBytes = (byte[]) unpackedIndex.get(1);
                ObjectId objectId = new ObjectId(objectIdBytes);
                // Verify it's one of the ObjectIds we inserted
                int matchedIndex = -1;
                for (int i = 0; i < objectIds.length; i++) {
                    if (objectIds[i].equals(objectId)) {
                        matchedIndex = i;
                        break;
                    }
                }
                assertTrue(matchedIndex >= 0, "ObjectId should be one of the inserted ones");

                IndexEntry indexEntry = IndexEntry.decode(entry.getValue());
                assertEquals(SHARD_ID, indexEntry.shardId());
                assertArrayEquals(expectedEntryMetadata, indexEntry.entryMetadata());
            }
        }
    }

    @Test
    void shouldDropEntry() {
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();
        ObjectId[] objectIds = new ObjectId[entries.length];

        // First set the ID index entries
        CompletableFuture<byte[]> future;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < entries.length; i++) {
                objectIds[i] = new ObjectId();
                PrimaryIndexMaintainer.setEntry(tr, metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE), metadata, objectIds[i].toByteArray(), new IndexEntry(SHARD_ID, entries[i].metadataBytes()).encode(), entries[i].userVersion());
            }
            future = tr.getVersionstamp();
            tr.commit().join();
        }
        byte[] trVersion = future.join();

        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        assertNotNull(primaryIndex, "Primary index should exist");
        DirectorySubspace primaryIndexSubspace = primaryIndex.subspace();

        byte[] prefix = primaryIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(entries.length, indexEntries.size(), "Should have entries for all appended entries");
        }

        // Drop the first primary index entry
        ObjectId objectIdToRemove = objectIds[0];
        Versionstamp versionstamp = Versionstamp.complete(trVersion, entries[0].userVersion());
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrimaryIndexMaintainer.dropEntry(
                    tr,
                    objectIdToRemove.toByteArray(),
                    versionstamp,
                    metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE),
                    metadata
            );
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
                byte[] objectIdBytes = (byte[]) unpacked.get(1);
                ObjectId entryObjectId = new ObjectId(objectIdBytes);
                if (entryObjectId.equals(objectIdToRemove)) {
                    found = true;
                    break;
                }
            }
            assertFalse(found, "Dropped ObjectId should not be found in remaining entries");

            // Verify cardinality was decremented
            IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), primaryIndex.definition().id());
            assertEquals(entries.length - 1, statistics.cardinality(), "Primary index cardinality should be decremented by 1");
        }

        // Verify volume pointer was removed for the dropped entry
        Map<ObjectId, Integer> volumePointers = getVolumePointers(primaryIndexSubspace);
        assertEquals(entries.length - 1, volumePointers.size(), "Should have one less volume pointer after dropping");
        assertFalse(volumePointers.containsKey(objectIdToRemove), "Dropped ObjectId should not have a volume pointer");
    }

    @Test
    void shouldUpdateIndexEntry() {
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();
        ObjectId[] objectIds = new ObjectId[entries.length];

        // First set the primary index entries
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < entries.length; i++) {
                objectIds[i] = new ObjectId();
                PrimaryIndexMaintainer.setEntry(tr, metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE), metadata, objectIds[i].toByteArray(), new IndexEntry(SHARD_ID, entries[i].metadataBytes()).encode(), entries[i].userVersion());
            }
            tr.commit().join();
        }

        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        assertNotNull(primaryIndex, "Primary index should exist");
        DirectorySubspace primaryIndexSubspace = primaryIndex.subspace();

        // Get the original index entry
        byte[] originalIndexEntry;
        byte[] prefix = primaryIndexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<KeyValue> indexEntries = tr.getRange(begin, end).asList().join();
            assertEquals(entries.length, indexEntries.size(), "Should have entries for all appended entries");
            originalIndexEntry = indexEntries.get(0).getValue();
        }

        // Create new metadata for update
        byte[] newMetadata = VolumeTestUtil.generateEntryMetadata(1, 2, 1, 2, "updated-primary").encode();
        assertFalse(Arrays.equals(originalIndexEntry, newMetadata), "New metadata should be different from original");

        // Update the first primary index entry metadata
        ObjectId objectIdToUpdate = objectIds[0];
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrimaryIndexMaintainer.updateIndexEntry(
                    tr,
                    objectIdToUpdate.toByteArray(),
                    metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE),
                    SHARD_ID,
                    newMetadata
            );
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
                byte[] objectIdBytes = (byte[]) unpacked.get(1);
                ObjectId entryObjectId = new ObjectId(objectIdBytes);
                if (entryObjectId.equals(objectIdToUpdate)) {
                    IndexEntry expectedEntry = new IndexEntry(SHARD_ID, newMetadata);
                    IndexEntry decodedEntry = IndexEntry.decode(kv.getValue());
                    assertArrayEquals(expectedEntry.encode(), decodedEntry.encode(), "Entry metadata should be updated");
                    assertFalse(Arrays.equals(originalIndexEntry, decodedEntry.encode()), "Updated metadata should differ from original");
                    found = true;
                    break;
                }
            }
            assertTrue(found, "Updated entry should be found in primary index");
        }
    }

    @Test
    void shouldGetVolumePointerByObjectId() {
        // Behavior: Verifies that getVolumePointer returns the correct Versionstamp for an inserted document's ObjectId.
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();
        AppendedEntry entry = entries[0];
        ObjectId objectId = new ObjectId();

        CompletableFuture<byte[]> future;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrimaryIndexMaintainer.setEntry(
                    tr, metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE), metadata, objectId.toByteArray(), new IndexEntry(SHARD_ID, entry.metadataBytes()).encode(), entry.userVersion()
            );
            future = tr.getVersionstamp();
            tr.commit().join();
        }
        byte[] trVersion = future.join();

        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Versionstamp result = PrimaryIndexMaintainer.getVolumePointer(tr, primaryIndex, objectId.toByteArray(), metadata);
            assertNotNull(result, "Should return a Versionstamp for an inserted document");
            Versionstamp expected = Versionstamp.complete(trVersion, entry.userVersion());
            assertEquals(expected, result);
        }
    }

    @Test
    void shouldGetVolumePointerForMultipleDocuments() {
        // Behavior: Verifies that getVolumePointer returns correct, distinct Versionstamps for multiple documents inserted in the same transaction.
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();
        ObjectId[] objectIds = new ObjectId[entries.length];

        CompletableFuture<byte[]> future;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < entries.length; i++) {
                objectIds[i] = new ObjectId();
                PrimaryIndexMaintainer.setEntry(
                        tr, metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE), metadata, objectIds[i].toByteArray(), new IndexEntry(SHARD_ID, entries[i].metadataBytes()).encode(), entries[i].userVersion()
                );
            }
            future = tr.getVersionstamp();
            tr.commit().join();
        }
        byte[] trVersion = future.join();

        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < entries.length; i++) {
                Versionstamp result = PrimaryIndexMaintainer.getVolumePointer(tr, primaryIndex, objectIds[i].toByteArray(), metadata);
                assertNotNull(result, "Should return a Versionstamp for ObjectId " + i);
                Versionstamp expected = Versionstamp.complete(trVersion, entries[i].userVersion());
                assertEquals(expected, result, "Versionstamp should match for ObjectId " + i);
            }
        }
    }

    @Test
    void shouldReturnNullVolumePointerForNonExistentObjectId() {
        // Behavior: Verifies that getVolumePointer returns null when queried with an ObjectId that was never inserted.
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Versionstamp result = PrimaryIndexMaintainer.getVolumePointer(tr, primaryIndex, new ObjectId().toByteArray(), metadata);
            assertNull(result, "Should return null for non-existent ObjectId");
        }
    }

    @Test
    void shouldSetVolumePointerForPrimaryIndex() {
        // Behavior: Verifies that setPrimaryIndexEntry correctly creates a volume pointer entry with
        // structure (VOLUME_POINTER, Versionstamp, ObjectId) and NULL_VALUE.
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();
        AppendedEntry entry = entries[0];
        ObjectId objectId = new ObjectId();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrimaryIndexMaintainer.setEntry(tr, metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE), metadata, objectId.toByteArray(), new IndexEntry(SHARD_ID, entry.metadataBytes()).encode(), entry.userVersion());
            tr.commit().join();
        }

        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        assertNotNull(primaryIndex, "Primary index should exist");
        DirectorySubspace indexSubspace = primaryIndex.subspace();

        Map<ObjectId, Integer> volumePointers = getVolumePointers(indexSubspace);
        assertEquals(1, volumePointers.size(), "Should have exactly one volume pointer");
        assertTrue(volumePointers.containsKey(objectId), "Volume pointer should contain the ObjectId");
        assertEquals(entry.userVersion(), volumePointers.get(objectId), "UserVersion should match AppendedEntry.userVersion()");
    }


    @Test
    void shouldSetMultipleVolumePointersForMultipleDocuments() {
        // Behavior: Verifies that multiple volume pointers are created correctly when inserting
        // multiple documents, each with its own ObjectId and userVersion.
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();
        ObjectId[] objectIds = new ObjectId[entries.length];

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < entries.length; i++) {
                objectIds[i] = new ObjectId();
                PrimaryIndexMaintainer.setEntry(
                        tr, metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE), metadata, objectIds[i].toByteArray(), new IndexEntry(SHARD_ID, entries[i].metadataBytes()).encode(), entries[i].userVersion()
                );
            }
            tr.commit().join();
        }

        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        assertNotNull(primaryIndex, "Primary index should exist");
        DirectorySubspace indexSubspace = primaryIndex.subspace();

        Map<ObjectId, Integer> volumePointers = getVolumePointers(indexSubspace);
        assertEquals(entries.length, volumePointers.size(), "Should have volume pointers for all documents");

        for (int i = 0; i < entries.length; i++) {
            assertTrue(volumePointers.containsKey(objectIds[i]), "Volume pointer should contain ObjectId " + i);
            assertEquals(entries[i].userVersion(), volumePointers.get(objectIds[i]),
                    "UserVersion should match AppendedEntry.userVersion() for entry " + i);
        }
    }

    @Test
    void shouldDropVolumePointerForPrimaryIndex() {
        // Behavior: Verifies that both a volume pointer and its reverse pointer are removed when
        // dropPrimaryIndexEntry is called.
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();
        AppendedEntry entry = entries[0];
        ObjectId objectId = new ObjectId();

        CompletableFuture<byte[]> future;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrimaryIndexMaintainer.setEntry(
                    tr, metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE), metadata, objectId.toByteArray(), new IndexEntry(SHARD_ID, entry.metadataBytes()).encode(), entry.userVersion()
            );
            future = tr.getVersionstamp();
            tr.commit().join();
        }
        byte[] trVersion = future.join();

        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        assertNotNull(primaryIndex, "Primary index should exist");
        DirectorySubspace indexSubspace = primaryIndex.subspace();

        // Verify pointers exist before dropping
        Map<ObjectId, Integer> volumePointersBefore = getVolumePointers(indexSubspace);
        assertEquals(1, volumePointersBefore.size(), "Should have one volume pointer before dropping");

        // Drop the primary index entry
        Versionstamp versionstamp = Versionstamp.complete(trVersion, entry.userVersion());
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Index index = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE);
            PrimaryIndexMaintainer.dropEntry(tr, objectId.toByteArray(), versionstamp, index, metadata);
            tr.commit().join();
        }

        // Verify both pointers are removed
        Map<ObjectId, Integer> volumePointersAfter = getVolumePointers(indexSubspace);
        assertEquals(0, volumePointersAfter.size(), "Should have no volume pointers after dropping");
    }

    @Test
    void shouldReturnCachedVolumePointer() {
        // Behavior: Verifies that getVolumePointer populates the cache on first call and returns the cached value on subsequent calls.
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();
        AppendedEntry entry = entries[0];
        ObjectId objectId = new ObjectId();

        CompletableFuture<byte[]> future;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrimaryIndexMaintainer.setEntry(
                    tr, metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE), metadata, objectId.toByteArray(), new IndexEntry(SHARD_ID, entry.metadataBytes()).encode(), entry.userVersion()
            );
            future = tr.getVersionstamp();
            tr.commit().join();
        }
        byte[] trVersion = future.join();

        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        assertEquals(0, metadata.volumePointerCache().size(), "Cache should be empty before first lookup");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Versionstamp first = PrimaryIndexMaintainer.getVolumePointer(tr, primaryIndex, objectId.toByteArray(), metadata);
            assertNotNull(first);
            assertEquals(1, metadata.volumePointerCache().size(), "Cache should have one entry after first lookup");

            Versionstamp second = PrimaryIndexMaintainer.getVolumePointer(tr, primaryIndex, objectId.toByteArray(), metadata);
            assertEquals(first, second, "Second lookup should return the same Versionstamp");
            assertEquals(1, metadata.volumePointerCache().size(), "Cache size should remain 1 after second lookup");
        }

        Versionstamp expected = Versionstamp.complete(trVersion, entry.userVersion());
        assertEquals(expected, metadata.volumePointerCache().getIfPresent(objectId));
    }

    @Test
    void shouldInvalidateCacheOnDropEntry() {
        // Behavior: Verifies that dropEntry removes the corresponding entry from the volume pointer cache.
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        AppendedEntry[] entries = getAppendedEntries();
        AppendedEntry entry = entries[0];
        ObjectId objectId = new ObjectId();

        CompletableFuture<byte[]> future;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrimaryIndexMaintainer.setEntry(
                    tr, metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE), metadata, objectId.toByteArray(), new IndexEntry(SHARD_ID, entry.metadataBytes()).encode(), entry.userVersion()
            );
            future = tr.getVersionstamp();
            tr.commit().join();
        }
        byte[] trVersion = future.join();

        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrimaryIndexMaintainer.getVolumePointer(tr, primaryIndex, objectId.toByteArray(), metadata);
        }
        assertEquals(1, metadata.volumePointerCache().size(), "Cache should have one entry after lookup");

        Versionstamp versionstamp = Versionstamp.complete(trVersion, entry.userVersion());
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrimaryIndexMaintainer.dropEntry(tr, objectId.toByteArray(), versionstamp, metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE), metadata);
            tr.commit().join();
        }
        assertEquals(0, metadata.volumePointerCache().size(), "Cache should be empty after dropEntry");
    }

    @Test
    void shouldNotCacheNullVolumePointer() {
        // Behavior: Verifies that a cache miss returning null does not pollute the cache.
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Versionstamp result = PrimaryIndexMaintainer.getVolumePointer(tr, primaryIndex, new ObjectId().toByteArray(), metadata);
            assertNull(result, "Should return null for non-existent ObjectId");
        }
        assertEquals(0, metadata.volumePointerCache().size(), "Cache should remain empty after null result");
    }
}
