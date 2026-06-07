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

package com.kronotop.bucket;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.*;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.Prefix;
import com.kronotop.volume.Subspaces;
import com.kronotop.volume.Volume;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BucketEntryEvacuatorTest extends BaseBucketHandlerTest {

    private List<VolumeEntry> scanVolumeEntries(Volume volume, Prefix prefix, Transaction tr) {
        byte[] scanPrefix = volume.getConfig().subspace().pack(
                Tuple.from(Subspaces.ENTRY_SUBSPACE, prefix.asBytes())
        );
        Range range = Range.startsWith(scanPrefix);
        List<VolumeEntry> result = new ArrayList<>();
        for (KeyValue kv : tr.getRange(range)) {
            Tuple unpacked = volume.getConfig().subspace().unpack(kv.getKey());
            Versionstamp versionstamp = unpacked.getVersionstamp(2);
            EntryMetadata entryMetadata = EntryMetadata.decode(kv.getValue());
            result.add(new VolumeEntry(versionstamp, entryMetadata));
        }
        return result;
    }

    private byte[] readNewEncodedMetadata(Volume volume, Prefix prefix, Versionstamp versionstamp, Transaction tr) {
        byte[] entryKey = volume.getConfig().subspace().pack(
                Tuple.from(Subspaces.ENTRY_SUBSPACE, prefix.asBytes(), versionstamp)
        );
        byte[] value = tr.get(entryKey).join();
        assertNotNull(value);
        return value;
    }

    private void verifyPrimaryIndex(Transaction tr, BucketMetadata metadata, ObjectId objectId, int shardId, byte[] expectedEncodedMetadata) {
        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE);
        byte[] key = primaryIndex.subspace().pack(
                Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), objectId.toByteArray())
        );
        byte[] value = tr.get(key).join();
        assertNotNull(value);
        IndexEntry decoded = IndexEntry.decode(value);
        assertEquals(shardId, decoded.shardId());
        assertArrayEquals(expectedEncodedMetadata, decoded.entryMetadata());
    }

    private void verifySingleFieldIndex(Transaction tr, BucketMetadata metadata, String selector, ObjectId objectId, int shardId, byte[] expectedEncodedMetadata) {
        Index sfIndex = metadata.indexes().getIndex(selector, IndexSelectionPolicy.READWRITE);
        byte[] prefix = sfIndex.subspace().pack(
                Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue(), objectId.toByteArray())
        );
        Range range = Range.startsWith(prefix);
        List<KeyValue> backPointers = tr.getRange(range).asList().join();
        assertFalse(backPointers.isEmpty());

        for (KeyValue bp : backPointers) {
            Tuple unpacked = sfIndex.subspace().unpack(bp.getKey());
            Tuple entryTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), unpacked.get(2), objectId.toByteArray());
            byte[] entryKey = sfIndex.subspace().pack(entryTuple);
            byte[] entryValue = tr.get(entryKey).join();
            assertNotNull(entryValue);
            IndexEntry decoded = IndexEntry.decode(entryValue);
            assertEquals(shardId, decoded.shardId());
            assertArrayEquals(expectedEncodedMetadata, decoded.entryMetadata());
        }
    }

    private int verifySingleFieldIndexAndCountBackPointers(Transaction tr, BucketMetadata metadata, String selector, ObjectId objectId, int shardId, byte[] expectedEncodedMetadata) {
        Index sfIndex = metadata.indexes().getIndex(selector, IndexSelectionPolicy.READWRITE);
        byte[] prefix = sfIndex.subspace().pack(
                Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue(), objectId.toByteArray())
        );
        Range range = Range.startsWith(prefix);
        List<KeyValue> backPointers = tr.getRange(range).asList().join();
        assertFalse(backPointers.isEmpty());

        for (KeyValue bp : backPointers) {
            Tuple unpacked = sfIndex.subspace().unpack(bp.getKey());
            Tuple entryTuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), unpacked.get(2), objectId.toByteArray());
            byte[] entryKey = sfIndex.subspace().pack(entryTuple);
            byte[] entryValue = tr.get(entryKey).join();
            assertNotNull(entryValue);
            IndexEntry decoded = IndexEntry.decode(entryValue);
            assertEquals(shardId, decoded.shardId());
            assertArrayEquals(expectedEncodedMetadata, decoded.entryMetadata());
        }
        return backPointers.size();
    }

    private void verifyCompoundIndex(Transaction tr, BucketMetadata metadata, ObjectId objectId, int shardId, byte[] expectedEncodedMetadata) {
        for (CompoundIndex ci : metadata.compoundIndexes().getIndexes(IndexSelectionPolicy.READWRITE)) {
            byte[] prefix = ci.subspace().pack(
                    Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue(), objectId.toByteArray())
            );
            Range range = Range.startsWith(prefix);
            List<KeyValue> backPointers = tr.getRange(range).asList().join();
            assertFalse(backPointers.isEmpty());

            int fieldCount = ci.definition().fields().size();
            for (KeyValue bp : backPointers) {
                Tuple unpacked = ci.subspace().unpack(bp.getKey());
                Object[] items = new Object[fieldCount + 2];
                items[0] = IndexSubspaceMagic.ENTRIES.getValue();
                for (int i = 0; i < fieldCount; i++) {
                    items[i + 1] = unpacked.get(2 + i);
                }
                items[fieldCount + 1] = objectId.toByteArray();
                byte[] entryKey = ci.subspace().pack(Tuple.from(items));
                byte[] entryValue = tr.get(entryKey).join();
                assertNotNull(entryValue);
                IndexEntry decoded = IndexEntry.decode(entryValue);
                assertEquals(shardId, decoded.shardId());
                assertArrayEquals(expectedEncodedMetadata, decoded.entryMetadata());
            }
        }
    }

    private int verifyCompoundIndexAndCountBackPointers(Transaction tr, BucketMetadata metadata, ObjectId objectId, int shardId, byte[] expectedEncodedMetadata) {
        int totalBackPointers = 0;
        for (CompoundIndex ci : metadata.compoundIndexes().getIndexes(IndexSelectionPolicy.READWRITE)) {
            byte[] prefix = ci.subspace().pack(
                    Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue(), objectId.toByteArray())
            );
            Range range = Range.startsWith(prefix);
            List<KeyValue> backPointers = tr.getRange(range).asList().join();
            assertFalse(backPointers.isEmpty());
            totalBackPointers += backPointers.size();

            int fieldCount = ci.definition().fields().size();
            for (KeyValue bp : backPointers) {
                Tuple unpacked = ci.subspace().unpack(bp.getKey());
                Object[] items = new Object[fieldCount + 2];
                items[0] = IndexSubspaceMagic.ENTRIES.getValue();
                for (int i = 0; i < fieldCount; i++) {
                    items[i + 1] = unpacked.get(2 + i);
                }
                items[fieldCount + 1] = objectId.toByteArray();
                byte[] entryKey = ci.subspace().pack(Tuple.from(items));
                byte[] entryValue = tr.get(entryKey).join();
                assertNotNull(entryValue);
                IndexEntry decoded = IndexEntry.decode(entryValue);
                assertEquals(shardId, decoded.shardId());
                assertArrayEquals(expectedEncodedMetadata, decoded.entryMetadata());
            }
        }
        return totalBackPointers;
    }

    private void verifyVectorIndex(Transaction tr, BucketMetadata metadata, ObjectId objectId, int shardId, byte[] expectedEncodedMetadata, float[] expectedVector) {
        for (VectorIndex vi : metadata.vectorIndexes().getIndexes(IndexSelectionPolicy.READWRITE)) {
            byte[] key = vi.subspace().pack(
                    Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), objectId.toByteArray())
            );
            byte[] value = tr.get(key).join();
            assertNotNull(value);
            VectorIndexValue decoded = VectorIndexValue.decode(value);
            assertEquals(shardId, decoded.indexEntry().shardId());
            assertArrayEquals(expectedEncodedMetadata, decoded.indexEntry().entryMetadata());
            assertArrayEquals(expectedVector, decoded.vector());
        }
    }

    @Test
    void shouldEvacuateEntryAndUpdatePrimaryIndex() throws IOException {
        // Behavior: evacuating an entry rewrites the document to a new segment position and updates the primary index entry with the new EntryMetadata
        createBucket(TEST_BUCKET);

        Map<ObjectId, byte[]> inserted = insertDocumentsAndGetObjectIds(
                List.of(BSONUtil.jsonToDocumentThenBytes("{\"key\": \"value\"}"))
        );
        ObjectId objectId = inserted.keySet().iterator().next();

        Volume volume = getShard(TEST_SHARD_ID).volume();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        VolumeEntry entry;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<VolumeEntry> entries = scanVolumeEntries(volume, metadata.prefix(), tr);
            assertEquals(1, entries.size());
            entry = entries.getFirst();
        }

        BucketEntryEvacuator evacuator = new BucketEntryEvacuator(volume, TEST_SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            boolean result = evacuator.evacuate(context, tr, entry.entryMetadata(), entry.versionstamp());
            tr.commit().join();
            assertTrue(result);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] newEncodedMetadata = readNewEncodedMetadata(volume, metadata.prefix(), entry.versionstamp(), tr);
            EntryMetadata newEntryMetadata = EntryMetadata.decode(newEncodedMetadata);
            assertNotEquals(entry.entryMetadata().position(), newEntryMetadata.position());

            verifyPrimaryIndex(tr, metadata, objectId, TEST_SHARD_ID, newEncodedMetadata);
        }
    }

    @Test
    void shouldEvacuateEntryAndUpdateSingleFieldIndex() throws IOException {
        // Behavior: evacuating an entry with a single field index updates both the primary index and the single field index entry values
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create(
                "age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING
        );
        createIndexThenWaitForReadiness(ageIndex);

        Map<ObjectId, byte[]> inserted = insertDocumentsAndGetObjectIds(
                List.of(BSONUtil.jsonToDocumentThenBytes("{\"age\": 25}"))
        );
        ObjectId objectId = inserted.keySet().iterator().next();

        Volume volume = getShard(TEST_SHARD_ID).volume();
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        VolumeEntry entry;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<VolumeEntry> entries = scanVolumeEntries(volume, metadata.prefix(), tr);
            assertEquals(1, entries.size());
            entry = entries.getFirst();
        }

        BucketEntryEvacuator evacuator = new BucketEntryEvacuator(volume, TEST_SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            boolean result = evacuator.evacuate(context, tr, entry.entryMetadata(), entry.versionstamp());
            tr.commit().join();
            assertTrue(result);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] newEncodedMetadata = readNewEncodedMetadata(volume, metadata.prefix(), entry.versionstamp(), tr);
            verifyPrimaryIndex(tr, metadata, objectId, TEST_SHARD_ID, newEncodedMetadata);
            verifySingleFieldIndex(tr, metadata, "age", objectId, TEST_SHARD_ID, newEncodedMetadata);
        }
    }

    @Test
    void shouldEvacuateEntryAndUpdateCompoundIndex() throws IOException {
        // Behavior: evacuating an entry with a compound index updates the compound index entry values via back pointers
        CompoundIndexDefinition compoundDef = CompoundIndexDefinition.create(
                "name_age_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                ),
                IndexStatus.WAITING
        );
        createIndexThenWaitForReadiness(compoundDef);

        Map<ObjectId, byte[]> inserted = insertDocumentsAndGetObjectIds(
                List.of(BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}"))
        );
        ObjectId objectId = inserted.keySet().iterator().next();

        Volume volume = getShard(TEST_SHARD_ID).volume();
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        VolumeEntry entry;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<VolumeEntry> entries = scanVolumeEntries(volume, metadata.prefix(), tr);
            assertEquals(1, entries.size());
            entry = entries.getFirst();
        }

        BucketEntryEvacuator evacuator = new BucketEntryEvacuator(volume, TEST_SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            boolean result = evacuator.evacuate(context, tr, entry.entryMetadata(), entry.versionstamp());
            tr.commit().join();
            assertTrue(result);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] newEncodedMetadata = readNewEncodedMetadata(volume, metadata.prefix(), entry.versionstamp(), tr);
            verifyPrimaryIndex(tr, metadata, objectId, TEST_SHARD_ID, newEncodedMetadata);
            verifyCompoundIndex(tr, metadata, objectId, TEST_SHARD_ID, newEncodedMetadata);
        }
    }

    @Test
    void shouldEvacuateEntryAndUpdateVectorIndex() throws IOException {
        // Behavior: evacuating an entry with a vector index updates the vector index entry while preserving the original vector data
        createBucket(TEST_BUCKET);

        String vectorIndexName = VectorIndexNameGenerator.generate("embedding", 3, DistanceFunction.COSINE);
        VectorIndexDefinition vectorDef = VectorIndexDefinition.create(
                vectorIndexName, "embedding", 3, DistanceFunction.COSINE, IndexStatus.READY
        );
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            VectorIndexUtil.create(tx, getBucketMetadata(TEST_BUCKET), vectorDef);
            tr.commit().join();
        }

        Map<ObjectId, byte[]> inserted = insertDocumentsAndGetObjectIds(
                List.of(BSONUtil.jsonToDocumentThenBytes("{\"embedding\": [1.0, 2.0, 3.0]}"))
        );
        ObjectId objectId = inserted.keySet().iterator().next();

        Volume volume = getShard(TEST_SHARD_ID).volume();
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        VolumeEntry entry;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<VolumeEntry> entries = scanVolumeEntries(volume, metadata.prefix(), tr);
            assertEquals(1, entries.size());
            entry = entries.getFirst();
        }

        BucketEntryEvacuator evacuator = new BucketEntryEvacuator(volume, TEST_SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            boolean result = evacuator.evacuate(context, tr, entry.entryMetadata(), entry.versionstamp());
            tr.commit().join();
            assertTrue(result);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] newEncodedMetadata = readNewEncodedMetadata(volume, metadata.prefix(), entry.versionstamp(), tr);
            verifyPrimaryIndex(tr, metadata, objectId, TEST_SHARD_ID, newEncodedMetadata);
            verifyVectorIndex(tr, metadata, objectId, TEST_SHARD_ID, newEncodedMetadata, new float[]{1.0f, 2.0f, 3.0f});
        }
    }

    @Test
    void shouldEvacuateEntryAndUpdateAllIndexTypes() throws IOException {
        // Behavior: evacuating an entry updates the primary index, single field index, compound index, and vector index simultaneously
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create(
                "age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING
        );
        createIndexThenWaitForReadiness(ageIndex);

        BucketMetadata metadataBeforeCompound = getBucketMetadata(TEST_BUCKET);
        CompoundIndexDefinition compoundDef = CompoundIndexDefinition.create(
                "name_age_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                ),
                IndexStatus.WAITING
        );
        DirectorySubspace compoundSubspace;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            compoundSubspace = CompoundIndexUtil.create(tx, metadataBeforeCompound, compoundDef);
            tr.commit().join();
        }
        waitForCompoundIndexReadiness(compoundSubspace);

        String vectorIndexName = VectorIndexNameGenerator.generate("embedding", 3, DistanceFunction.COSINE);
        VectorIndexDefinition vectorDef = VectorIndexDefinition.create(
                vectorIndexName, "embedding", 3, DistanceFunction.COSINE, IndexStatus.READY
        );
        BucketMetadata metadataBeforeVector = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            VectorIndexUtil.create(tx, metadataBeforeVector, vectorDef);
            tr.commit().join();
        }

        Map<ObjectId, byte[]> inserted = insertDocumentsAndGetObjectIds(
                List.of(BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 42, \"embedding\": [0.1, 0.2, 0.3]}"))
        );
        ObjectId objectId = inserted.keySet().iterator().next();

        Volume volume = getShard(TEST_SHARD_ID).volume();
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        VolumeEntry entry;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<VolumeEntry> entries = scanVolumeEntries(volume, metadata.prefix(), tr);
            assertEquals(1, entries.size());
            entry = entries.getFirst();
        }

        BucketEntryEvacuator evacuator = new BucketEntryEvacuator(volume, TEST_SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            boolean result = evacuator.evacuate(context, tr, entry.entryMetadata(), entry.versionstamp());
            tr.commit().join();
            assertTrue(result);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] newEncodedMetadata = readNewEncodedMetadata(volume, metadata.prefix(), entry.versionstamp(), tr);
            verifyPrimaryIndex(tr, metadata, objectId, TEST_SHARD_ID, newEncodedMetadata);
            verifySingleFieldIndex(tr, metadata, "age", objectId, TEST_SHARD_ID, newEncodedMetadata);
            verifyCompoundIndex(tr, metadata, objectId, TEST_SHARD_ID, newEncodedMetadata);
            verifyVectorIndex(tr, metadata, objectId, TEST_SHARD_ID, newEncodedMetadata, new float[]{0.1f, 0.2f, 0.3f});
        }
    }

    @Test
    void shouldEvacuateMultipleEntries() throws IOException {
        // Behavior: each evacuation independently updates all index entries for the evacuated document
        SingleFieldIndexDefinition keyIndex = SingleFieldIndexDefinition.create(
                "key_idx", "key", BsonType.STRING, false, IndexStatus.WAITING
        );
        createIndexThenWaitForReadiness(keyIndex);

        List<byte[]> docs = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"key\": \"a\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"key\": \"b\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"key\": \"c\"}")
        );
        Map<ObjectId, byte[]> inserted = insertDocumentsAndGetObjectIds(docs);
        List<ObjectId> objectIds = new ArrayList<>(inserted.keySet());

        Volume volume = getShard(TEST_SHARD_ID).volume();
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        List<VolumeEntry> entries;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            entries = scanVolumeEntries(volume, metadata.prefix(), tr);
            assertEquals(3, entries.size());
        }

        BucketEntryEvacuator evacuator = new BucketEntryEvacuator(volume, TEST_SHARD_ID);
        for (VolumeEntry entry : entries) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                boolean result = evacuator.evacuate(context, tr, entry.entryMetadata(), entry.versionstamp());
                tr.commit().join();
                assertTrue(result);
            }
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < entries.size(); i++) {
                byte[] newEncodedMetadata = readNewEncodedMetadata(volume, metadata.prefix(), entries.get(i).versionstamp(), tr);
                verifyPrimaryIndex(tr, metadata, objectIds.get(i), TEST_SHARD_ID, newEncodedMetadata);
                verifySingleFieldIndex(tr, metadata, "key", objectIds.get(i), TEST_SHARD_ID, newEncodedMetadata);
            }
        }
    }

    @Test
    void shouldReturnFalseWhenMaxEntriesReached() throws IOException {
        // Behavior: evacuate returns false when the number of evacuated entries reaches maxEntries, then resets the counter for the next batch
        createBucket(TEST_BUCKET);

        List<byte[]> docs = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"key\": \"a\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"key\": \"b\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"key\": \"c\"}")
        );
        insertDocumentsAndGetObjectIds(docs);

        Volume volume = getShard(TEST_SHARD_ID).volume();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        List<VolumeEntry> entries;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            entries = scanVolumeEntries(volume, metadata.prefix(), tr);
            assertEquals(3, entries.size());
        }

        BucketEntryEvacuator evacuator = new BucketEntryEvacuator(volume, TEST_SHARD_ID, 2, Long.MAX_VALUE);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertTrue(evacuator.evacuate(context, tr, entries.get(0).entryMetadata(), entries.get(0).versionstamp()));
            assertFalse(evacuator.evacuate(context, tr, entries.get(1).entryMetadata(), entries.get(1).versionstamp()));
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertTrue(evacuator.evacuate(context, tr, entries.get(2).entryMetadata(), entries.get(2).versionstamp()));
            tr.commit().join();
        }
    }

    @Test
    void shouldReturnFalseWhenMaxBytesReached() throws IOException {
        // Behavior: evacuate returns false when the total bytes evacuated reaches maxBytes
        createBucket(TEST_BUCKET);

        insertDocumentsAndGetObjectIds(
                List.of(BSONUtil.jsonToDocumentThenBytes("{\"key\": \"value\"}"))
        );

        Volume volume = getShard(TEST_SHARD_ID).volume();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        VolumeEntry entry;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<VolumeEntry> entries = scanVolumeEntries(volume, metadata.prefix(), tr);
            assertEquals(1, entries.size());
            entry = entries.getFirst();
        }

        BucketEntryEvacuator evacuator = new BucketEntryEvacuator(volume, TEST_SHARD_ID, Integer.MAX_VALUE, 1);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertFalse(evacuator.evacuate(context, tr, entry.entryMetadata(), entry.versionstamp()));
            tr.commit().join();
        }
    }

    @Test
    void shouldEvacuateEntryWithMultiValueSingleFieldIndex() throws IOException {
        // Behavior: evacuating an entry with a multi-key single field index updates all back-pointed entries for array values
        SingleFieldIndexDefinition tagsIndex = SingleFieldIndexDefinition.create(
                "tags_idx", "tags", BsonType.STRING, true, IndexStatus.WAITING
        );
        createIndexThenWaitForReadiness(tagsIndex);

        Map<ObjectId, byte[]> inserted = insertDocumentsAndGetObjectIds(
                List.of(BSONUtil.jsonToDocumentThenBytes("{\"tags\": [\"red\", \"blue\", \"green\"]}"))
        );
        ObjectId objectId = inserted.keySet().iterator().next();

        Volume volume = getShard(TEST_SHARD_ID).volume();
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        VolumeEntry entry;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<VolumeEntry> entries = scanVolumeEntries(volume, metadata.prefix(), tr);
            assertEquals(1, entries.size());
            entry = entries.getFirst();
        }

        BucketEntryEvacuator evacuator = new BucketEntryEvacuator(volume, TEST_SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            boolean result = evacuator.evacuate(context, tr, entry.entryMetadata(), entry.versionstamp());
            tr.commit().join();
            assertTrue(result);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] newEncodedMetadata = readNewEncodedMetadata(volume, metadata.prefix(), entry.versionstamp(), tr);
            verifyPrimaryIndex(tr, metadata, objectId, TEST_SHARD_ID, newEncodedMetadata);
            int backPointerCount = verifySingleFieldIndexAndCountBackPointers(tr, metadata, "tags", objectId, TEST_SHARD_ID, newEncodedMetadata);
            assertEquals(3, backPointerCount);
        }
    }

    @Test
    void shouldEvacuateEntryWithMultiKeyCompoundIndex() throws IOException {
        // Behavior: evacuating an entry with a multi-key compound index updates all back-pointed entries for array element combinations
        CompoundIndexDefinition compoundDef = CompoundIndexDefinition.create(
                "name_tags_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("tags", BsonType.STRING, true)
                ),
                IndexStatus.WAITING
        );
        createIndexThenWaitForReadiness(compoundDef);

        Map<ObjectId, byte[]> inserted = insertDocumentsAndGetObjectIds(
                List.of(BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"tags\": [\"x\", \"y\"]}"))
        );
        ObjectId objectId = inserted.keySet().iterator().next();

        Volume volume = getShard(TEST_SHARD_ID).volume();
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        VolumeEntry entry;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<VolumeEntry> entries = scanVolumeEntries(volume, metadata.prefix(), tr);
            assertEquals(1, entries.size());
            entry = entries.getFirst();
        }

        BucketEntryEvacuator evacuator = new BucketEntryEvacuator(volume, TEST_SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            boolean result = evacuator.evacuate(context, tr, entry.entryMetadata(), entry.versionstamp());
            tr.commit().join();
            assertTrue(result);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] newEncodedMetadata = readNewEncodedMetadata(volume, metadata.prefix(), entry.versionstamp(), tr);
            verifyPrimaryIndex(tr, metadata, objectId, TEST_SHARD_ID, newEncodedMetadata);
            int backPointerCount = verifyCompoundIndexAndCountBackPointers(tr, metadata, objectId, TEST_SHARD_ID, newEncodedMetadata);
            assertEquals(2, backPointerCount);
        }
    }

    @Test
    void shouldPreserveDocumentContentAfterEvacuation() throws IOException {
        // Behavior: evacuating an entry rewrites it to a new position but the document content read back is byte-for-byte identical to the original
        createBucket(TEST_BUCKET);

        insertDocumentsAndGetObjectIds(
                List.of(BSONUtil.jsonToDocumentThenBytes("{\"key\": \"value\"}"))
        );

        Volume volume = getShard(TEST_SHARD_ID).volume();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        VolumeEntry entry;
        ByteBuffer originalDocument;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<VolumeEntry> entries = scanVolumeEntries(volume, metadata.prefix(), tr);
            assertEquals(1, entries.size());
            entry = entries.getFirst();
            originalDocument = volume.getByEntryMetadata(entry.entryMetadata());
        }

        BucketEntryEvacuator evacuator = new BucketEntryEvacuator(volume, TEST_SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            boolean result = evacuator.evacuate(context, tr, entry.entryMetadata(), entry.versionstamp());
            tr.commit().join();
            assertTrue(result);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] newEncodedMetadata = readNewEncodedMetadata(volume, metadata.prefix(), entry.versionstamp(), tr);
            EntryMetadata newEntryMetadata = EntryMetadata.decode(newEncodedMetadata);
            assertNotEquals(entry.entryMetadata().position(), newEntryMetadata.position());

            ByteBuffer evacuatedDocument = volume.getByEntryMetadata(newEntryMetadata);
            assertEquals(originalDocument, evacuatedDocument);
        }
    }

    @Test
    void shouldResetCounterAfterMaxBytesReached() throws IOException {
        // Behavior: evacuate returns false when maxBytes is reached, then resets the counter so the next batch starts fresh
        createBucket(TEST_BUCKET);

        List<byte[]> docs = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"key\": \"a\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"key\": \"b\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"key\": \"c\"}")
        );
        insertDocumentsAndGetObjectIds(docs);

        Volume volume = getShard(TEST_SHARD_ID).volume();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        List<VolumeEntry> entries;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            entries = scanVolumeEntries(volume, metadata.prefix(), tr);
            assertEquals(3, entries.size());
        }

        long entryLength = entries.getFirst().entryMetadata().length();
        long maxBytes = entryLength * 2 - 1;
        BucketEntryEvacuator evacuator = new BucketEntryEvacuator(volume, TEST_SHARD_ID, Integer.MAX_VALUE, maxBytes);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertTrue(evacuator.evacuate(context, tr, entries.get(0).entryMetadata(), entries.get(0).versionstamp()));
            assertFalse(evacuator.evacuate(context, tr, entries.get(1).entryMetadata(), entries.get(1).versionstamp()));
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertTrue(evacuator.evacuate(context, tr, entries.get(2).entryMetadata(), entries.get(2).versionstamp()));
            tr.commit().join();
        }
    }

    @Test
    void shouldEvacuateAlreadyEvacuatedEntry() throws IOException {
        // Behavior: an entry that has already been evacuated can be evacuated again; document content is preserved and indexes point to the latest position
        createBucket(TEST_BUCKET);

        Map<ObjectId, byte[]> inserted = insertDocumentsAndGetObjectIds(
                List.of(BSONUtil.jsonToDocumentThenBytes("{\"key\": \"value\"}"))
        );
        ObjectId objectId = inserted.keySet().iterator().next();

        Volume volume = getShard(TEST_SHARD_ID).volume();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        VolumeEntry entry;
        ByteBuffer originalDocument;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<VolumeEntry> entries = scanVolumeEntries(volume, metadata.prefix(), tr);
            assertEquals(1, entries.size());
            entry = entries.getFirst();
            originalDocument = volume.getByEntryMetadata(entry.entryMetadata());
        }

        // First evacuation
        BucketEntryEvacuator evacuator = new BucketEntryEvacuator(volume, TEST_SHARD_ID);
        EntryMetadata firstEvacMetadata;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertTrue(evacuator.evacuate(context, tr, entry.entryMetadata(), entry.versionstamp()));
            tr.commit().join();
        }
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] encoded = readNewEncodedMetadata(volume, metadata.prefix(), entry.versionstamp(), tr);
            firstEvacMetadata = EntryMetadata.decode(encoded);
            assertNotEquals(entry.entryMetadata().position(), firstEvacMetadata.position());
        }

        // Second evacuation
        evacuator = new BucketEntryEvacuator(volume, TEST_SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertTrue(evacuator.evacuate(context, tr, firstEvacMetadata, entry.versionstamp()));
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] secondEncoded = readNewEncodedMetadata(volume, metadata.prefix(), entry.versionstamp(), tr);
            EntryMetadata secondEvacMetadata = EntryMetadata.decode(secondEncoded);
            assertNotEquals(firstEvacMetadata.position(), secondEvacMetadata.position());

            ByteBuffer evacuatedDocument = volume.getByEntryMetadata(secondEvacMetadata);
            assertEquals(originalDocument, evacuatedDocument);

            verifyPrimaryIndex(tr, metadata, objectId, TEST_SHARD_ID, secondEncoded);
        }
    }

    @Test
    void shouldReturnCorrectResultsFromQueryAfterEvacuation() throws IOException {
        // Behavior: after evacuating all entries, BUCKET.QUERY still returns the correct documents
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create(
                "age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING
        );
        createIndexThenWaitForReadiness(ageIndex);

        List<byte[]> docs = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Carol\", \"age\": 35}")
        );
        insertDocumentsAndGetObjectIds(docs);

        Volume volume = getShard(TEST_SHARD_ID).volume();
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        List<VolumeEntry> entries;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            entries = scanVolumeEntries(volume, metadata.prefix(), tr);
            assertEquals(3, entries.size());
        }

        BucketEntryEvacuator evacuator = new BucketEntryEvacuator(volume, TEST_SHARD_ID);
        for (VolumeEntry entry : entries) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                evacuator.evacuate(context, tr, entry.entryMetadata(), entry.versionstamp());
                tr.commit().join();
            }
        }

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Query all documents with age >= 25
        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"age\": {\"$gte\": 25}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        List<BsonDocument> allEntries = extractEntries(msg);
        assertEquals(3, allEntries.size());

        // Query for a specific document
        buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{\"age\": 30}").encode(buf);
        msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        List<BsonDocument> singleEntry = extractEntries(msg);
        assertEquals(1, singleEntry.size());
        assertEquals("Bob", singleEntry.getFirst().getString("name").getValue());
    }

    private record VolumeEntry(Versionstamp versionstamp, EntryMetadata entryMetadata) {
    }
}
