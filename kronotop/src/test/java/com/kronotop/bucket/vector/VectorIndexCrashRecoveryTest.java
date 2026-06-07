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

package com.kronotop.bucket.vector;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.index.*;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.VolumeTestUtil;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class VectorIndexCrashRecoveryTest extends BaseStandaloneInstanceTest {
    private static final String SELECTOR = "embedding";
    private static final int DIMENSIONS = 3;
    private static final int SHARD_ID = 1;
    private static final float[] TEST_VECTOR_1 = {0.1f, 0.2f, 0.3f};
    private static final float[] TEST_VECTOR_2 = {0.4f, 0.5f, 0.6f};
    private static final float[] TEST_VECTOR_3 = {0.7f, 0.8f, 0.9f};

    private ExecutorService executor;

    @BeforeEach
    void setUpExecutor() {
        executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @AfterEach
    void tearDownExecutor() {
        executor.close();
    }

    private EntryMetadata newEntryMetadata() {
        return VolumeTestUtil.generateEntryMetadata(1, 1, 0, 1, "test");
    }

    private byte[] encodedIndexEntry() {
        return new IndexEntry(SHARD_ID, newEntryMetadata().encode()).encode();
    }

    private VectorIndex createVectorIndex() {
        createBucket(TEST_BUCKET);
        String name = VectorIndexNameGenerator.generate(SELECTOR, DIMENSIONS, DistanceFunction.COSINE);
        VectorIndexDefinition definition = VectorIndexDefinition.create(name, SELECTOR, DIMENSIONS, DistanceFunction.COSINE, IndexStatus.WAITING);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            VectorIndexUtil.create(tx, getBucketMetadata(TEST_BUCKET), definition);
            tr.commit().join();
        }
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        return metadata.vectorIndexes().getIndexBySelector(SELECTOR, IndexSelectionPolicy.ALL);
    }

    @Test
    void shouldRecoverInsertedEntries() throws IOException {
        // Behavior: Mutation log INSERT entries are replayed into a fresh on-heap index during recovery.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid1 = new ObjectId();
        ObjectId oid2 = new ObjectId();
        byte[] encoded = encodedIndexEntry();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setMutationLog(
                    tr,
                    vectorIndex.subspace(),
                    MutationLogMarker.INSERT,
                    oid1.toByteArray(),
                    encoded,
                    TEST_VECTOR_1,
                    0
            );
            VectorIndexMaintainer.setMutationLog(
                    tr,
                    vectorIndex.subspace(),
                    MutationLogMarker.INSERT,
                    oid2.toByteArray(),
                    encoded,
                    TEST_VECTOR_2,
                    1
            );
            tr.commit().join();
        }

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        OnHeapVectorGraphIndex recovered = VectorIndexCrashRecovery.recover(
                context.getFoundationDB(), vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 0, 6
        );

        assertNotNull(recovered);
        assertEquals(2, recovered.size());
        assertTrue(recovered.getMetadata().findOrdinal(oid1) >= 0);
        assertTrue(recovered.getMetadata().findOrdinal(oid2) >= 0);
        recovered.close();
    }

    @Test
    void shouldRecoverOnlyEntriesAfterLastFlush() throws IOException {
        // Behavior: Only mutation log entries after the max on-disk versionstamp are replayed.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid1 = new ObjectId();
        ObjectId oid2 = new ObjectId();
        byte[] encoded = encodedIndexEntry();

        // Write the first entry to the mutation log and commit
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setMutationLog(
                    tr,
                    vectorIndex.subspace(),
                    MutationLogMarker.INSERT,
                    oid1.toByteArray(),
                    encoded,
                    TEST_VECTOR_1,
                    0
            );
            tr.commit().join();
        }

        // Build an on-heap index with that entry, flush to disk
        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        OnHeapVectorGraphIndex onHeap = new OnHeapVectorGraphIndex(DIMENSIONS, VectorSimilarityFunction.COSINE);
        group.addOnHeap(onHeap);

        onHeap.addGraphNode(oid1, SHARD_ID, newEntryMetadata(), TEST_VECTOR_1, executor).join();
        // Use a versionstamp that matches what was written in the mutation log
        // Read the actual versionstamp from the mutation log
        Versionstamp flushVersionstamp;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            var prefix = vectorIndex.subspace().pack(
                    com.apple.foundationdb.tuple.Tuple.from(IndexSubspaceMagic.MUTATION_LOG.getValue()));
            var begin = com.apple.foundationdb.KeySelector.firstGreaterOrEqual(prefix);
            var end = com.apple.foundationdb.KeySelector.firstGreaterOrEqual(
                    com.apple.foundationdb.tuple.ByteArrayUtil.strinc(prefix));
            var entries = tr.getRange(begin, end).asList().join();
            flushVersionstamp = vectorIndex.subspace().unpack(entries.getFirst().getKey()).getVersionstamp(1);
            onHeap.advanceVersionstamp(flushVersionstamp);
        }
        BucketService service = context.getService(BucketService.NAME);
        group.flush(service.getBucketDataDir());

        // Load on-disk indexes
        List<OnDiskVectorGraphIndex> onDiskIndexes = VectorGraphIndexGroup.openOnDiskIndexes(
                service.getBucketDataDir(),
                metadata.uuid(),
                vectorIndex.definition().id(),
                VectorSimilarityFunction.COSINE
        );
        assertFalse(onDiskIndexes.isEmpty());
        for (OnDiskVectorGraphIndex idx : onDiskIndexes) {
            group.addOnDisk(idx);
        }

        // Write a second entry to the mutation log (after the flush versionstamp)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setMutationLog(
                    tr,
                    vectorIndex.subspace(),
                    MutationLogMarker.INSERT,
                    oid2.toByteArray(),
                    encoded,
                    TEST_VECTOR_2,
                    0
            );
            tr.commit().join();
        }

        // Recover — should only get oid2
        OnHeapVectorGraphIndex recovered = VectorIndexCrashRecovery.recover(
                context.getFoundationDB(), vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 0, 6
        );

        assertNotNull(recovered);
        assertEquals(1, recovered.size());
        assertEquals(-1, recovered.getMetadata().findOrdinal(oid1));
        assertTrue(recovered.getMetadata().findOrdinal(oid2) >= 0);

        recovered.close();
        group.closeAll();
    }

    @Test
    void shouldHandleDeleteDuringRecovery() throws IOException {
        // Behavior: A DELETE mutation after an INSERT for the same ObjectId marks the node as deleted during recovery.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid = new ObjectId();
        byte[] encoded = encodedIndexEntry();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setMutationLog(
                    tr,
                    vectorIndex.subspace(),
                    MutationLogMarker.INSERT,
                    oid.toByteArray(),
                    encoded,
                    TEST_VECTOR_1,
                    0
            );
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.deleteMutationLog(tr, vectorIndex.subspace(), oid.toByteArray(), 0);
            tr.commit().join();
        }

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        OnHeapVectorGraphIndex recovered = VectorIndexCrashRecovery.recover(
                context.getFoundationDB(), vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 0, 6
        );

        assertNotNull(recovered);
        // The node was inserted then deleted — its mapping should be removed from metadata
        int ordinal = recovered.getMetadata().findOrdinal(oid);
        assertEquals(-1, ordinal);
        // Search for a vector identical to TEST_VECTOR_1 — deleted node should not appear in results
        var result = recovered.search(TEST_VECTOR_1, 1);
        assertEquals(0, result.getNodes().length);
        recovered.close();
    }

    @Test
    void shouldReturnNullWhenNoMutationLogEntries() {
        // Behavior: Recovery returns null when the mutation log is empty.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        OnHeapVectorGraphIndex recovered = VectorIndexCrashRecovery.recover(
                context.getFoundationDB(), vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 0, 6
        );

        assertNull(recovered);
    }

    @Test
    void shouldRecoverUpdateEntries() throws IOException {
        // Behavior: An UPDATE mutation replays the real UpdateExecutor sequence (INSERT, DELETE, UPDATE)
        // and produces a single active node with the updated vector.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid = new ObjectId();
        byte[] encoded = encodedIndexEntry();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setMutationLog(
                    tr,
                    vectorIndex.subspace(),
                    MutationLogMarker.INSERT,
                    oid.toByteArray(),
                    encoded,
                    TEST_VECTOR_1,
                    0
            );
            tr.commit().join();
        }

        // UpdateExecutor always writes a DELETE before the UPDATE
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.deleteMutationLog(
                    tr,
                    vectorIndex.subspace(),
                    oid.toByteArray(),
                    0
            );
            VectorIndexMaintainer.setMutationLog(
                    tr,
                    vectorIndex.subspace(),
                    MutationLogMarker.UPDATE,
                    oid.toByteArray(),
                    encoded,
                    TEST_VECTOR_3,
                    1
            );
            tr.commit().join();
        }

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        OnHeapVectorGraphIndex recovered = VectorIndexCrashRecovery.recover(
                context.getFoundationDB(), vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 0, 6
        );

        assertNotNull(recovered);
        // INSERT adds ordinal 0, DELETE marks it deleted; UPDATE adds ordinal 1
        assertEquals(2, recovered.size());
        // Only the updated node is active in metadata
        int activeOrdinal = recovered.getMetadata().findOrdinal(oid);
        assertTrue(activeOrdinal > 0);
        recovered.close();
    }

    @Test
    void shouldRecoverWhenNoOnDiskFilesExist() throws IOException {
        // Behavior: When no on-disk files exist, recovery replays the entire mutation log.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid1 = new ObjectId();
        ObjectId oid2 = new ObjectId();
        ObjectId oid3 = new ObjectId();
        byte[] encoded = encodedIndexEntry();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setMutationLog(
                    tr,
                    vectorIndex.subspace(),
                    MutationLogMarker.INSERT,
                    oid1.toByteArray(),
                    encoded,
                    TEST_VECTOR_1,
                    0
            );
            VectorIndexMaintainer.setMutationLog(
                    tr,
                    vectorIndex.subspace(),
                    MutationLogMarker.INSERT,
                    oid2.toByteArray(),
                    encoded,
                    TEST_VECTOR_2,
                    1
            );
            VectorIndexMaintainer.setMutationLog(
                    tr,
                    vectorIndex.subspace(),
                    MutationLogMarker.INSERT,
                    oid3.toByteArray(),
                    encoded,
                    TEST_VECTOR_3,
                    2
            );
            tr.commit().join();
        }

        // Empty group — no on-disk files
        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        OnHeapVectorGraphIndex recovered = VectorIndexCrashRecovery.recover(
                context.getFoundationDB(), vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 0, 6
        );

        assertNotNull(recovered);
        assertEquals(3, recovered.size());
        assertTrue(recovered.getMetadata().findOrdinal(oid1) >= 0);
        assertTrue(recovered.getMetadata().findOrdinal(oid2) >= 0);
        assertTrue(recovered.getMetadata().findOrdinal(oid3) >= 0);
        recovered.close();
    }

    @Test
    void shouldRecoverWithPQTrainingTriggered() throws IOException {
        // Behavior: Recovery with enough mutation log entries triggers PQ training on the recovered index.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        byte[] encoded = encodedIndexEntry();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < 5; i++) {
                float[] vector = {0.1f + i * 0.15f, 0.2f + i * 0.1f, 0.3f + i * 0.05f};
                VectorIndexMaintainer.setMutationLog(tr, vectorIndex.subspace(), MutationLogMarker.INSERT,
                        new ObjectId().toByteArray(), encoded, vector, i);
            }
            tr.commit().join();
        }

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        OnHeapVectorGraphIndex recovered = VectorIndexCrashRecovery.recover(
                context.getFoundationDB(), vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 5, 1
        );

        assertNotNull(recovered);
        assertEquals(5, recovered.size());
        assertTrue(recovered.isPqTrained());

        var result = recovered.search(TEST_VECTOR_1, 1);
        assertEquals(1, result.getNodes().length);
        recovered.close();
    }

    @Test
    void shouldRecoverWithPQNotTriggeredBelowThreshold() throws IOException {
        // Behavior: Recovery with fewer mutation log entries than the PQ threshold does not trigger PQ training.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid1 = new ObjectId();
        ObjectId oid2 = new ObjectId();
        byte[] encoded = encodedIndexEntry();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setMutationLog(tr, vectorIndex.subspace(), MutationLogMarker.INSERT,
                    oid1.toByteArray(), encoded, TEST_VECTOR_1, 0);
            VectorIndexMaintainer.setMutationLog(tr, vectorIndex.subspace(), MutationLogMarker.INSERT,
                    oid2.toByteArray(), encoded, TEST_VECTOR_2, 1);
            tr.commit().join();
        }

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        OnHeapVectorGraphIndex recovered = VectorIndexCrashRecovery.recover(
                context.getFoundationDB(), vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 5, 1
        );

        assertNotNull(recovered);
        assertEquals(2, recovered.size());
        assertFalse(recovered.isPqTrained());
        recovered.close();
    }

    @Test
    void shouldUpdateOnDiskAliveBytesDuringRecovery() throws IOException {
        // Behavior: A DELETE mutation log entry replayed during crash recovery flips the alive byte
        // to 0 on the on-disk metadata for the corresponding ObjectId.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid1 = new ObjectId();
        ObjectId oid2 = new ObjectId();
        ObjectId oid3 = new ObjectId();
        byte[] encoded = encodedIndexEntry();

        // Write INSERT entries for all three ObjectIds
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setMutationLog(
                    tr,
                    vectorIndex.subspace(),
                    MutationLogMarker.INSERT,
                    oid1.toByteArray(),
                    encoded,
                    TEST_VECTOR_1,
                    0
            );
            VectorIndexMaintainer.setMutationLog(
                    tr,
                    vectorIndex.subspace(),
                    MutationLogMarker.INSERT,
                    oid2.toByteArray(),
                    encoded,
                    TEST_VECTOR_2,
                    1
            );
            VectorIndexMaintainer.setMutationLog(
                    tr,
                    vectorIndex.subspace(),
                    MutationLogMarker.INSERT,
                    oid3.toByteArray(),
                    encoded,
                    TEST_VECTOR_3,
                    2
            );
            tr.commit().join();
        }

        // Build an on-heap index with those entries and flush to disk
        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        OnHeapVectorGraphIndex onHeap = new OnHeapVectorGraphIndex(DIMENSIONS, VectorSimilarityFunction.COSINE);
        group.addOnHeap(onHeap);

        onHeap.addGraphNode(oid1, SHARD_ID, newEntryMetadata(), TEST_VECTOR_1, executor).join();
        onHeap.addGraphNode(oid2, SHARD_ID, newEntryMetadata(), TEST_VECTOR_2, executor).join();
        onHeap.addGraphNode(oid3, SHARD_ID, newEntryMetadata(), TEST_VECTOR_3, executor).join();

        // Read the latest versionstamp from the mutation log
        Versionstamp flushVersionstamp;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            var prefix = vectorIndex.subspace().pack(
                    com.apple.foundationdb.tuple.Tuple.from(IndexSubspaceMagic.MUTATION_LOG.getValue()));
            var begin = com.apple.foundationdb.KeySelector.firstGreaterOrEqual(prefix);
            var end = com.apple.foundationdb.KeySelector.firstGreaterOrEqual(
                    com.apple.foundationdb.tuple.ByteArrayUtil.strinc(prefix));
            var entries = tr.getRange(begin, end).asList().join();
            flushVersionstamp = vectorIndex.subspace().unpack(entries.getLast().getKey()).getVersionstamp(1);
            onHeap.advanceVersionstamp(flushVersionstamp);
        }
        BucketService service = context.getService(BucketService.NAME);
        group.flush(service.getBucketDataDir());

        // Load on-disk indexes
        List<OnDiskVectorGraphIndex> onDiskIndexes = VectorGraphIndexGroup.openOnDiskIndexes(
                service.getBucketDataDir(),
                metadata.uuid(),
                vectorIndex.definition().id(),
                VectorSimilarityFunction.COSINE
        );
        assertFalse(onDiskIndexes.isEmpty());
        for (OnDiskVectorGraphIndex idx : onDiskIndexes) {
            group.addOnDisk(idx);
        }

        // Write a DELETE mutation log entry for oid2 (after the flush versionstamp)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.deleteMutationLog(tr, vectorIndex.subspace(), oid2.toByteArray(), 0);
            tr.commit().join();
        }

        // Run crash recovery
        OnHeapVectorGraphIndex recovered = VectorIndexCrashRecovery.recover(
                context.getFoundationDB(), vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 0, 6
        );

        // The recovered on-heap index should have the DELETE replayed
        assertNotNull(recovered);

        // On-disk metadata should now return null for the deleted ObjectId
        OnDiskVectorGraphIndex onDisk = group.getOnDiskIndexes().getFirst();
        assertNull(onDisk.getMetadata().findDocumentLocation(onDisk.getMetadata().findOrdinal(oid2)));

        // Other ObjectIds should still be accessible on disk
        int ordinal1 = onDisk.getMetadata().findOrdinal(oid1);
        int ordinal3 = onDisk.getMetadata().findOrdinal(oid3);
        assertTrue(ordinal1 >= 0);
        assertTrue(ordinal3 >= 0);
        assertNotNull(onDisk.getMetadata().findDocumentLocation(ordinal1));
        assertNotNull(onDisk.getMetadata().findDocumentLocation(ordinal3));

        recovered.close();
        group.closeAll();
    }

    @Test
    void shouldRecoverInsertMutationLogViaBootstrap() {
        // Behavior: bootstrapVectorGroup recovers INSERT mutation log entries written before a crash,
        // making the vectors searchable without VectorNodeAddHook ever running.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        BucketService service = context.getService(BucketService.NAME);

        ObjectId oid1 = new ObjectId();
        ObjectId oid2 = new ObjectId();
        byte[] encoded = encodedIndexEntry();

        // Simulate: INSERT transaction committed (mutation log written), but crash before hook ran
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setMutationLog(
                    tr,
                    vectorIndex.subspace(),
                    MutationLogMarker.INSERT,
                    oid1.toByteArray(),
                    encoded,
                    TEST_VECTOR_1,
                    0
            );
            VectorIndexMaintainer.setMutationLog(
                    tr,
                    vectorIndex.subspace(),
                    MutationLogMarker.INSERT,
                    oid2.toByteArray(),
                    encoded,
                    TEST_VECTOR_2,
                    1
            );
            tr.commit().join();
        }

        // Bootstrap detects mutation log entries → slow-path (not immediately ready)
        VectorGraphIndexGroup group = service.bootstrapVectorGroup(metadata, vectorIndex);
        assertFalse(group.isReady());

        group.awaitReady();

        assertTrue(group.isReady());
        assertFalse(group.getOnHeapIndexes().isEmpty());

        OnHeapVectorGraphIndex recovered = group.getOnHeapIndexes().getFirst();
        assertEquals(2, recovered.size());
        assertTrue(recovered.getMetadata().findOrdinal(oid1) >= 0);
        assertTrue(recovered.getMetadata().findOrdinal(oid2) >= 0);

        // Verify recovered vectors are searchable
        List<MergedNodeScore> results = group.searchAll(TEST_VECTOR_1, 2, 0.0f, 1.0f);
        assertEquals(2, results.size());
        assertTrue(results.getFirst().score() > 0);

        group.closeAll();
    }

    @Test
    void shouldRecoverUpsertMutationLogViaBootstrap() {
        // Behavior: bootstrapVectorGroup recovers an INSERT mutation log entry written by an upsert
        // operation that crashed before VectorNodeAddHook ran.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        BucketService service = context.getService(BucketService.NAME);

        ObjectId oid = new ObjectId();
        byte[] encoded = encodedIndexEntry();

        // Simulate: upsert committed (writes INSERT marker to mutation log), crash before hook
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setMutationLog(
                    tr,
                    vectorIndex.subspace(),
                    MutationLogMarker.INSERT,
                    oid.toByteArray(),
                    encoded,
                    TEST_VECTOR_3,
                    0
            );
            tr.commit().join();
        }

        VectorGraphIndexGroup group = service.bootstrapVectorGroup(metadata, vectorIndex);
        assertFalse(group.isReady());

        group.awaitReady();

        assertTrue(group.isReady());
        OnHeapVectorGraphIndex recovered = group.getOnHeapIndexes().getFirst();
        assertEquals(1, recovered.size());
        assertTrue(recovered.getMetadata().findOrdinal(oid) >= 0);

        // Verify recovered vector is searchable with correct similarity
        List<MergedNodeScore> results = group.searchAll(TEST_VECTOR_3, 1, 0.0f, 1.0f);
        assertEquals(1, results.size());
        assertEquals(1.0f, results.getFirst().score(), 0.001f);

        group.closeAll();
    }

    private List<KeyValue> getMutationLogEntries(VectorIndex vectorIndex) {
        byte[] prefix = vectorIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.MUTATION_LOG.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return tr.getRange(begin, end).asList().join();
        }
    }

    @Test
    void shouldTruncateMutationLogAfterFlushSingle(@TempDir Path tempDir) throws IOException {
        // Behavior: flushSingle clears all mutation log entries up to the flushed index's latest versionstamp.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid1 = new ObjectId();
        ObjectId oid2 = new ObjectId();
        ObjectId oid3 = new ObjectId();
        byte[] encoded = encodedIndexEntry();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setMutationLog(tr, vectorIndex.subspace(), MutationLogMarker.INSERT,
                    oid1.toByteArray(), encoded, TEST_VECTOR_1, 0);
            VectorIndexMaintainer.setMutationLog(tr, vectorIndex.subspace(), MutationLogMarker.INSERT,
                    oid2.toByteArray(), encoded, TEST_VECTOR_2, 1);
            VectorIndexMaintainer.setMutationLog(tr, vectorIndex.subspace(), MutationLogMarker.INSERT,
                    oid3.toByteArray(), encoded, TEST_VECTOR_3, 2);
            tr.commit().join();
        }

        assertEquals(3, getMutationLogEntries(vectorIndex).size());

        // Read versionstamps from FDB
        List<KeyValue> entries = getMutationLogEntries(vectorIndex);
        Versionstamp vs1 = vectorIndex.subspace().unpack(entries.get(0).getKey()).getVersionstamp(1);
        Versionstamp vs2 = vectorIndex.subspace().unpack(entries.get(1).getKey()).getVersionstamp(1);
        Versionstamp vs3 = vectorIndex.subspace().unpack(entries.get(2).getKey()).getVersionstamp(1);

        OnHeapVectorGraphIndex onHeap = new OnHeapVectorGraphIndex(DIMENSIONS, VectorSimilarityFunction.COSINE);
        onHeap.addGraphNode(oid1, SHARD_ID, newEntryMetadata(), TEST_VECTOR_1, executor).join();
        onHeap.advanceVersionstamp(vs1);
        onHeap.addGraphNode(oid2, SHARD_ID, newEntryMetadata(), TEST_VECTOR_2, executor).join();
        onHeap.advanceVersionstamp(vs2);
        onHeap.addGraphNode(oid3, SHARD_ID, newEntryMetadata(), TEST_VECTOR_3, executor).join();
        onHeap.advanceVersionstamp(vs3);

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        group.addOnHeap(onHeap);

        assertFalse(getMutationLogEntries(vectorIndex).isEmpty());
        group.flushSingle(tempDir, onHeap);
        assertEquals(0, getMutationLogEntries(vectorIndex).size());

        group.closeAll();
    }

    @Test
    void shouldTruncateMutationLogUpToFlushedVersionstampOnly(@TempDir Path tempDir) throws IOException {
        // Behavior: flushSingle only clears mutation log entries up to the flushed index's versionstamp,
        // preserving entries written after it.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid1 = new ObjectId();
        ObjectId oid2 = new ObjectId();
        ObjectId oid3 = new ObjectId();
        byte[] encoded = encodedIndexEntry();

        // Write first batch
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setMutationLog(tr, vectorIndex.subspace(), MutationLogMarker.INSERT,
                    oid1.toByteArray(), encoded, TEST_VECTOR_1, 0);
            VectorIndexMaintainer.setMutationLog(tr, vectorIndex.subspace(), MutationLogMarker.INSERT,
                    oid2.toByteArray(), encoded, TEST_VECTOR_2, 1);
            VectorIndexMaintainer.setMutationLog(tr, vectorIndex.subspace(), MutationLogMarker.INSERT,
                    oid3.toByteArray(), encoded, TEST_VECTOR_3, 2);
            tr.commit().join();
        }

        // Build on-heap with only oid1 and oid2
        List<KeyValue> allEntries = getMutationLogEntries(vectorIndex);
        Versionstamp vs1 = vectorIndex.subspace().unpack(allEntries.get(0).getKey()).getVersionstamp(1);
        Versionstamp vs2 = vectorIndex.subspace().unpack(allEntries.get(1).getKey()).getVersionstamp(1);

        OnHeapVectorGraphIndex onHeap = new OnHeapVectorGraphIndex(DIMENSIONS, VectorSimilarityFunction.COSINE);
        onHeap.addGraphNode(oid1, SHARD_ID, newEntryMetadata(), TEST_VECTOR_1, executor).join();
        onHeap.advanceVersionstamp(vs1);
        onHeap.addGraphNode(oid2, SHARD_ID, newEntryMetadata(), TEST_VECTOR_2, executor).join();
        onHeap.advanceVersionstamp(vs2);

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        group.addOnHeap(onHeap);
        group.flushSingle(tempDir, onHeap);

        // oid3's mutation log entry should survive
        List<KeyValue> remaining = getMutationLogEntries(vectorIndex);
        assertEquals(1, remaining.size());

        MutationLogValue decoded = MutationLogValue.decode(remaining.getFirst().getValue());
        assertArrayEquals(oid3.toByteArray(), decoded.objectIdBytes());

        group.closeAll();
    }

    @Test
    void shouldTruncateMutationLogAfterBatchFlush(@TempDir Path tempDir) throws IOException {
        // Behavior: flush (batch) clears mutation log entries up to the max versionstamp across all flushed indexes.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid1 = new ObjectId();
        ObjectId oid2 = new ObjectId();
        byte[] encoded = encodedIndexEntry();

        // Write two entries in separate transactions to get distinct versionstamps
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setMutationLog(tr, vectorIndex.subspace(), MutationLogMarker.INSERT,
                    oid1.toByteArray(), encoded, TEST_VECTOR_1, 0);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setMutationLog(tr, vectorIndex.subspace(), MutationLogMarker.INSERT,
                    oid2.toByteArray(), encoded, TEST_VECTOR_2, 0);
            tr.commit().join();
        }

        List<KeyValue> allEntries = getMutationLogEntries(vectorIndex);
        Versionstamp vs1 = vectorIndex.subspace().unpack(allEntries.get(0).getKey()).getVersionstamp(1);
        Versionstamp vs2 = vectorIndex.subspace().unpack(allEntries.get(1).getKey()).getVersionstamp(1);

        // Build two on-heap indexes, one per entry
        OnHeapVectorGraphIndex heap1 = new OnHeapVectorGraphIndex(DIMENSIONS, VectorSimilarityFunction.COSINE);
        heap1.addGraphNode(oid1, SHARD_ID, newEntryMetadata(), TEST_VECTOR_1, executor).join();
        heap1.advanceVersionstamp(vs1);

        OnHeapVectorGraphIndex heap2 = new OnHeapVectorGraphIndex(DIMENSIONS, VectorSimilarityFunction.COSINE);
        heap2.addGraphNode(oid2, SHARD_ID, newEntryMetadata(), TEST_VECTOR_2, executor).join();
        heap2.advanceVersionstamp(vs2);

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        group.addOnHeap(heap1);
        group.addOnHeap(heap2);

        assertFalse(getMutationLogEntries(vectorIndex).isEmpty());
        group.flush(tempDir);
        assertEquals(0, getMutationLogEntries(vectorIndex).size());

        group.closeAll();
    }

    @Test
    void shouldTruncateDeleteMutationLogWhenVersionstampAdvanced(@TempDir Path tempDir) throws IOException {
        // Behavior: A DELETE mutation log entry is truncated during flush when the active on-heap's
        // versionstamp has been advanced past the DELETE entry (simulating VectorNodeDeleteHook behavior).
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid1 = new ObjectId();
        ObjectId oid2 = new ObjectId();
        byte[] encoded = encodedIndexEntry();

        // Write INSERT entries for two documents
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setMutationLog(tr, vectorIndex.subspace(), MutationLogMarker.INSERT,
                    oid1.toByteArray(), encoded, TEST_VECTOR_1, 0);
            VectorIndexMaintainer.setMutationLog(tr, vectorIndex.subspace(), MutationLogMarker.INSERT,
                    oid2.toByteArray(), encoded, TEST_VECTOR_2, 1);
            tr.commit().join();
        }

        // Build on-heap with both entries and flush to disk
        List<KeyValue> insertEntries = getMutationLogEntries(vectorIndex);
        Versionstamp vs1 = vectorIndex.subspace().unpack(insertEntries.get(0).getKey()).getVersionstamp(1);
        Versionstamp vs2 = vectorIndex.subspace().unpack(insertEntries.get(1).getKey()).getVersionstamp(1);

        OnHeapVectorGraphIndex firstHeap = new OnHeapVectorGraphIndex(DIMENSIONS, VectorSimilarityFunction.COSINE);
        firstHeap.addGraphNode(oid1, SHARD_ID, newEntryMetadata(), TEST_VECTOR_1, executor).join();
        firstHeap.advanceVersionstamp(vs1);
        firstHeap.addGraphNode(oid2, SHARD_ID, newEntryMetadata(), TEST_VECTOR_2, executor).join();
        firstHeap.advanceVersionstamp(vs2);

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        group.addOnHeap(firstHeap);
        group.flushSingle(tempDir, firstHeap);

        // INSERT entries are now truncated
        assertEquals(0, getMutationLogEntries(vectorIndex).size());

        // Write a DELETE mutation log entry (simulating DeleteExecutor)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.deleteMutationLog(tr, vectorIndex.subspace(), oid1.toByteArray(), 0);
            tr.commit().join();
        }

        assertEquals(1, getMutationLogEntries(vectorIndex).size());

        // Simulate VectorNodeDeleteHook: advance the active on-heap's versionstamp with the DELETE's versionstamp
        List<KeyValue> deleteEntries = getMutationLogEntries(vectorIndex);
        Versionstamp deleteVs = vectorIndex.subspace().unpack(deleteEntries.getFirst().getKey()).getVersionstamp(1);

        OnHeapVectorGraphIndex activeHeap = new OnHeapVectorGraphIndex(DIMENSIONS, VectorSimilarityFunction.COSINE);
        activeHeap.advanceVersionstamp(deleteVs);

        // Add a node so the on-heap is non-empty and eligible for flush
        ObjectId oid3 = new ObjectId();
        activeHeap.addGraphNode(oid3, SHARD_ID, newEntryMetadata(), TEST_VECTOR_3, executor).join();

        group.addOnHeap(activeHeap);
        group.flushSingle(tempDir, activeHeap);

        // The DELETE mutation log entry should now be truncated
        assertEquals(0, getMutationLogEntries(vectorIndex).size());

        group.closeAll();
    }
}
