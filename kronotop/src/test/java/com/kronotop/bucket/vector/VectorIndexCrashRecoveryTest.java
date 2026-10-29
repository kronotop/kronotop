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

import com.apple.foundationdb.Transaction;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class VectorIndexCrashRecoveryTest extends BaseStandaloneInstanceTest {
    private static final String SELECTOR = "embedding";
    private static final int DIMENSIONS = 3;
    private static final int SHARD_ID = 1;
    private static final byte[] GENERATION_ID = new ObjectId().toByteArray();
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
        try (Transaction tr = context.getTransactionManager().create()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            VectorIndexUtil.create(tx, getBucketMetadata(TEST_BUCKET), definition);
            tr.commit().join();
        }
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        return metadata.vectorIndexes().getIndexBySelector(SELECTOR, IndexSelectionPolicy.ALL);
    }

    @Test
    void shouldRecoverInsertedEntries() {
        // Behavior: Mutation log INSERT entries are replayed into a fresh on-heap index during recovery.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid1 = new ObjectId();
        ObjectId oid2 = new ObjectId();
        byte[] encoded = encodedIndexEntry();

        try (Transaction tr = context.getTransactionManager().create()) {
            VectorIndexMaintainer.appendMutation(
                    tr,
                    vectorIndex.subspace(), GENERATION_ID,
                    MutationLogKind.INSERT, SHARD_ID,
                    oid1.toByteArray(),
                    encoded,
                    TEST_VECTOR_1,
                    0
            );
            VectorIndexMaintainer.appendMutation(
                    tr,
                    vectorIndex.subspace(), GENERATION_ID,
                    MutationLogKind.INSERT, SHARD_ID,
                    oid2.toByteArray(),
                    encoded,
                    TEST_VECTOR_2,
                    1
            );
            tr.commit().join();
        }

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        VectorIndexCrashRecovery.recover(
                context, vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 0, 6
        );

        OnHeapVectorGraphIndex recovered = group.getOnHeapIndexes().getLast();
        assertEquals(2, recovered.size());
        assertNotNull(recovered.getMetadata().findNodeRef(oid1));
        assertNotNull(recovered.getMetadata().findNodeRef(oid2));
        group.closeAll();
    }

    @Test
    void shouldHandleDeleteDuringRecovery() {
        // Behavior: A DELETE mutation after an INSERT for the same ObjectId marks the node as deleted during recovery.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid = new ObjectId();
        byte[] encoded = encodedIndexEntry();

        try (Transaction tr = context.getTransactionManager().create()) {
            VectorIndexMaintainer.appendMutation(
                    tr,
                    vectorIndex.subspace(), GENERATION_ID,
                    MutationLogKind.INSERT, SHARD_ID,
                    oid.toByteArray(),
                    encoded,
                    TEST_VECTOR_1,
                    0
            );
            tr.commit().join();
        }

        try (Transaction tr = context.getTransactionManager().create()) {
            VectorIndexMaintainer.appendDeleteMutation(tr, vectorIndex.subspace(), GENERATION_ID, SHARD_ID, oid.toByteArray(), 0);
            tr.commit().join();
        }

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        VectorIndexCrashRecovery.recover(
                context, vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 0, 6
        );

        OnHeapVectorGraphIndex recovered = group.getOnHeapIndexes().getLast();
        // The node was inserted then deleted, so its mapping should be removed from metadata
        assertNull(recovered.getMetadata().findNodeRef(oid));
        // Search for a vector identical to TEST_VECTOR_1. The deleted node should not appear in results.
        var result = recovered.search(TEST_VECTOR_1, 1);
        assertEquals(0, result.getNodes().length);
        group.closeAll();
    }

    @Test
    void shouldRecoverNothingWhenNoMutationLogEntries() {
        // Behavior: Recovery on an empty mutation log completes and creates no on-heap index.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        VectorIndexCrashRecovery.recover(
                context, vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 0, 6
        );

        assertTrue(group.getOnHeapIndexes().isEmpty());
        group.closeAll();
    }

    @Test
    void shouldRecoverUpdateEntries() {
        // Behavior: An UPDATE mutation replays the real UpdateExecutor sequence (INSERT, DELETE, UPDATE)
        // and produces a single active node with the updated vector.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid = new ObjectId();
        byte[] encoded = encodedIndexEntry();

        try (Transaction tr = context.getTransactionManager().create()) {
            VectorIndexMaintainer.appendMutation(
                    tr,
                    vectorIndex.subspace(), GENERATION_ID,
                    MutationLogKind.INSERT, SHARD_ID,
                    oid.toByteArray(),
                    encoded,
                    TEST_VECTOR_1,
                    0
            );
            tr.commit().join();
        }

        // UpdateExecutor always writes a DELETE before the UPDATE
        try (Transaction tr = context.getTransactionManager().create()) {
            VectorIndexMaintainer.appendDeleteMutation(
                    tr,
                    vectorIndex.subspace(), GENERATION_ID, SHARD_ID,
                    oid.toByteArray(),
                    0
            );
            VectorIndexMaintainer.appendMutation(
                    tr,
                    vectorIndex.subspace(), GENERATION_ID,
                    MutationLogKind.UPDATE, SHARD_ID,
                    oid.toByteArray(),
                    encoded,
                    TEST_VECTOR_3,
                    1
            );
            tr.commit().join();
        }

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        VectorIndexCrashRecovery.recover(
                context, vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 0, 6
        );

        OnHeapVectorGraphIndex recovered = group.getOnHeapIndexes().getLast();
        // INSERT adds ordinal 0, DELETE marks it deleted; UPDATE adds ordinal 1
        assertEquals(2, recovered.size());
        // Only the updated node is active in metadata
        int activeOrdinal = recovered.getMetadata().findNodeRef(oid).ordinal();
        assertTrue(activeOrdinal > 0);
        group.closeAll();
    }

    @Test
    void shouldRecoverWhenNoOnDiskFilesExist() {
        // Behavior: When no on-disk files exist, recovery replays the entire mutation log.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid1 = new ObjectId();
        ObjectId oid2 = new ObjectId();
        ObjectId oid3 = new ObjectId();
        byte[] encoded = encodedIndexEntry();

        try (Transaction tr = context.getTransactionManager().create()) {
            VectorIndexMaintainer.appendMutation(
                    tr,
                    vectorIndex.subspace(), GENERATION_ID,
                    MutationLogKind.INSERT, SHARD_ID,
                    oid1.toByteArray(),
                    encoded,
                    TEST_VECTOR_1,
                    0
            );
            VectorIndexMaintainer.appendMutation(
                    tr,
                    vectorIndex.subspace(), GENERATION_ID,
                    MutationLogKind.INSERT, SHARD_ID,
                    oid2.toByteArray(),
                    encoded,
                    TEST_VECTOR_2,
                    1
            );
            VectorIndexMaintainer.appendMutation(
                    tr,
                    vectorIndex.subspace(), GENERATION_ID,
                    MutationLogKind.INSERT, SHARD_ID,
                    oid3.toByteArray(),
                    encoded,
                    TEST_VECTOR_3,
                    2
            );
            tr.commit().join();
        }

        // Empty group, no on-disk files
        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        VectorIndexCrashRecovery.recover(
                context, vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 0, 6
        );

        OnHeapVectorGraphIndex recovered = group.getOnHeapIndexes().getLast();
        assertEquals(3, recovered.size());
        assertNotNull(recovered.getMetadata().findNodeRef(oid1));
        assertNotNull(recovered.getMetadata().findNodeRef(oid2));
        assertNotNull(recovered.getMetadata().findNodeRef(oid3));
        group.closeAll();
    }

    @Test
    void shouldRecoverWithPQTrainingTriggered() {
        // Behavior: Recovery with enough mutation log entries triggers PQ training on the recovered index.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        byte[] encoded = encodedIndexEntry();
        try (Transaction tr = context.getTransactionManager().create()) {
            for (int i = 0; i < 5; i++) {
                float[] vector = {0.1f + i * 0.15f, 0.2f + i * 0.1f, 0.3f + i * 0.05f};
                VectorIndexMaintainer.appendMutation(tr, vectorIndex.subspace(), GENERATION_ID, MutationLogKind.INSERT, SHARD_ID,
                        new ObjectId().toByteArray(), encoded, vector, i);
            }
            tr.commit().join();
        }

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        VectorIndexCrashRecovery.recover(
                context, vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 5, 1
        );

        OnHeapVectorGraphIndex recovered = group.getOnHeapIndexes().getLast();
        assertEquals(5, recovered.size());
        assertTrue(recovered.isPqTrained());

        var result = recovered.search(TEST_VECTOR_1, 1);
        assertEquals(1, result.getNodes().length);
        group.closeAll();
    }

    @Test
    void shouldRecoverWithPQNotTriggeredBelowThreshold() {
        // Behavior: Recovery with fewer mutation log entries than the PQ threshold does not trigger PQ training.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid1 = new ObjectId();
        ObjectId oid2 = new ObjectId();
        byte[] encoded = encodedIndexEntry();

        try (Transaction tr = context.getTransactionManager().create()) {
            VectorIndexMaintainer.appendMutation(tr, vectorIndex.subspace(), GENERATION_ID, MutationLogKind.INSERT, SHARD_ID,
                    oid1.toByteArray(), encoded, TEST_VECTOR_1, 0);
            VectorIndexMaintainer.appendMutation(tr, vectorIndex.subspace(), GENERATION_ID, MutationLogKind.INSERT, SHARD_ID,
                    oid2.toByteArray(), encoded, TEST_VECTOR_2, 1);
            tr.commit().join();
        }

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        VectorIndexCrashRecovery.recover(
                context, vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 5, 1
        );

        OnHeapVectorGraphIndex recovered = group.getOnHeapIndexes().getLast();
        assertEquals(2, recovered.size());
        assertFalse(recovered.isPqTrained());
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
        try (Transaction tr = context.getTransactionManager().create()) {
            VectorIndexMaintainer.appendMutation(
                    tr,
                    vectorIndex.subspace(), GENERATION_ID,
                    MutationLogKind.INSERT, SHARD_ID,
                    oid1.toByteArray(),
                    encoded,
                    TEST_VECTOR_1,
                    0
            );
            VectorIndexMaintainer.appendMutation(
                    tr,
                    vectorIndex.subspace(), GENERATION_ID,
                    MutationLogKind.INSERT, SHARD_ID,
                    oid2.toByteArray(),
                    encoded,
                    TEST_VECTOR_2,
                    1
            );
            tr.commit().join();
        }

        // Bootstrap detects mutation log entries -> slow-path (not immediately ready)
        VectorGraphIndexGroup group = service.bootstrapVectorGroup(metadata, vectorIndex);
        assertFalse(group.isReady());

        group.awaitReady();

        assertTrue(group.isReady());
        assertFalse(group.getOnHeapIndexes().isEmpty());

        OnHeapVectorGraphIndex recovered = group.getOnHeapIndexes().getFirst();
        assertEquals(2, recovered.size());
        assertNotNull(recovered.getMetadata().findNodeRef(oid1));
        assertNotNull(recovered.getMetadata().findNodeRef(oid2));

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

        // Simulate: upsert committed (writes INSERT kind to mutation log), crash before hook
        try (Transaction tr = context.getTransactionManager().create()) {
            VectorIndexMaintainer.appendMutation(
                    tr,
                    vectorIndex.subspace(), GENERATION_ID,
                    MutationLogKind.INSERT, SHARD_ID,
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
        assertNotNull(recovered.getMetadata().findNodeRef(oid));

        // Verify recovered vector is searchable with correct similarity
        List<MergedNodeScore> results = group.searchAll(TEST_VECTOR_3, 1, 0.0f, 1.0f);
        assertEquals(1, results.size());
        assertEquals(1.0f, results.getFirst().score(), 0.001f);

        group.closeAll();
    }

    @Test
    void shouldRecoverLogLargerThanOnePage() {
        // Behavior: A mutation log larger than one page is replayed completely into a single on-heap index
        // across several transactions.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        int total = VectorIndexCrashRecovery.PAGE_SIZE + 1;
        List<ObjectId> oids = new ArrayList<>(total);
        byte[] encoded = encodedIndexEntry();
        try (Transaction tr = context.getTransactionManager().create()) {
            for (int userVersion = 0; userVersion < total; userVersion++) {
                ObjectId oid = new ObjectId();
                oids.add(oid);
                VectorIndexMaintainer.appendMutation(tr, vectorIndex.subspace(), GENERATION_ID, MutationLogKind.INSERT, SHARD_ID,
                        oid.toByteArray(), encoded, TEST_VECTOR_1, userVersion);
            }
            tr.commit().join();
        }

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        VectorIndexCrashRecovery.recover(
                context, vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 0, 6
        );

        assertEquals(1, group.getOnHeapIndexes().size());
        OnHeapVectorGraphIndex recovered = group.getOnHeapIndexes().getLast();
        assertEquals(total, recovered.size());
        for (ObjectId oid : oids) {
            assertNotNull(recovered.getMetadata().findNodeRef(oid));
        }
        group.closeAll();
    }

    @Test
    void shouldSkipEntriesAlreadyOnDisk() throws IOException {
        // Behavior: A node that was flushed to disk before the crash, but whose log entry was not
        // truncated yet, is not added to the on-heap index again during recovery. The generation
        // is still queued for flush, so the maintainer truncates its log.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        BucketService service = context.getService(BucketService.NAME);

        ObjectId oid = new ObjectId();
        byte[] trVersion;
        try (Transaction tr = context.getTransactionManager().create()) {
            VectorIndexMaintainer.appendMutation(tr, vectorIndex.subspace(), GENERATION_ID, MutationLogKind.INSERT, SHARD_ID,
                    oid.toByteArray(), encodedIndexEntry(), TEST_VECTOR_1, 0);
            var versionstampFuture = tr.getVersionstamp();
            tr.commit().join();
            trVersion = versionstampFuture.join();
        }
        Versionstamp versionstamp = Versionstamp.complete(trVersion, 0);

        VectorGraphIndexGroup flushed = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        OnHeapVectorGraphIndex onHeap = flushed.getOrCreateOnHeap(GENERATION_ID, DIMENSIONS, VectorSimilarityFunction.COSINE, 0, 6);
        onHeap.addGraphNode(oid, versionstamp, SHARD_ID, newEntryMetadata(), TEST_VECTOR_1, executor).join();
        flushed.flush(service.getBucketDataDir(), onHeap);
        flushed.closeAll();

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        List<OnDiskVectorGraphIndex> onDiskIndexes = VectorGraphIndexGroup.openOnDiskIndexes(
                context, vectorIndex.subspace(), service.getBucketDataDir(),
                VectorGraphIndexGroup.ownerShardId(metadata), metadata.uuid(), vectorIndex.definition().id(),
                VectorSimilarityFunction.COSINE
        );
        assertEquals(1, onDiskIndexes.size());
        for (OnDiskVectorGraphIndex onDisk : onDiskIndexes) {
            group.addOnDisk(onDisk);
        }

        VectorIndexCrashRecovery.recover(
                context, vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 0, 6
        );

        assertTrue(group.getOnHeapIndexes().isEmpty());
        assertEquals(1, group.searchAll(TEST_VECTOR_1, 2, 0.0f, 1.0f).size());
        ObjectId generationId = new ObjectId(GENERATION_ID);
        assertTrue(service.getVectorFlushQueue().stream().anyMatch(task -> task.key().generationId().equals(generationId)));
        group.closeAll();
    }

    @Test
    void shouldApplyDeleteAcrossGenerations() {
        // Behavior: An INSERT logged in one generation is deleted by a DELETE logged in a later
        // generation during recovery. The DELETE does not create an on-heap index for its generation.
        VectorIndex vectorIndex = createVectorIndex();
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        ObjectId oid = new ObjectId();
        byte[] firstGeneration = new ObjectId().toByteArray();
        try (Transaction tr = context.getTransactionManager().create()) {
            VectorIndexMaintainer.appendMutation(tr, vectorIndex.subspace(), firstGeneration, MutationLogKind.INSERT, SHARD_ID,
                    oid.toByteArray(), encodedIndexEntry(), TEST_VECTOR_1, 0);
            tr.commit().join();
        }
        byte[] secondGeneration = new ObjectId().toByteArray();
        try (Transaction tr = context.getTransactionManager().create()) {
            VectorIndexMaintainer.appendDeleteMutation(tr, vectorIndex.subspace(), secondGeneration, SHARD_ID, oid.toByteArray(), 0);
            tr.commit().join();
        }

        VectorGraphIndexGroup group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        VectorIndexCrashRecovery.recover(
                context, vectorIndex.subspace(), group,
                DIMENSIONS, VectorSimilarityFunction.COSINE, executor, 0, 6
        );

        assertEquals(1, group.getOnHeapIndexes().size());
        assertNull(group.getOnHeapIndexes().getFirst().getMetadata().findNodeRef(oid));
        assertTrue(group.searchAll(TEST_VECTOR_1, 1, 0.0f, 1.0f).isEmpty());
        group.closeAll();
    }
}
