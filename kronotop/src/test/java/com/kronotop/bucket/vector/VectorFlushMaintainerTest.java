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
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.TestUtil;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.index.*;
import com.kronotop.internal.DirectoryUtil;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.VolumeTestUtil;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class VectorFlushMaintainerTest extends BaseStandaloneInstanceTest {
    private static final String SELECTOR = "embedding";
    private static final int DIMENSIONS = 3;
    private BucketService service;
    private BucketMetadata metadata;
    private VectorIndex vectorIndex;
    private VectorGraphIndexGroup group;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        service = context.getService(BucketService.NAME);
        vectorIndex = createVectorIndex();
        metadata = getBucketMetadata(TEST_BUCKET);
        group = service.getVectorGraphRegistry().computeIfAbsent(
                metadata, vectorIndex, () -> service.bootstrapVectorGroup(metadata, vectorIndex));
        group.awaitReady();
        executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    @AfterEach
    public void tearDown() {
        executor.close();
        cleanupVectorIndexDir();
        super.tearDown();
    }

    private VectorIndex createVectorIndex() {
        createBucket(TEST_BUCKET);
        String name = VectorIndexNameGenerator.generate(SELECTOR, DIMENSIONS, DistanceFunction.COSINE);
        VectorIndexDefinition definition = VectorIndexDefinition.create(
                name, SELECTOR, DIMENSIONS, DistanceFunction.COSINE, IndexStatus.WAITING);
        try (Transaction tr = context.getTransactionManager().create()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            VectorIndexUtil.create(tx, getBucketMetadata(TEST_BUCKET), definition);
            tr.commit().join();
        }
        BucketMetadata md = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        return md.vectorIndexes().getIndexBySelector(SELECTOR, IndexSelectionPolicy.ALL);
    }

    private void cleanupVectorIndexDir() {
        Path vectorDir = VectorGraphIndexGroup.resolveVectorDir(
                context.getDataDir().resolve("bucket"),
                VectorGraphIndexGroup.ownerShardId(metadata)
        );
        DirectoryUtil.deleteRecursively(vectorDir);
    }

    private EntryMetadata newEntryMetadata(long segment) {
        return new EntryMetadata(1L, new byte[8], 0L, segment, 1L);
    }

    private VectorFlushTask newTask(ObjectId generationId) {
        VectorFlushKey key = new VectorFlushKey(metadata.namespace(), metadata.name(), vectorIndex.definition().id(), generationId);
        return new VectorFlushTask(key, 0);
    }

    private void addOneVector(ObjectId generationId) {
        OnHeapVectorGraphIndex onHeap = group.getOrCreateOnHeap(generationId.toByteArray(), DIMENSIONS, VectorSimilarityFunction.COSINE, 0, 6);
        onHeap.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
    }

    private List<KeyValue> mutationLogEntries(ObjectId generationId) {
        byte[] prefix = vectorIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.MUTATION_LOG.getValue(), generationId.toByteArray()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
        try (Transaction tr = context.getTransactionManager().create()) {
            return tr.getRange(begin, end).asList().join();
        }
    }

    @Test
    void shouldRescheduleWhenGenerationStillHasData() {
        // Behavior: When the on-heap index of a generation received a mutation inside the idle window,
        // the maintainer does not flush it and puts the task back into the queue with a later due time.
        ObjectId generationId = new ObjectId();
        addOneVector(generationId);
        service.getVectorFlushQueue().clear();

        service.getVectorFlushQueue().add(newTask(generationId));

        await().atMost(Duration.ofSeconds(5)).until(() ->
                service.getVectorFlushQueue().stream().anyMatch(task ->
                        task.key().generationId().equals(generationId) && task.dueNanos() > 0)
        );
        assertFalse(group.hasNoOnHeapIndexes(generationId));
        assertTrue(group.getOnDiskIndexes().isEmpty());
    }

    @Test
    void shouldFlushIdleIndexAndTruncateMutationLog() {
        // Behavior: When the on-heap index of a generation stayed idle past the idle window,
        // the maintainer writes it to disk and removes the mutation log entries of that generation.
        ObjectId generationId = new ObjectId();
        addOneVector(generationId);
        byte[] encodedEntry = new IndexEntry(1, VolumeTestUtil.generateEntryMetadata(1, 1, 0, 1, "test").encode()).encode();
        try (Transaction tr = context.getTransactionManager().create()) {
            VectorIndexMaintainer.appendMutation(
                    tr,
                    vectorIndex.subspace(),
                    generationId.toByteArray(),
                    MutationLogKind.INSERT,
                    1,
                    new ObjectId().toByteArray(),
                    encodedEntry,
                    new float[]{1.0f, 0.0f, 0.0f},
                    0
            );
            tr.commit().join();
        }
        assertEquals(1, mutationLogEntries(generationId).size());
        service.getVectorFlushQueue().clear();

        service.getVectorFlushQueue().add(newTask(generationId));

        await().atMost(Duration.ofSeconds(10)).until(() -> group.hasNoOnHeapIndexes(generationId));
        assertEquals(1, group.getOnDiskIndexes().size());
        await().atMost(Duration.ofSeconds(5)).until(() -> mutationLogEntries(generationId).isEmpty());
    }
}
