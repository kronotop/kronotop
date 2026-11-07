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
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.TestUtil;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.index.*;
import com.kronotop.internal.DirectoryUtil;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.Prefix;
import com.kronotop.volume.Volume;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

abstract class BaseVectorMaintainerTest extends BaseStandaloneInstanceTest {
    protected static final String SELECTOR = "embedding";
    protected static final int DIMENSIONS = 3;
    protected BucketService service;
    protected BucketMetadata metadata;
    protected VectorIndex vectorIndex;
    protected VectorGraphIndexGroup group;
    protected ExecutorService executor;

    @BeforeEach
    void setUp() {
        service = context.getService(BucketService.NAME);
        vectorIndex = createVectorIndex();
        metadata = getBucketMetadata(TEST_BUCKET);
        group = service.getVectorGraphRegistry().computeIfAbsent(
                metadata,
                vectorIndex, () -> service.bootstrapVectorGroup(metadata, vectorIndex)
        );
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

    protected OnHeapVectorGraphIndex addOneVector(ObjectId generationId) {
        OnHeapVectorGraphIndex onHeap = group.getOrCreateOnHeap(
                generationId.toByteArray(),
                DIMENSIONS,
                VectorSimilarityFunction.COSINE,
                0,
                6
        );
        onHeap.addGraphNode(
                new ObjectId(),
                TestUtil.zeroVersionstamp(),
                0,
                newEntryMetadata(1),
                new float[]{1.0f, 0.0f, 0.0f},
                executor
        ).join();
        return onHeap;
    }

    protected boolean hasKey(byte magic, ObjectId id) {
        byte[] key = vectorIndex.subspace().pack(Tuple.from(magic, id.toByteArray()));
        try (Transaction tr = context.getTransactionManager().create()) {
            return tr.get(key).join() != null;
        }
    }

    protected Path indexDir() {
        return VectorGraphIndexGroup.resolveIndexDir(context.getDataDir().resolve("bucket"), metadata, vectorIndex.definition().id());
    }

    protected Volume volume() {
        return service.getShard(VectorGraphIndexGroup.ownerShardId(metadata)).volume();
    }

    protected Prefix prefixOf(ObjectId indexId) {
        return new Prefix(indexId.toByteArray());
    }

    protected VectorSyncTask newSyncTask(ObjectId indexId) {
        return new VectorSyncTask(metadata.namespace(), metadata.name(), vectorIndex.definition().id(), indexId);
    }

    // Creates an on-heap index with one vector and flushes it, so its .index and .vmeta files are on disk.
    // The flush and sync queues are cleared first, so the test controls when the sync task runs.
    protected OnHeapVectorGraphIndex flushOneVector() {
        OnHeapVectorGraphIndex onHeap = addOneVector(new ObjectId());
        service.getVectorFlushQueue().clear();
        service.getVectorSyncQueue().clear();
        group.flush(service.getBucketDataDir(), onHeap);
        return onHeap;
    }

    protected boolean isSynced(ObjectId indexId) {
        return hasKey(IndexSubspaceMagic.GRAPH_SYNCED.getValue(), indexId);
    }
}
