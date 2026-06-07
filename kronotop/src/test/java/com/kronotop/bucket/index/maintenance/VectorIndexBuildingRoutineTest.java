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

package com.kronotop.bucket.index.maintenance;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.vector.VectorGraphIndexGroup;
import com.kronotop.bucket.vector.VectorGraphIndexRegistry;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.server.resp3.ArrayRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class VectorIndexBuildingRoutineTest extends BaseBucketHandlerTest {

    private static final String SELECTOR = "embedding";
    private static final int DIMENSIONS = 3;

    @BeforeEach
    void setUp() {
        createBucket(TEST_BUCKET);
    }

    private VectorIndexDefinition createVectorIndex(IndexStatus status) {
        String name = VectorIndexNameGenerator.generate(SELECTOR, DIMENSIONS, DistanceFunction.COSINE);
        VectorIndexDefinition definition = VectorIndexDefinition.create(name, SELECTOR, DIMENSIONS, DistanceFunction.COSINE, status);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            VectorIndexUtil.create(tx, getBucketMetadata(TEST_BUCKET), definition);
            tr.commit().join();
        }
        return definition;
    }

    private void insertDocumentWithVector(double... components) {
        StringBuilder json = new StringBuilder("{\"embedding\": [");
        for (int i = 0; i < components.length; i++) {
            if (i > 0) json.append(", ");
            json.append(components[i]);
        }
        json.append("]}");

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(List.of(BSONUtil.jsonToDocumentThenBytes(json.toString())));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
    }

    private void insertDocumentWithoutVector() {
        String json = "{\"name\": \"test\"}";

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(List.of(BSONUtil.jsonToDocumentThenBytes(json)));
        cmd.insert(TEST_BUCKET, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
    }

    private int countFdbEntries(VectorIndex vectorIndex) {
        DirectorySubspace indexSubspace = vectorIndex.subspace();
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        AtomicInteger count = new AtomicInteger();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (KeyValue ignored : tr.getRange(Range.startsWith(prefix))) {
                count.incrementAndGet();
            }
        }
        return count.get();
    }

    @Test
    void shouldBuildVectorIndexAndTransitionToReady() {
        // Behavior: A full pipeline (BOUNDARY -> BUILD -> READY) completes successfully, and
        // FDB entries are written for all documents with vector fields.
        insertDocumentWithVector(0.1, 0.2, 0.3);
        insertDocumentWithVector(0.4, 0.5, 0.6);
        insertDocumentWithVector(0.7, 0.8, 0.9);

        VectorIndexDefinition definition = createVectorIndex(IndexStatus.WAITING);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);

        // Wait for the index to reach READY
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            VectorIndex index = metadata.vectorIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
            return index != null && index.definition().status() == IndexStatus.READY;
        });

        // Verify all tasks are swept
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            AtomicInteger counter = new AtomicInteger();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TaskStorage.tasks(tr, taskSubspace, (id) -> {
                    counter.incrementAndGet();
                    return true;
                });
                return counter.get() == 0;
            }
        });

        // Verify FDB entries were written
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
        assertEquals(3, countFdbEntries(vectorIndex));
    }

    @Test
    void shouldSkipDocumentsWithoutVectorField() {
        // Behavior: Documents missing the vector field are skipped during background building
        // without errors; only documents with vectors get FDB entries.
        insertDocumentWithVector(0.1, 0.2, 0.3);
        insertDocumentWithVector(0.4, 0.5, 0.6);
        insertDocumentWithoutVector();

        VectorIndexDefinition definition = createVectorIndex(IndexStatus.WAITING);

        // Wait for READY
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            VectorIndex index = metadata.vectorIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
            return index != null && index.definition().status() == IndexStatus.READY;
        });

        // Only 2 FDB entries (not 3)
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
        assertEquals(2, countFdbEntries(vectorIndex));
    }

    @Test
    void shouldTrackCardinalityForAllVectorDocuments() {
        // Behavior: After background building completes, the cardinality counter in FDB
        // equals the number of documents that had vector fields.
        insertDocumentWithVector(0.1, 0.2, 0.3);
        insertDocumentWithVector(0.4, 0.5, 0.6);
        insertDocumentWithVector(0.7, 0.8, 0.9);

        VectorIndexDefinition definition = createVectorIndex(IndexStatus.WAITING);

        // Wait for READY
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            VectorIndex index = metadata.vectorIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
            return index != null && index.definition().status() == IndexStatus.READY;
        });

        // Verify cardinality
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            Map<Long, IndexStatistics> stats = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics indexStats = stats.get(definition.id());
            assertNotNull(indexStats);
            assertEquals(3L, indexStats.cardinality());
        }
    }

    @Test
    void shouldTrackCardinalityOnlyForDocumentsWithVectorField() {
        // Behavior: Documents missing the vector field are skipped during background building;
        // cardinality counts only the documents that had vectors.
        insertDocumentWithVector(0.1, 0.2, 0.3);
        insertDocumentWithVector(0.4, 0.5, 0.6);
        insertDocumentWithoutVector();

        VectorIndexDefinition definition = createVectorIndex(IndexStatus.WAITING);

        // Wait for READY
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            VectorIndex index = metadata.vectorIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
            return index != null && index.definition().status() == IndexStatus.READY;
        });

        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
        assertEquals(2, countFdbEntries(vectorIndex));

        // Verify cardinality is 2, not 3
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<Long, IndexStatistics> stats = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics indexStats = stats.get(definition.id());
            assertNotNull(indexStats);
            assertEquals(2L, indexStats.cardinality());
        }
    }

    @Test
    void shouldHandleIndexDeletedDuringBuilding() {
        // Behavior: When a vector index is removed from metadata while building is in progress,
        // the watchdog detects the orphaned task and drops it during garbage collection.
        insertDocumentWithVector(0.1, 0.2, 0.3);

        VectorIndexDefinition definition = createVectorIndex(IndexStatus.WAITING);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);

        // Wait for the index to reach BUILDING or READY. Also, checking for READY because the background builder
        // changes the states very fast.
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            VectorIndex index = metadata.vectorIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
            return index != null && (index.definition().status() == IndexStatus.BUILDING
                    || index.definition().status() == IndexStatus.READY);
        });

        // Remove the index entirely
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            VectorIndexUtil.clear(tr, metadata.subspace(), definition.name());
            tr.commit().join();
        }

        // Wait for all tasks to be swept
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            AtomicInteger counter = new AtomicInteger();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TaskStorage.tasks(tr, taskSubspace, (id) -> {
                    counter.incrementAndGet();
                    return true;
                });
                return counter.get() == 0;
            }
        });
    }

    @Test
    void shouldStopBuildingWhenIndexStatusIsDropped() {
        // Behavior: When a vector index is marked DROPPED while building is in progress,
        // the watchdog detects the status change and sweeps the task.
        insertDocumentWithVector(0.1, 0.2, 0.3);
        insertDocumentWithVector(0.4, 0.5, 0.6);
        insertDocumentWithVector(0.7, 0.8, 0.9);

        VectorIndexDefinition definition = createVectorIndex(IndexStatus.WAITING);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);

        // Wait for the index to reach BUILDING or READY. Also, checking for READY because the background builder
        // changes the states very fast.
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            VectorIndex index = metadata.vectorIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
            return index != null && (index.definition().status() == IndexStatus.BUILDING
                    || index.definition().status() == IndexStatus.READY);
        });

        // Mark the index as DROPPED
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            VectorIndexDefinition dropped = definition.updateStatus(IndexStatus.DROPPED);
            VectorIndexUtil.saveIndexDefinition(tr, metadata, dropped);
            tr.commit().join();
        }

        // Wait for all tasks to be swept
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            AtomicInteger counter = new AtomicInteger();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TaskStorage.tasks(tr, taskSubspace, (id) -> {
                    counter.incrementAndGet();
                    return true;
                });
                return counter.get() == 0;
            }
        });
    }

    @Test
    void shouldSkipDocumentsWithMismatchedVectorDimensions() {
        // Behavior: Documents inserted before the vector index exists may have vectors with
        // wrong dimensions. The building routine skips them without errors; only documents
        // with correct dimensions get FDB entries and cardinality counts.
        insertDocumentWithVector(0.1, 0.2, 0.3);       // correct: 3 dimensions
        insertDocumentWithVector(0.4, 0.5);             // wrong: 2 dimensions
        insertDocumentWithVector(0.7, 0.8, 0.9);       // correct: 3 dimensions
        insertDocumentWithVector(1.0, 1.1, 1.2, 1.3);  // wrong: 4 dimensions

        VectorIndexDefinition definition = createVectorIndex(IndexStatus.WAITING);

        // Wait for READY
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            VectorIndex index = metadata.vectorIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
            return index != null && index.definition().status() == IndexStatus.READY;
        });

        // Only 2 correct-dimension documents should be indexed
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
        assertEquals(2, countFdbEntries(vectorIndex));

        // Cardinality should also be 2
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<Long, IndexStatistics> stats = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics indexStats = stats.get(definition.id());
            assertNotNull(indexStats);
            assertEquals(2L, indexStats.cardinality());
        }
    }

    @Test
    void shouldProduceSearchableOnDiskGraph() {
        // Behavior: After background building completes, the on-disk graph is registered
        // in the VectorGraphIndexGroup and is searchable.
        insertDocumentWithVector(0.1, 0.2, 0.3);
        insertDocumentWithVector(0.4, 0.5, 0.6);
        insertDocumentWithVector(0.7, 0.8, 0.9);

        VectorIndexDefinition definition = createVectorIndex(IndexStatus.WAITING);

        // Wait for READY
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            VectorIndex index = metadata.vectorIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
            return index != null && index.definition().status() == IndexStatus.READY;
        });

        // Verify on-disk index is registered and searchable
        BucketService bucketService = context.getService(BucketService.NAME);
        VectorGraphIndexRegistry registry = bucketService.getVectorGraphRegistry();
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        VectorGraphIndexGroup group = registry.get(metadata.namespace(), metadata.name(), definition.id());
        assertNotNull(group, "VectorGraphIndexGroup should be registered");
        assertFalse(group.getOnDiskIndexes().isEmpty(), "On-disk indexes should exist");

        // Search for nearest neighbors
        float[] queryVector = {0.1f, 0.2f, 0.3f};
        var results = group.searchAll(queryVector, 3, 0.0f, 1.0f);
        assertEquals(3, results.size(), "All 3 documents should be in the graph");
    }

    @Test
    void shouldBuildVectorIndexAcrossMultipleBatches() {
        // Behavior: When the bucket contains more documents than the batch size (100), the
        // building routine processes multiple batches via cursor pagination. All documents
        // get FDB entries, cardinality matches the total, and the index reaches READY.
        int totalDocs = 101;
        for (int i = 0; i < totalDocs; i++) {
            double val = (i + 1) * 0.01;
            insertDocumentWithVector(val, val + 0.1, val + 0.2);
        }

        VectorIndexDefinition definition = createVectorIndex(IndexStatus.WAITING);

        // Wait for READY (longer timeout for multi-batch processing)
        await().atMost(60, TimeUnit.SECONDS).until(() -> {
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            VectorIndex index = metadata.vectorIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
            return index != null && index.definition().status() == IndexStatus.READY;
        });

        // Verify all documents got FDB entries
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
        assertEquals(totalDocs, countFdbEntries(vectorIndex));

        // Verify cardinality matches total
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<Long, IndexStatistics> stats = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics indexStats = stats.get(definition.id());
            assertNotNull(indexStats);
            assertEquals((long) totalDocs, indexStats.cardinality());
        }
    }

}
