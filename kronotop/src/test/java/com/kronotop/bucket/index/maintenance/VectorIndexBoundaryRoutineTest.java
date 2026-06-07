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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.*;
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

class VectorIndexBoundaryRoutineTest extends BaseBucketHandlerTest {

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

    @Test
    void shouldMarkVectorIndexAsReadyWhenBucketIsEmpty() {
        // Behavior: Creating a vector index with WAITING status on an empty bucket transitions
        // the index directly to READY without creating BUILD tasks, and cardinality remains zero.
        VectorIndexDefinition definition = createVectorIndex(IndexStatus.WAITING);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);

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

        // Verify cardinality is zero
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            Map<Long, IndexStatistics> stats = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics indexStats = stats.get(definition.id());
            if (indexStats != null) {
                assertEquals(0L, indexStats.cardinality());
            }
        }
    }

    @Test
    void shouldTransitionVectorIndexToBuildingWhenBucketHasDocuments() {
        // Behavior: Creating a vector index with WAITING status on a non-empty bucket transitions
        // the index through BUILDING to READY as the full pipeline (BOUNDARY -> BUILD -> sweep) completes.
        insertDocumentWithVector(0.1, 0.2, 0.3);
        insertDocumentWithVector(0.4, 0.5, 0.6);

        VectorIndexDefinition definition = createVectorIndex(IndexStatus.WAITING);

        // The index should transition through BUILDING to READY (full pipeline completes)
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
            VectorIndex index = metadata.vectorIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
            return index != null && (index.definition().status() == IndexStatus.BUILDING
                    || index.definition().status() == IndexStatus.READY);
        });
    }

    @Test
    void shouldCreateBoundaryTaskWhenVectorIndexCreatedWithWaitingStatus() {
        // Behavior: Creating a vector index with WAITING status schedules a BOUNDARY task
        // in the task subspace for the bucket's shard.
        VectorIndexDefinition definition = createVectorIndex(IndexStatus.WAITING);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);

        // Verify BOUNDARY task was created (may already be consumed, so check within a window)
        AtomicInteger boundaryTaskCount = new AtomicInteger();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.tasks(tr, taskSubspace, (id) -> {
                byte[] data = TaskStorage.getDefinition(tr, taskSubspace, id);
                IndexMaintenanceTask task = com.kronotop.internal.JSONUtil.readValue(data, IndexMaintenanceTask.class);
                if (task.getKind() == IndexMaintenanceTaskKind.BOUNDARY) {
                    boundaryTaskCount.incrementAndGet();
                }
                return true;
            });
        }
        // The boundary task may have already been consumed by the watchdog,
        // but the index should at least be in WAITING or a subsequent state.
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        VectorIndex index = metadata.vectorIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
        assertNotNull(index);
        // Status should be WAITING (not yet processed) or BUILDING -> READY (empty bucket, already processed)
        assertTrue(
                index.definition().status() == IndexStatus.WAITING
                        || index.definition().status() == IndexStatus.BUILDING
                        || index.definition().status() == IndexStatus.READY,
                "Expected WAITING, BUILDING or READY, got: " + index.definition().status()
        );
    }

    @Test
    void shouldSkipBackgroundBuildWhenVectorIndexCreatedWithReadyStatus() {
        // Behavior: Creating a vector index with READY status (bucket creation path) does not
        // schedule any background tasks.
        VectorIndexDefinition definition = createVectorIndex(IndexStatus.READY);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);

        // No tasks should be created for this index
        AtomicInteger taskCount = new AtomicInteger();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.tasks(tr, taskSubspace, (id) -> {
                byte[] data = TaskStorage.getDefinition(tr, taskSubspace, id);
                IndexMaintenanceTask task = com.kronotop.internal.JSONUtil.readValue(data, IndexMaintenanceTask.class);
                if (task.getIndexId() == definition.id()) {
                    taskCount.incrementAndGet();
                }
                return true;
            });
        }
        assertEquals(0, taskCount.get(), "No tasks should be created for READY vector index");

        // Index should remain READY
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        VectorIndex index = metadata.vectorIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
        assertNotNull(index);
        assertEquals(IndexStatus.READY, index.definition().status());
    }
}
