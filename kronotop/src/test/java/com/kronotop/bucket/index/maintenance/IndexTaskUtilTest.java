/*
 * Copyright (c) 2023-2025 Burak Sezer
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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.TestUtil;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for IndexTaskUtil covering task subspace management.
 */
class IndexTaskUtilTest extends BaseBucketHandlerTest {

    @Test
    void shouldOpenTasksSubspace() {
        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);

        assertNotNull(taskSubspace, "Task subspace should not be null");
        assertNotNull(taskSubspace.getKey(), "Task subspace key should not be null");
        assertTrue(taskSubspace.getKey().length > 0, "Task subspace key should not be empty");
    }

    @Test
    void shouldOpenTasksSubspaceForMultipleShards() {
        DirectorySubspace subspace0 = IndexTaskUtil.openTasksSubspace(context, 0);
        DirectorySubspace subspace1 = IndexTaskUtil.openTasksSubspace(context, 1);
        DirectorySubspace subspace2 = IndexTaskUtil.openTasksSubspace(context, 2);

        assertNotNull(subspace0);
        assertNotNull(subspace1);
        assertNotNull(subspace2);

        assertNotEquals(subspace0.getKey(), subspace1.getKey(),
                "Different shards should have different subspaces");
        assertNotEquals(subspace1.getKey(), subspace2.getKey(),
                "Different shards should have different subspaces");
        assertNotEquals(subspace0.getKey(), subspace2.getKey(),
                "Different shards should have different subspaces");
    }

    @Test
    void shouldOpenTasksSubspaceConsistently() {
        DirectorySubspace subspace1 = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        DirectorySubspace subspace2 = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);

        assertNotNull(subspace1);
        assertNotNull(subspace2);
        assertArrayEquals(subspace1.getKey(), subspace2.getKey(),
                "Opening the same shard subspace should return consistent results");
    }

    @Test
    void shouldClearBucketTasks() {
        // Create a bucket (which has the primary index)
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Get the primary index name
        String primaryIndexName;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            primaryIndexName = IndexUtil.list(tr, metadata.subspace()).getFirst();
        }

        // Manually create a task and set a back pointer in the same transaction
        // so they share the same versionstamp
        DirectorySubspace indexSubspace;
        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        Versionstamp taskId;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            indexSubspace = IndexUtil.open(tr, metadata.subspace(), primaryIndexName);

            // Create a dummy task
            IndexBuildingTask task = new IndexBuildingTask(
                    TEST_NAMESPACE,
                    TEST_BUCKET,
                    1,
                    SHARD_ID,
                    TestUtil.generateVersionstamp(1),
                    TestUtil.generateVersionstamp(2)
            );

            // Create a task and back pointer in the same transaction with matching userVersion
            int userVersion = 0;
            var taskFuture = TaskStorage.create(tr, userVersion, taskSubspace, JSONUtil.writeValueAsBytes(task));
            IndexTaskUtil.setBackPointer(tr, indexSubspace, userVersion, SHARD_ID);

            tr.commit().join();
            taskId = Versionstamp.complete(taskFuture.join());
        }

        // Verify the task exists
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] definition = TaskStorage.getDefinition(tr, taskSubspace, taskId);
            assertNotNull(definition, "Task should exist before clearBucketTasks");
        }

        // Verify the back pointer exists
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            AtomicInteger backPointerCount = new AtomicInteger(0);
            IndexTaskUtil.scanTaskBackPointers(tr, indexSubspace, (id, shardId) -> {
                backPointerCount.incrementAndGet();
                return true;
            });
            assertTrue(backPointerCount.get() > 0, "Back pointer should exist before clearBucketTasks");
        }

        // Call clearBucketTasks
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            IndexTaskUtil.clearBucketTasks(tx, metadata);
            tr.commit().join();
        }

        // Verify task is removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] definition = TaskStorage.getDefinition(tr, taskSubspace, taskId);
            assertNull(definition, "Task should be removed after clearBucketTasks");
        }

        // Verify the back pointer is removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            AtomicInteger backPointerCount = new AtomicInteger(0);
            IndexTaskUtil.scanTaskBackPointers(tr, indexSubspace, (id, shardId) -> {
                backPointerCount.incrementAndGet();
                return true;
            });
            assertEquals(0, backPointerCount.get(), "Back pointer should be removed after clearBucketTasks");
        }
    }
}
