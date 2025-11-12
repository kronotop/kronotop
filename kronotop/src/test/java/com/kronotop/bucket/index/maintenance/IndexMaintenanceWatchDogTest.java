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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketShard;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.*;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;
import org.bson.BsonType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class IndexMaintenanceWatchDogTest extends BaseBucketHandlerTest {
    private static final String SKIP_WAIT_TRANSACTION_LIMIT_KEY =
            "__test__.index_maintenance.skip_wait_transaction_limit";

    @BeforeAll
    static void setUp() {
        System.setProperty(SKIP_WAIT_TRANSACTION_LIMIT_KEY, "false");
    }

    @AfterAll
    static void teardown() {
        System.clearProperty(SKIP_WAIT_TRANSACTION_LIMIT_KEY);
    }

    @Test
    void shouldBuildIndexAtBackground() throws Exception {
        // This test verifies that the background index building works correctly
        // while insert operations are still in progress.
        //
        // Steps:
        // 1. Start a background thread that performs 1000 inserts.
        // 2. Wait until half of the inserts (500) are done, then create an index definition and enqueue a build task.
        // 3. The background index builder should pick up the task and build the index for all documents
        //    that existed before the index was created.
        // 4. The remaining inserts should be automatically indexed by the regular insert path (Netty threads).
        // 5. Finally, assert that the index contains entries for all 2000 inserted documents (2 docs per insert).

        int halfway = 500;
        int totalInserts = 1000;

        CountDownLatch halfLatch = new CountDownLatch(halfway);
        CountDownLatch allLatch = new CountDownLatch(totalInserts);


        try (ExecutorService service = Executors.newSingleThreadExecutor()) {
            Future<?> bgFuture = service.submit(() -> insertAtBackground(halfLatch, allLatch, totalInserts));

            halfLatch.await();

            IndexDefinition definition = IndexDefinition.create(
                    "test-index",
                    "age",
                    BsonType.INT32
            );

            DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                IndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
                tr.commit().join();
            }

            AtomicReference<Versionstamp> taskId = new AtomicReference<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TaskStorage.tasks(tr, taskSubspace, (id) -> {
                    taskId.set(id);
                    return false; // break;
                });
            }

            allLatch.await();

            bgFuture.get();

            await().atMost(Duration.ofSeconds(15)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
                    Index index = metadata.indexes().getIndex(definition.selector(), IndexSelectionPolicy.ALL);
                    byte[] begin = index.subspace().pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
                    byte[] end = ByteArrayUtil.strinc(begin);

                    int count = 0;
                    for (KeyValue ignored : tr.getRange(begin, end)) {
                        count++;
                    }
                    return count == totalInserts * 2;
                }
            });

            await().atMost(15, TimeUnit.SECONDS).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    byte[] value = TaskStorage.getDefinition(tr, taskSubspace, taskId.get());
                    return value == null; // swept & dropped task
                }
            });
        }
    }

    @Test
    void test_cleanupStaleWorkers_noWorkers() {
        // Given: A watchdog with no workers
        BucketShard shard = getShard(SHARD_ID);
        IndexMaintenanceWatchDog watchdog = new IndexMaintenanceWatchDog(context, shard);

        // When: cleanupStaleWorkers is called
        watchdog.cleanupStaleWorkers();

        // Then: No workers should exist
        assertTrue(watchdog.getWorkers().isEmpty());

        watchdog.shutdown();
    }

    @Test
    void test_cleanupStaleWorkers_freshWorkerNotRemoved() {
        // Given: A watchdog with a fresh worker (just created)
        BucketShard shard = getShard(SHARD_ID);
        IndexMaintenanceWatchDog watchdog = new IndexMaintenanceWatchDog(context, shard);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexBuildingTask task = createIndexBuildingTask(1);
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        // Create a worker that was just initiated (fresh)
        IndexMaintenanceWorker worker = new IndexMaintenanceWorker(
                context,
                taskSubspace,
                SHARD_ID,
                taskId,
                (id) -> {
                }
        );
        Future<?> future = CompletableFuture.completedFuture(null);
        watchdog.getWorkers().put(taskId, new IndexMaintenanceWatchDog.Worker(worker, future));

        // When: cleanupStaleWorkers is called immediately
        watchdog.cleanupStaleWorkers();

        // Then: Worker should NOT be removed (it's fresh)
        assertEquals(1, watchdog.getWorkers().size());
        assertTrue(watchdog.getWorkers().containsKey(taskId));

        watchdog.shutdown();
    }

    @Test
    void test_cleanupStaleWorkers_neverExecutedStaleWorkerRemoved() throws Exception {
        // Given: A watchdog with a worker that never executed and is stale
        BucketShard shard = getShard(SHARD_ID);
        IndexMaintenanceWatchDog watchdog = new IndexMaintenanceWatchDog(context, shard);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexBuildingTask task = createIndexBuildingTask(1);
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        IndexMaintenanceWorker worker = new IndexMaintenanceWorker(
                context,
                taskSubspace,
                SHARD_ID,
                taskId,
                (id) -> {
                }
        );

        // Manipulate the metrics to make it appear stale (initiated 70 seconds ago)
        long staleInitiationTime = System.currentTimeMillis() - (watchdog.WORKER_MAX_STALE_PERIOD + 10000);
        setInitiatedAt(worker.getMetrics(), staleInitiationTime);

        Future<?> future = CompletableFuture.completedFuture(null);
        watchdog.getWorkers().put(taskId, new IndexMaintenanceWatchDog.Worker(worker, future));

        assertEquals(1, watchdog.getWorkers().size());

        // When: cleanupStaleWorkers is called
        watchdog.cleanupStaleWorkers();

        // Then: Worker should be removed (it's stale and never executed)
        assertTrue(watchdog.getWorkers().isEmpty());

        watchdog.shutdown();
    }

    @Test
    void test_cleanupStaleWorkers_executedButStaleWorkerRemoved() {
        // Given: A watchdog with a worker that executed but is now stale
        BucketShard shard = getShard(SHARD_ID);
        IndexMaintenanceWatchDog watchdog = new IndexMaintenanceWatchDog(context, shard);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexBuildingTask task = createIndexBuildingTask(1);
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        IndexMaintenanceWorker worker = new IndexMaintenanceWorker(
                context,
                taskSubspace,
                SHARD_ID,
                taskId,
                (id) -> {
                }
        );

        // Set latestExecution to a stale timestamp (70 seconds ago)
        long staleExecutionTime = System.currentTimeMillis() - (watchdog.WORKER_MAX_STALE_PERIOD + 10000);
        worker.getMetrics().setLatestExecution(staleExecutionTime);

        Future<?> future = CompletableFuture.completedFuture(null);
        watchdog.getWorkers().put(taskId, new IndexMaintenanceWatchDog.Worker(worker, future));

        assertEquals(1, watchdog.getWorkers().size());

        // When: cleanupStaleWorkers is called
        watchdog.cleanupStaleWorkers();

        // Then: Worker should be removed (last execution was too long ago)
        assertTrue(watchdog.getWorkers().isEmpty());

        watchdog.shutdown();
    }

    @Test
    void test_cleanupStaleWorkers_executedRecentlyNotRemoved() {
        // Given: A watchdog with a worker that executed recently
        BucketShard shard = getShard(SHARD_ID);
        IndexMaintenanceWatchDog watchdog = new IndexMaintenanceWatchDog(context, shard);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexBuildingTask task = createIndexBuildingTask(1);
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        IndexMaintenanceWorker worker = new IndexMaintenanceWorker(
                context,
                taskSubspace,
                SHARD_ID,
                taskId,
                (id) -> {
                }
        );

        // Set latestExecution to a recent timestamp (30 seconds ago - within the 60s window)
        long recentExecutionTime = System.currentTimeMillis() - 30000;
        worker.getMetrics().setLatestExecution(recentExecutionTime);

        Future<?> future = CompletableFuture.completedFuture(null);
        watchdog.getWorkers().put(taskId, new IndexMaintenanceWatchDog.Worker(worker, future));

        assertEquals(1, watchdog.getWorkers().size());

        // When: cleanupStaleWorkers is called
        watchdog.cleanupStaleWorkers();

        // Then: Worker should NOT be removed (it executed recently)
        assertEquals(1, watchdog.getWorkers().size());
        assertTrue(watchdog.getWorkers().containsKey(taskId));

        watchdog.shutdown();
    }

    @Test
    void test_cleanupStaleWorkers_mixedWorkers() throws Exception {
        // Given: A watchdog with multiple workers - some stale, some fresh
        BucketShard shard = getShard(SHARD_ID);
        IndexMaintenanceWatchDog watchdog = new IndexMaintenanceWatchDog(context, shard);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);

        // Create 4 tasks
        IndexBuildingTask task1 = createIndexBuildingTask(1);
        IndexBuildingTask task2 = createIndexBuildingTask(2);
        IndexBuildingTask task3 = createIndexBuildingTask(3);
        IndexBuildingTask task4 = createIndexBuildingTask(4);

        Versionstamp taskId1 = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task1));
        Versionstamp taskId2 = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task2));
        Versionstamp taskId3 = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task3));
        Versionstamp taskId4 = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task4));

        // Worker 1: Fresh (just initiated)
        IndexMaintenanceWorker worker1 = new IndexMaintenanceWorker(context, taskSubspace, SHARD_ID, taskId1, (id) -> {
        });
        watchdog.getWorkers().put(taskId1, new IndexMaintenanceWatchDog.Worker(worker1, CompletableFuture.completedFuture(null)));

        // Worker 2: Stale (never executed, initiated 70s ago)
        IndexMaintenanceWorker worker2 = new IndexMaintenanceWorker(context, taskSubspace, SHARD_ID, taskId2, (id) -> {
        });
        setInitiatedAt(worker2.getMetrics(), System.currentTimeMillis() - (watchdog.WORKER_MAX_STALE_PERIOD + 10000));
        watchdog.getWorkers().put(taskId2, new IndexMaintenanceWatchDog.Worker(worker2, CompletableFuture.completedFuture(null)));

        // Worker 3: Fresh (executed 30s ago)
        IndexMaintenanceWorker worker3 = new IndexMaintenanceWorker(context, taskSubspace, SHARD_ID, taskId3, (id) -> {
        });
        worker3.getMetrics().setLatestExecution(System.currentTimeMillis() - 30000);
        watchdog.getWorkers().put(taskId3, new IndexMaintenanceWatchDog.Worker(worker3, CompletableFuture.completedFuture(null)));

        // Worker 4: Stale (last executed 70s ago)
        IndexMaintenanceWorker worker4 = new IndexMaintenanceWorker(context, taskSubspace, SHARD_ID, taskId4, (id) -> {
        });
        worker4.getMetrics().setLatestExecution(System.currentTimeMillis() - (watchdog.WORKER_MAX_STALE_PERIOD + 10000));
        watchdog.getWorkers().put(taskId4, new IndexMaintenanceWatchDog.Worker(worker4, CompletableFuture.completedFuture(null)));

        assertEquals(4, watchdog.getWorkers().size());

        // When: cleanupStaleWorkers is called
        watchdog.cleanupStaleWorkers();

        // Then: Only the 2 fresh workers should remain (worker1 and worker3)
        assertEquals(2, watchdog.getWorkers().size());
        assertTrue(watchdog.getWorkers().containsKey(taskId1));
        assertFalse(watchdog.getWorkers().containsKey(taskId2)); // Stale - removed
        assertTrue(watchdog.getWorkers().containsKey(taskId3));
        assertFalse(watchdog.getWorkers().containsKey(taskId4)); // Stale - removed

        watchdog.shutdown();
    }

    @Test
    void test_cleanupStaleWorkers_boundaryCondition_exactlyAtStaleLimit() {
        // Given: A worker at exactly the stale boundary (60s)
        BucketShard shard = getShard(SHARD_ID);
        IndexMaintenanceWatchDog watchdog = new IndexMaintenanceWatchDog(context, shard);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexBuildingTask task = createIndexBuildingTask(1);
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        IndexMaintenanceWorker worker = new IndexMaintenanceWorker(context, taskSubspace, SHARD_ID, taskId, (id) -> {
        });

        // Set latestExecution to exactly the stale period (60s ago)
        long exactlyStaleTime = System.currentTimeMillis() - watchdog.WORKER_MAX_STALE_PERIOD;
        worker.getMetrics().setLatestExecution(exactlyStaleTime);

        Future<?> future = CompletableFuture.completedFuture(null);
        watchdog.getWorkers().put(taskId, new IndexMaintenanceWatchDog.Worker(worker, future));

        // When: cleanupStaleWorkers is called
        watchdog.cleanupStaleWorkers();

        // Then: Worker should NOT be removed (condition is <, not <=)
        // lastActivity + WORKER_MAX_STALE_PERIOD < now
        assertEquals(1, watchdog.getWorkers().size());

        watchdog.shutdown();
    }

    @Test
    void test_cleanupStaleWorkers_boundaryCondition_justBeyondStaleLimit() {
        // Given: A worker just beyond the stale boundary (60s + 1ms)
        BucketShard shard = getShard(SHARD_ID);
        IndexMaintenanceWatchDog watchdog = new IndexMaintenanceWatchDog(context, shard);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexBuildingTask task = createIndexBuildingTask(1);
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));

        IndexMaintenanceWorker worker = new IndexMaintenanceWorker(context, taskSubspace, SHARD_ID, taskId, (id) -> {
        });

        // Set latestExecution to just beyond the stale period (60s + 1ms ago)
        long justBeyondStaleTime = System.currentTimeMillis() - (watchdog.WORKER_MAX_STALE_PERIOD + 1);
        worker.getMetrics().setLatestExecution(justBeyondStaleTime);

        Future<?> future = CompletableFuture.completedFuture(null);
        watchdog.getWorkers().put(taskId, new IndexMaintenanceWatchDog.Worker(worker, future));

        // When: cleanupStaleWorkers is called
        watchdog.cleanupStaleWorkers();

        // Then: Worker SHOULD be removed (it's beyond the limit)
        assertTrue(watchdog.getWorkers().isEmpty());

        watchdog.shutdown();
    }

    /**
     * Helper method to set the initiatedAt field using reflection.
     * This is necessary because IndexMaintenanceRoutineMetrics sets initiatedAt in constructor.
     */
    private void setInitiatedAt(IndexMaintenanceRoutineMetrics metrics, long timestamp) throws Exception {
        Field initiatedAtField = IndexMaintenanceRoutineMetrics.class.getDeclaredField("initiatedAt");
        initiatedAtField.setAccessible(true);
        initiatedAtField.set(metrics, timestamp);
    }
}
