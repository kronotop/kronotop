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

package com.kronotop.bucket;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.Context;
import com.kronotop.bucket.index.*;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

import static com.kronotop.internal.task.TaskStorage.TASKS_MAGIC;

public class IndexMaintenanceWatchDog implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexMaintenanceWatchDog.class);
    private static final int WORKER_POOL_SIZE = 2;
    private static final int MAX_WORKER_POOL_SIZE = WORKER_POOL_SIZE * 2;
    private final long WORKER_MAX_STALE_PERIOD = 60000; // 60s
    private final Context context;
    private final BucketService service;
    private final BucketShard shard;
    private final IndexMaintenanceTaskSweeper sweeper;
    private final DirectorySubspace subspace;
    private final byte[] trigger;
    private final ExecutorService workerExecutor;
    private final ScheduledExecutorService scheduler;
    private final Map<Versionstamp, Worker> workers = new ConcurrentHashMap<>();
    private volatile CompletableFuture<Void> watcher;

    public IndexMaintenanceWatchDog(Context context, BucketShard shard) {
        this.context = context;
        this.service = context.getService(BucketService.NAME);
        this.shard = shard;
        this.subspace = IndexTaskUtil.createOrOpenTasksSubspace(context, shard.id());
        this.sweeper = new IndexMaintenanceTaskSweeper(context);
        this.trigger = TaskStorage.trigger(subspace);

        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat("kr-index-maintenance-%d")
                .build();
        this.workerExecutor = Executors.newFixedThreadPool(WORKER_POOL_SIZE, factory);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(factory);
    }

    private CompletableFuture<Void> watcher() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            CompletableFuture<Void> watcher = tr.watch(trigger);
            tr.commit().join();
            return watcher;
        }
    }

    private void indexTaskCompletionHook(Versionstamp taskId) {
        workers.remove(taskId);
        spawnWorkersForPendingTasks();
    }

    private synchronized void cleanupStaleWorkers() {
        long now = System.currentTimeMillis();
        workers.entrySet().removeIf(entry -> {
            Worker worker = entry.getValue();
            IndexMaintenanceWorker instance = worker.instance();

            boolean neverExecuted = instance.getMetrics().getLatestExecution() == 0;
            long lastActivity = neverExecuted
                    ? instance.getMetrics().getInitiatedAt()
                    : instance.getMetrics().getLatestExecution();

            boolean stale = lastActivity + WORKER_MAX_STALE_PERIOD < now;
            if (stale) {
                worker.shutdown();
            }
            return stale; // remove from map if stale
        });
    }

    private synchronized void spawnWorkersForPendingTasks() {
        if (workers.size() >= MAX_WORKER_POOL_SIZE) {
            // There are already too many tasks in the executor's queue.
            return;
        }
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] begin = subspace.pack(Tuple.from(TASKS_MAGIC));
            byte[] end = ByteArrayUtil.strinc(begin);
            for (KeyValue entry : tr.getRange(begin, end)) {
                Tuple tuple = subspace.unpack(entry.getKey());
                Versionstamp taskId = (Versionstamp) tuple.get(1);
                IndexBuilderTaskState state = IndexBuilderTaskState.load(tr, subspace, taskId);
                IndexTaskStatus status = state.status();
                if (status == IndexTaskStatus.RUNNING || status == IndexTaskStatus.WAITING) {
                    if (workers.containsKey(taskId)) {
                        continue;
                    }
                    IndexMaintenanceWorker worker =
                            new IndexMaintenanceWorker(context, subspace, shard.id(), taskId, this::indexTaskCompletionHook);
                    Future<?> future = workerExecutor.submit(worker);
                    workers.put(taskId, new Worker(worker, future));
                    if (workers.size() >= MAX_WORKER_POOL_SIZE) {
                        // Backpressure - WORKER_POOL_SIZE = 2
                        break;
                    }
                } else if (status == IndexTaskStatus.COMPLETED) {
                    int counter = IndexTaskUtil.readTaskCounter(context, taskId);
                    if (counter == service.getNumberOfShards()) {
                        sweeper.sweep(subspace, taskId);
                    }
                }
            }
        }
    }

    @Override
    public void run() {
        scheduler.scheduleAtFixedRate(() -> {
            cleanupStaleWorkers();
            spawnWorkersForPendingTasks();
        }, 0, 60, TimeUnit.SECONDS);

        while (!shard.isClosed()) {
            try {
                watcher = watcher();
                // Waits until receiving a new task
                watcher.join();
                spawnWorkersForPendingTasks();
            } catch (Exception exp) {
                if (!(shard.isClosed() && exp instanceof CancellationException)) {
                    LOGGER.error("Failed to run shard maintenance worker on Bucket shard: {}", shard.id(), exp);
                }
            }
        }
    }

    public void shutdown() {
        if (watcher != null) {
            watcher.cancel(true);
        }
        for (Worker worker : workers.values()) {
            worker.shutdown();
        }
        workerExecutor.shutdownNow();
        try {
            if (!workerExecutor.awaitTermination(6000, TimeUnit.MILLISECONDS)) {
                LOGGER.warn("Failed to stop all index maintenance workers for Bucket shard: {}", shard.id());
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    record Worker(IndexMaintenanceWorker instance, Future<?> future) {
        void shutdown() {
            instance.shutdown();
            future.cancel(true);
        }
    }
}
