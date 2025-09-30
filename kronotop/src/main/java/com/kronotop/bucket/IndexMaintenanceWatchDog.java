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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.index.IndexBuilderTaskState;
import com.kronotop.bucket.index.IndexTaskStatus;
import com.kronotop.bucket.index.IndexTaskUtil;
import com.kronotop.internal.task.TaskStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

import static com.kronotop.internal.task.TaskStorage.TASKS_MAGIC;

public class IndexMaintenanceWatchDog implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexMaintenanceWatchDog.class);
    private static final int WORKER_POOL_SIZE = 2;
    private final Context context;
    private final BucketShard shard;
    private final DirectorySubspace subspace;
    private final byte[] trigger;
    private final ExecutorService workerExecutor;
    private final Map<Versionstamp, Future<?>> workers = new ConcurrentHashMap<>();
    private volatile CompletableFuture<Void> watcher;

    public IndexMaintenanceWatchDog(Context context, BucketShard shard) {
        this.context = context;
        this.shard = shard;
        this.subspace = IndexTaskUtil.createOrOpenTasksSubspace(context, shard.id());
        this.trigger = TaskStorage.trigger(subspace);
        this.workerExecutor = Executors.newFixedThreadPool(WORKER_POOL_SIZE);
    }

    private CompletableFuture<Void> watcher() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            CompletableFuture<Void> watcher = tr.watch(trigger);
            tr.commit().join();
            return watcher;
        }
    }

    private void indexTaskCompletionHook(Versionstamp taskId) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilderTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.COMPLETED);
            tr.commit().join();
            workers.remove(taskId);
            spawnWorkersForPendingTasks();
        } catch (CompletionException exp) {
            if (exp.getCause() instanceof FDBException fdbException) {
                int code = fdbException.getCode();
                if (code == 1007 || code == 1020) {
                    indexTaskCompletionHook(taskId);
                    return;
                }
            }
            throw exp;
        }
    }

    private synchronized void spawnWorkersForPendingTasks() {
        if (workers.size() >= WORKER_POOL_SIZE * 2) {
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
                    workers.put(taskId, future);
                    if (workers.size() >= WORKER_POOL_SIZE * 2) {
                        // Backpressure - WORKER_POOL_SIZE = 2
                        break;
                    }
                }
            }
        }
    }

    @Override
    public void run() {
        while (!shard.isClosed()) {
            try {
                // Initial run, start the workers if we have room to run tasks.
                spawnWorkersForPendingTasks();
                watcher = watcher();
                // Waits until receiving a new task
                watcher.join();
                spawnWorkersForPendingTasks();
            } catch (Exception e) {
                if (!shard.isClosed()) {
                    LOGGER.error("Failed to run shard maintenance worker on Bucket shard: {}", shard.id(), e);
                }
            }
        }
    }

    public void shutdown() {
        if (watcher != null) {
            watcher.cancel(true);
        }
        for (Future<?> worker : workers.values()) {
            worker.cancel(true);
        }
        workerExecutor.shutdownNow();
    }
}
