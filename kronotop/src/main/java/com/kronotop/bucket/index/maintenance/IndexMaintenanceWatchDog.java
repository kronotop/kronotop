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
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.*;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.statistics.IndexAnalyzeTaskState;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.internal.ExecutorServiceUtil;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.KrExecutors;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.namespace.NamespaceBeingRemovedException;
import com.kronotop.namespace.NoSuchNamespaceException;
import com.kronotop.worker.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/**
 * Watches the task queue of one bucket shard and runs its index maintenance tasks.
 *
 * <p>The watchdog waits on a FoundationDB watch for new tasks, spawns {@link IndexMaintenanceWorker}
 * threads for pending tasks, and hands COMPLETED or STOPPED tasks to the
 * {@link IndexMaintenanceTaskSweeper}. A scheduled pass also runs periodically so stale workers are
 * cleaned up and pending tasks are processed even if a watch is missed. Maintenance runs only on the
 * primary owner of the shard.
 *
 * <p>Workers run in a bounded pool from {@link KrExecutors#newBoundedExecutor} sized by
 * {@code bucket.index.maintenance.worker_pool_size} (defaults to CPU cores when 0), with a 1-minute
 * thread keep-alive. At most {@code MAX_WORKER_POOL_SIZE} workers (twice the pool size) are tracked
 * in memory, which provides backpressure. Workers idle beyond {@code WORKER_MAX_STALE_PERIOD} (60s)
 * are shut down.
 *
 * @see IndexMaintenanceWorker
 * @see IndexMaintenanceTaskSweeper
 * @see BucketShard
 */
public class IndexMaintenanceWatchDog implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexMaintenanceWatchDog.class);
    final long WORKER_MAX_STALE_PERIOD = 60000; // 60s (package-private for testing)
    private final int MAX_WORKER_POOL_SIZE;
    private final Context context;
    private final BucketShard shard;
    private final IndexMaintenanceTaskSweeper sweeper;
    private final DirectorySubspace subspace;
    private final byte[] trigger;
    private final ExecutorService workerExecutor;
    private final ScheduledExecutorService scheduler;
    private final Map<Versionstamp, WorkerHandle> workers = new ConcurrentHashMap<>();
    private volatile boolean shuttingDown;
    private volatile CompletableFuture<Void> watcher;

    /**
     * Creates a watchdog for the given bucket shard.
     *
     * <p>Opens the shard's task subspace, creates the sweeper, and sets up the bounded worker
     * executor (pool size from {@code bucket.index.maintenance.worker_pool_size}, CPU cores when 0)
     * and the single-threaded scheduler used for periodic maintenance.
     *
     * @param context the application context providing access to services and FoundationDB
     * @param shard   the bucket shard this watchdog will monitor
     */
    public IndexMaintenanceWatchDog(Context context, BucketShard shard) {
        this.context = context;
        this.shard = shard;
        this.subspace = IndexTaskUtil.openTasksSubspace(context, shard.id());
        this.sweeper = new IndexMaintenanceTaskSweeper(context);
        this.trigger = TaskStorage.trigger(subspace);

        int poolSize = context.getConfig().getInt("bucket.index.maintenance.worker_pool_size");
        int WORKER_POOL_SIZE = (poolSize == 0)
                ? Runtime.getRuntime().availableProcessors()
                : poolSize;
        this.MAX_WORKER_POOL_SIZE = WORKER_POOL_SIZE * 2;

        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat("kr-index-maintenance-%d")
                .build();
        this.workerExecutor = KrExecutors.newBoundedExecutor(
                WORKER_POOL_SIZE,
                1L,
                TimeUnit.MINUTES,
                factory
        );
        this.scheduler = Executors.newSingleThreadScheduledExecutor(factory);
    }

    /**
     * Registers a FoundationDB watch on the task trigger key.
     *
     * <p>The watch is committed so FoundationDB registers it, and completes when a new task
     * is added to the queue.
     *
     * @return a CompletableFuture that completes when the watched key changes
     */
    private CompletableFuture<Void> watcher() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            CompletableFuture<Void> watcher = tr.watch(trigger);
            tr.commit().join();
            return watcher;
        }
    }

    /**
     * Completion hook that workers call when they finish a task.
     *
     * <p>Removes the worker from the active map and reprocesses the queue so pending tasks
     * can be picked up.
     *
     * @param taskId the versionstamp identifier of the completed task
     */
    private void indexTaskCompletionHook(Versionstamp taskId) {
        WorkerHandle handle = workers.remove(taskId);
        if (handle != null) {
            context.getWorkerRegistry().remove(handle.getWorker().getNamespace(), handle);
        }
        processTaskQueue();
    }

    /**
     * Returns the active workers map.
     *
     * <p><b>Note:</b> Package-private for testing purposes only.
     *
     * @return the map of active workers keyed by task ID
     */
    Map<Versionstamp, WorkerHandle> getWorkers() {
        return workers;
    }

    /**
     * Cleans up stale workers that have been inactive beyond the maximum stale period.
     *
     * <p>Stale workers are first collected and shut down inside the synchronized
     * {@link #collectStaleWorkers()} method, but <b>not</b> removed from the workers map
     * at that point. This ensures that during the subsequent {@code await()} calls
     * (which run outside the lock), {@link #spawnWorker} still sees the entry and
     * will not spawn a duplicate worker for the same task.
     *
     * <p>Map removal happens only after {@code await()} completes for each stale worker.
     *
     * <p><b>Note:</b> Package-private for testing purposes.
     */
    void cleanupStaleWorkers() {
        List<Map.Entry<Versionstamp, WorkerHandle>> staleWorkers = collectStaleWorkers();
        for (Map.Entry<Versionstamp, WorkerHandle> entry : staleWorkers) {
            WorkerHandle worker = entry.getValue();
            try {
                worker.await(
                        ExecutorServiceUtil.DEFAULT_TIMEOUT,
                        ExecutorServiceUtil.DEFAULT_TIMEOUT_TIMEUNIT
                );
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            workers.remove(entry.getKey());
            context.getWorkerRegistry().remove(worker.getWorker().getNamespace(), worker);
        }
    }

    private synchronized List<Map.Entry<Versionstamp, WorkerHandle>> collectStaleWorkers() {
        List<Map.Entry<Versionstamp, WorkerHandle>> staleWorkers = new ArrayList<>();
        if (shuttingDown) return staleWorkers;
        long now = System.currentTimeMillis();
        for (Map.Entry<Versionstamp, WorkerHandle> entry : workers.entrySet()) {
            WorkerHandle worker = entry.getValue();
            IndexMaintenanceWorker instance = worker.getWorker();

            boolean neverExecuted = instance.getMetrics().getLatestExecution() == 0;
            long lastActivity = neverExecuted
                    ? instance.getMetrics().getInitiatedAt()
                    : instance.getMetrics().getLatestExecution();

            boolean stale = lastActivity + WORKER_MAX_STALE_PERIOD < now;
            if (stale) {
                worker.shutdown();
                staleWorkers.add(entry);
            }
        }
        return staleWorkers;
    }

    private boolean bucketExists(Transaction tr, String namespace, String bucket) {
        try {
            BucketMetadataUtil.reload(context, tr, namespace, bucket);
            return true;
        } catch (NoSuchBucketException |
                 NoSuchNamespaceException |
                 NamespaceBeingRemovedException |
                 BucketBeingRemovedException e
        ) {
            return false;
        }
    }

    private boolean indexExists(Transaction tr, IndexMaintenanceTask task) {
        BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, task.getNamespace(), task.getBucket());
        long indexId = task.getIndexId();
        if (metadata.indexes().getIndexById(indexId, IndexSelectionPolicy.ALL) != null) {
            return true;
        }
        if (metadata.compoundIndexes().getIndexById(indexId, IndexSelectionPolicy.ALL) != null) {
            return true;
        }
        return metadata.vectorIndexes().getIndexById(indexId, IndexSelectionPolicy.ALL) != null;
    }

    /**
     * Spawns a worker for the task unless one already runs for it.
     *
     * <p>Submits a new {@link IndexMaintenanceWorker} to the bounded pool, tracks it in the
     * workers map, and wires {@link #indexTaskCompletionHook} as its completion callback. If a
     * worker already exists for this task ID, does nothing and returns {@code true}.
     *
     * @param taskId the versionstamp identifier of the task to spawn a worker for
     * @return {@code true} if queue processing should continue, {@code false} when the worker pool
     * has reached capacity and processing should pause
     */
    private boolean spawnWorker(Versionstamp taskId) {
        if (shuttingDown) return false;
        if (workers.containsKey(taskId)) {
            return true; // means continue
        }
        IndexMaintenanceWorker worker =
                new IndexMaintenanceWorker(context, subspace, shard.id(), taskId, this::indexTaskCompletionHook);
        Future<?> future = workerExecutor.submit(worker);
        WorkerHandle handle = new WorkerHandle(worker, future);
        workers.put(taskId, handle);
        context.getWorkerRegistry().put(worker.getNamespace(), handle);
        // Backpressure
        return workers.size() < MAX_WORKER_POOL_SIZE;
    }

    private IndexTaskStatus getIndexTaskStatus(IndexMaintenanceTaskKind kind, Transaction tr, Versionstamp taskId) {
        return switch (kind) {
            case BOUNDARY -> {
                IndexBoundaryTaskState state = IndexBoundaryTaskState.load(tr, subspace, taskId);
                yield state.status();
            }
            case BUILD -> {
                IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, subspace, taskId);
                yield state.status();
            }
            case DROP -> {
                IndexDropTaskState state = IndexDropTaskState.load(tr, subspace, taskId);
                yield state.status();
            }
            case ANALYZE -> {
                IndexAnalyzeTaskState state = IndexAnalyzeTaskState.load(tr, subspace, taskId);
                yield state.status();
            }
        };
    }

    /**
     * Scans the task queue and acts on each task by its status.
     *
     * <p>WAITING and RUNNING tasks get a worker via {@link #spawnWorker}. COMPLETED and STOPPED
     * tasks are handed to the sweeper. Runs only on the shard's primary owner, and stops early
     * when the worker pool is at capacity.
     *
     * <p>Before checking status, each task is garbage collected: if its bucket, namespace, or
     * index no longer exists, the task is orphaned and dropped. This cleans up tasks left behind
     * by bucket or namespace purges and index deletions.
     *
     * <p>Called when the watch fires, after a worker completes, and periodically by the scheduler.
     */
    synchronized void processTaskQueue() {
        if (shuttingDown) return;
        if (workers.size() >= MAX_WORKER_POOL_SIZE) {
            // There are already too many tasks in the executor's queue.
            return;
        }

        // Index maintenance runs only on the primary owner of this shard.
        RoutingService routing = context.getService(RoutingService.NAME);
        Route route = routing.findRoute(ShardKind.BUCKET, shard.id());
        if (route == null || !route.primary().equals(context.getMember())) {
            return;
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.tasks(tr, subspace, (taskId) -> {
                byte[] definition = TaskStorage.getDefinition(tr, subspace, taskId);
                IndexMaintenanceTask task = JSONUtil.readValue(definition, IndexMaintenanceTask.class);

                // Garbage collection: check if the bucket still exists
                if (!bucketExists(tr, task.getNamespace(), task.getBucket())) {
                    LOGGER.info("Bucket '{}' purged, dropping orphaned task", task.getBucket());
                    TaskStorage.drop(tr, subspace, taskId);
                    return true; // continue to the next task
                }

                // Garbage collection: check if the index still exists
                if (!indexExists(tr, task)) {
                    LOGGER.info("Index {} in bucket '{}' deleted, dropping orphaned task",
                            task.getIndexId(), task.getBucket());
                    TaskStorage.drop(tr, subspace, taskId);
                    return true; // continue to the next task
                }

                IndexTaskStatus status = getIndexTaskStatus(task.getKind(), tr, taskId);
                if (status == IndexTaskStatus.RUNNING || status == IndexTaskStatus.WAITING) {
                    return spawnWorker(taskId);
                } else if (status == IndexTaskStatus.COMPLETED || status == IndexTaskStatus.STOPPED) {
                    sweeper.sweep(subspace, taskId);
                }
                return true;
            });
            tr.commit().join();
        }
    }

    /**
     * Runs the watchdog until the shard is closed.
     *
     * <p>Starts a periodic task at {@code bucket.index.maintenance.worker_maintenance_interval}
     * seconds that cleans up stale workers and processes the queue, so pending tasks make progress
     * even if a watch is missed. Then loops on the task watch: it waits for the trigger key to
     * change, processes the queue, and re-arms the watch.
     *
     * <p>Exceptions are logged and the loop keeps running, except for the CancellationException
     * expected when the shard closes.
     */
    @Override
    public void run() {
        int maintenanceInterval = context.getConfig().getInt("bucket.index.maintenance.worker_maintenance_interval");
        if (maintenanceInterval <= 0) {
            throw new IllegalStateException("bucket.index.maintenance.worker_maintenance_interval must be greater than zero");
        }
        scheduler.scheduleAtFixedRate(() -> {
            try {
                cleanupStaleWorkers();
                processTaskQueue();
            } catch (Exception e) {
                LOGGER.error("Scheduled maintenance failed for shard: {}", shard.id(), e);
            }
        }, 0, maintenanceInterval, TimeUnit.SECONDS);

        while (!shard.isClosed()) {
            try {
                watcher = watcher();
                // Waits until receiving a new task
                watcher.join();
                processTaskQueue();
            } catch (Exception exp) {
                if (!(shard.isClosed() && exp instanceof CancellationException)) {
                    LOGGER.error("Failed to run shard maintenance worker on Bucket shard: {}", shard.id(), exp);
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                }
            }
        }
    }

    /**
     * Shuts down the watchdog and all active workers.
     *
     * <p>Cancels the watch, shuts down every active worker, then stops the worker executor and
     * scheduler and waits up to the default termination timeout for them. A warning is logged if
     * they do not terminate in time, so shard closure is never blocked indefinitely.
     *
     * @throws KronotopException if interrupted while waiting for termination
     */
    public void shutdown() {
        shuttingDown = true;
        if (watcher != null) {
            watcher.cancel(true);
        }
        Iterator<Map.Entry<Versionstamp, WorkerHandle>> iterator = workers.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Versionstamp, WorkerHandle> entry = iterator.next();
            WorkerHandle handle = entry.getValue();
            handle.shutdown();
            context.getWorkerRegistry().remove(handle.getWorker().getNamespace(), handle);
            iterator.remove();
        }
        workerExecutor.shutdownNow();
        scheduler.shutdownNow();
        try {
            if (!workerExecutor.awaitTermination(
                    ExecutorServiceUtil.DEFAULT_TIMEOUT,
                    ExecutorServiceUtil.DEFAULT_TIMEOUT_TIMEUNIT
            )) {
                LOGGER.warn("Index maintenance worker pool did not fully terminate for shard: {}", shard.id());
            }
            if (!scheduler.awaitTermination(
                    ExecutorServiceUtil.DEFAULT_TIMEOUT,
                    ExecutorServiceUtil.DEFAULT_TIMEOUT_TIMEUNIT
            )) {
                LOGGER.warn("Index maintenance scheduler did not fully terminate for shard: {}", shard.id());
            }
        } catch (InterruptedException exp) {
            Thread.currentThread().interrupt();
            throw new KronotopException(exp);
        }
    }

    /**
     * Pairs an {@link IndexMaintenanceWorker} with the {@link Future} of its pool submission,
     * so the worker can be stopped and its future cancelled together.
     */
    protected static class WorkerHandle implements Worker {
        private final IndexMaintenanceWorker worker;
        private final Future<?> future;

        /**
         * Creates a new WorkerHandle.
         *
         * @param worker the index maintenance worker instance
         * @param future the future representing the worker's execution
         */
        WorkerHandle(IndexMaintenanceWorker worker, Future<?> future) {
            this.worker = worker;
            this.future = future;
        }

        /**
         * Returns the underlying IndexMaintenanceWorker.
         *
         * @return the wrapped worker instance
         */
        public IndexMaintenanceWorker getWorker() {
            return worker;
        }

        /**
         * Returns the tag of the underlying worker.
         *
         * @return the worker name
         */
        @Override
        public String getTag() {
            return worker.getTag();
        }

        /**
         * Signals the worker to stop, then cancels its future with interruption so the thread
         * pool releases the task.
         */
        @Override
        public void shutdown() {
            worker.shutdown();
            future.cancel(true);
        }

        /**
         * Waits for the worker to complete within the specified timeout.
         *
         * @param timeout the maximum time to wait
         * @param unit    the time unit of the timeout
         * @return true if the worker completed, false if timeout elapsed
         * @throws InterruptedException if interrupted while waiting
         */
        @Override
        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return worker.await(timeout, unit);
        }
    }
}
