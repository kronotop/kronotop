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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.BucketShard;
import com.kronotop.bucket.index.statistics.IndexAnalyzeTaskState;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.KrExecutors;
import com.kronotop.internal.task.TaskStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * Monitors and manages index maintenance tasks for a specific bucket shard.
 *
 * <p>This watchdog service continuously monitors the task queue for new index building
 * tasks and manages their execution lifecycle. It performs the following key functions:
 * <ul>
 *   <li>Watches for new tasks using FoundationDB's watch mechanism</li>
 *   <li>Spawns worker threads to execute pending index building tasks</li>
 *   <li>Manages worker pool with backpressure control to prevent overload</li>
 *   <li>Cleans up stale or stuck workers that exceed the maximum stale period</li>
 *   <li>Coordinates task completion across shards using the task sweeper</li>
 * </ul>
 *
 * <p>The watchdog implements a bounded worker pool strategy using {@link KrExecutors#newBoundedExecutor}:
 * <ul>
 *   <li>Dynamic thread scaling from 0 to {@code WORKER_POOL_SIZE} threads (configurable via
 *       {@code bucket.index_maintenance.worker_pool_size}; defaults to number of CPU cores if set to 0)</li>
 *   <li>A maximum of {@code MAX_WORKER_POOL_SIZE} concurrent workers tracked in-memory (2x {@code WORKER_POOL_SIZE})</li>
 *   <li>LinkedBlockingQueue for task buffering when threads are busy</li>
 *   <li>1-minute thread keep-alive time for automatic thread cleanup during idle periods</li>
 *   <li>Automatic cleanup of stale workers that have been inactive for 60 seconds</li>
 * </ul>
 *
 * <p>Task coordination across shards is handled through a completion counter.
 * When all shards report a task as completed, the watchdog triggers the
 * {@link IndexMaintenanceTaskSweeper} to finalize the index and clean up task data.
 *
 * <p>The watchdog runs continuously until the shard is closed, using both reactive
 * (watch-based) and proactive (scheduled) approaches to ensure timely task processing
 * and resource cleanup.
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
    private final BucketService service;
    private final BucketShard shard;
    private final IndexMaintenanceTaskSweeper sweeper;
    private final DirectorySubspace subspace;
    private final byte[] trigger;
    private final ExecutorService workerExecutor;
    private final ScheduledExecutorService scheduler;
    private final Map<Versionstamp, Worker> workers = new ConcurrentHashMap<>();
    private volatile CompletableFuture<Void> watcher;

    /**
     * Constructs a new IndexMaintenanceWatchDog for the specified bucket shard.
     *
     * <p>Initializes the watchdog with:
     * <ul>
     *   <li>Task subspace for the shard's index maintenance tasks</li>
     *   <li>Task sweeper for cross-shard coordination</li>
     *   <li>Bounded worker executor using {@link KrExecutors#newBoundedExecutor} with configurable pool size
     *       (via {@code bucket.index_maintenance.worker_pool_size}; defaults to CPU cores if 0),
     *       task buffering via LinkedBlockingQueue, and 1-minute thread keep-alive timeout</li>
     *   <li>Single-threaded scheduled executor for periodic cleanup operations
     *       (runs at interval configured via {@code bucket.index_maintenance.worker_maintenance_interval})</li>
     * </ul>
     *
     * @param context the application context providing access to services and FoundationDB
     * @param shard   the bucket shard this watchdog will monitor
     */
    public IndexMaintenanceWatchDog(Context context, BucketShard shard) {
        this.context = context;
        this.service = context.getService(BucketService.NAME);
        this.shard = shard;
        this.subspace = IndexTaskUtil.openTasksSubspace(context, shard.id());
        this.sweeper = new IndexMaintenanceTaskSweeper(context);
        this.trigger = TaskStorage.trigger(subspace);

        int poolSize = context.getConfig().getInt("bucket.index_maintenance.worker_pool_size");
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
     * Creates a FoundationDB watch on the task trigger key.
     *
     * <p>This method establishes a watch on the trigger key that will be notified
     * when new tasks are added to the queue. The watch is committed in a transaction
     * to ensure it's properly registered with FoundationDB.
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
     * Callback hook invoked when an index task completes execution.
     *
     * <p>This method is called by workers when they finish processing a task.
     * It removes the completed task from the active workers map and triggers
     * task queue processing to handle any pending tasks.
     *
     * @param taskId the versionstamp identifier of the completed task
     */
    private void indexTaskCompletionHook(Versionstamp taskId) {
        workers.remove(taskId);
        processTaskQueue();
    }

    /**
     * Returns the active workers map.
     *
     * <p><b>Note:</b> Package-private for testing purposes only.
     *
     * @return the map of active workers keyed by task ID
     */
    Map<Versionstamp, Worker> getWorkers() {
        return workers;
    }

    /**
     * Removes stale workers that have been inactive beyond the maximum stale period.
     *
     * <p>This synchronized method iterates through all active workers and checks their
     * last activity timestamp. Workers are considered stale if:
     * <ul>
     *   <li>They have never executed and initiation time exceeds the stale period</li>
     *   <li>Their last execution time exceeds the stale period (60 seconds)</li>
     * </ul>
     *
     * <p>Stale workers are shut down gracefully and removed from the workers map.
     * This prevents resource leaks from stuck or abandoned worker threads.
     *
     * <p>This method is called periodically by the scheduled executor to maintain
     * a healthy worker pool.
     *
     * <p><b>Note:</b> Package-private for testing purposes.
     */
    synchronized void cleanupStaleWorkers() {
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

    private IndexMaintenanceTaskKind getTaskKind(Transaction tr, Versionstamp taskId) {
        byte[] definition = TaskStorage.getDefinition(tr, subspace, taskId);
        IndexMaintenanceTask task = JSONUtil.readValue(definition, IndexMaintenanceTask.class);
        return task.getKind();
    }

    /**
     * Spawns a new worker thread for the specified task if not already running.
     *
     * <p>This method creates and submits a new {@link IndexMaintenanceWorker} to the worker
     * executor pool. The worker will execute the appropriate maintenance routine based on the
     * task kind (BUILD or DROP).
     *
     * <p>Worker spawning logic:
     * <ul>
     *   <li>If a worker already exists for this task ID, returns {@code true} to continue processing</li>
     *   <li>Creates a new IndexMaintenanceWorker with completion callback</li>
     *   <li>Submits the worker to the bounded executor pool</li>
     *   <li>Tracks the worker and its future in the workers map</li>
     *   <li>Implements backpressure by returning {@code false} when pool limit is reached</li>
     * </ul>
     *
     * <p>The worker is registered with a completion hook ({@link #indexTaskCompletionHook})
     * that will be invoked when the task reaches a terminal state, allowing the watchdog
     * to clean up and spawn new workers for pending tasks.
     *
     * @param taskId the versionstamp identifier of the task to spawn a worker for
     * @return {@code true} if task queue processing should continue (worker spawned or already exists),
     * {@code false} if the worker pool has reached capacity and processing should pause
     */
    private boolean spawnWorker(Versionstamp taskId) {
        if (workers.containsKey(taskId)) {
            return true; // means continue
        }
        IndexMaintenanceWorker worker =
                new IndexMaintenanceWorker(context, subspace, shard.id(), taskId, this::indexTaskCompletionHook);
        Future<?> future = workerExecutor.submit(worker);
        workers.put(taskId, new Worker(worker, future));
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
     * Processes the task queue by spawning workers for pending tasks and cleaning up completed ones.
     *
     * <p>This synchronized method scans the task queue and takes appropriate action based on task status:
     * <ul>
     *   <li><b>WAITING/RUNNING tasks:</b> Spawns new worker threads if not already running</li>
     *   <li><b>COMPLETED tasks:</b> Triggers task sweeping when all shards report completion</li>
     *   <li><b>STOPPED tasks:</b> Immediately triggers task sweeping</li>
     * </ul>
     *
     * <p>Worker spawning behaviors:
     * <ul>
     *   <li>Respects the MAX_WORKER_POOL_SIZE limit to prevent overload</li>
     *   <li>Skips tasks that already have active workers</li>
     *   <li>Creates new IndexMaintenanceWorker instances for eligible tasks</li>
     *   <li>Registers workers with completion callbacks for lifecycle management</li>
     *   <li>Implements backpressure by stopping task spawning when pool limit is reached</li>
     * </ul>
     *
     * <p>This method is called:
     * <ul>
     *   <li>When the watch detects new tasks added to the queue</li>
     *   <li>After a worker completes execution</li>
     *   <li>Periodically by the scheduled executor for maintenance</li>
     * </ul>
     */
    private synchronized void processTaskQueue() {
        if (workers.size() >= MAX_WORKER_POOL_SIZE) {
            // There are already too many tasks in the executor's queue.
            return;
        }
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.tasks(tr, subspace, (taskId) -> {
                IndexMaintenanceTaskKind kind = getTaskKind(tr, taskId);
                IndexTaskStatus status = getIndexTaskStatus(kind, tr, taskId);
                if (status == IndexTaskStatus.RUNNING || status == IndexTaskStatus.WAITING) {
                    return spawnWorker(taskId);
                } else if (status == IndexTaskStatus.COMPLETED || status == IndexTaskStatus.STOPPED) {
                    sweeper.sweep(subspace, taskId);
                }
                return true;
            });
        }
    }

    /**
     * Main execution loop for the watchdog service.
     *
     * <p>This method runs continuously until the shard is closed and performs two
     * primary functions:
     * <ol>
     *   <li>Schedules periodic cleanup and task spawning every 60 seconds</li>
     *   <li>Watches for new tasks and spawns workers when detected</li>
     * </ol>
     *
     * <p>The periodic scheduler ensures that:
     * <ul>
     *   <li>Stale workers are cleaned up even if no new tasks arrive</li>
     *   <li>Pending tasks are eventually processed even if watches fail</li>
     * </ul>
     *
     * <p>The watch loop:
     * <ul>
     *   <li>Creates a watch on the task trigger key</li>
     *   <li>Blocks until the key changes (new task added)</li>
     *   <li>Spawns workers for pending tasks when triggered</li>
     *   <li>Recreates the watch for the next notification</li>
     * </ul>
     *
     * <p>Exceptions are logged except for expected CancellationExceptions during
     * shard closure. The method continues running even after exceptions to ensure
     * resilience.
     */
    @Override
    public void run() {
        int maintenanceInterval = context.getConfig().getInt("bucket.index_maintenance.worker_maintenance_interval");
        if (maintenanceInterval <= 0) {
            throw new IllegalStateException("bucket.index_maintenance.worker_maintenance_interval must be greater than zero");
        }
        scheduler.scheduleAtFixedRate(() -> {
            cleanupStaleWorkers();
            processTaskQueue();
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
                }
            }
        }
    }

    /**
     * Gracefully shuts down the watchdog service and all active workers.
     *
     * <p>This method performs a complete shutdown sequence:
     * <ol>
     *   <li>Cancels the active watch to stop waiting for new tasks</li>
     *   <li>Shuts down all active workers by calling their shutdown methods</li>
     *   <li>Initiates immediate shutdown of the worker executor service</li>
     *   <li>Waits up to 6 seconds for all workers to terminate</li>
     * </ol>
     *
     * <p>If workers don't terminate within the timeout period, a warning is logged
     * but the shutdown continues. This ensures the watchdog doesn't block the
     * shard closure process indefinitely.
     *
     * @throws RuntimeException if interrupted while waiting for executor termination
     */
    public void shutdown() {
        if (watcher != null) {
            watcher.cancel(true);
        }
        for (Worker worker : workers.values()) {
            worker.shutdown();
        }
        workerExecutor.shutdownNow();
        scheduler.shutdownNow();
        try {
            if (!workerExecutor.awaitTermination(6000, TimeUnit.MILLISECONDS)) {
                LOGGER.warn("Index maintenance worker pool did not fully terminate for shard: {}", shard.id());
            }
            if (!scheduler.awaitTermination(6000, TimeUnit.MILLISECONDS)) {
                LOGGER.warn("Index maintenance scheduler did not fully terminate for shard: {}", shard.id());
            }
        } catch (InterruptedException exp) {
            Thread.currentThread().interrupt();
            throw new KronotopException(exp);
        }
    }

    /**
     * Record representing an active worker and its associated future.
     *
     * <p>This record encapsulates:
     * <ul>
     *   <li>The IndexMaintenanceWorker instance performing the actual work</li>
     *   <li>The Future representing the worker's execution in the thread pool</li>
     * </ul>
     *
     * <p>The record provides a shutdown method that gracefully stops the worker
     * and cancels its future, ensuring clean termination of the task.
     *
     * @param instance the IndexMaintenanceWorker performing index building
     * @param future   the Future representing the worker's execution
     */
    record Worker(IndexMaintenanceWorker instance, Future<?> future) {
        /**
         * Shuts down this worker by stopping its execution and canceling its future.
         *
         * <p>This method first calls the worker instance's shutdown method to signal
         * graceful termination, then cancels the future with interruption to ensure
         * the thread pool releases the task.
         */
        void shutdown() {
            instance.shutdown();
            future.cancel(true);
        }
    }
}
