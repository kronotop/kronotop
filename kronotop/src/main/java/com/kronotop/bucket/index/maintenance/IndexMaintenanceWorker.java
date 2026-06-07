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

import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.RetryMethods;
import com.kronotop.bucket.index.IndexType;
import com.kronotop.bucket.index.statistics.IndexAnalyzeRoutine;
import com.kronotop.bucket.index.statistics.IndexAnalyzeTask;
import com.kronotop.bucket.index.statistics.IndexAnalyzeTaskState;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.transaction.TransactionUtil;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Executes index maintenance tasks (BOUNDARY, BUILD, DROP, STATS) with retry logic and lifecycle management.
 *
 * <p>Workers load task definitions from FoundationDB, create the appropriate routine, and execute it
 * until completion or shutdown. Upon reaching a terminal state (COMPLETED, FAILED, STOPPED), workers
 * notify the watchdog via completion hook and terminate gracefully.
 *
 * @see IndexMaintenanceRoutine
 * @see IndexMaintenanceWatchDog
 */
public class IndexMaintenanceWorker implements Runnable {
    private static final int NEW = 0;
    private static final int RUNNING = 1;
    private static final int SHUTDOWN = 2;

    private final Context context;
    private final String namespace;
    private final String tag;
    private final IndexMaintenanceRoutine routine;
    private final DirectorySubspace subspace;
    private final Versionstamp taskId;
    private final Consumer<Versionstamp> completionHook;
    private final CountDownLatch latch = new CountDownLatch(1);
    private final AtomicInteger state = new AtomicInteger(NEW);

    /**
     * Creates a worker for the specified task by loading its definition and instantiating the appropriate routine.
     *
     * @param context        application context for service and FoundationDB access
     * @param subspace       directory subspace containing task data
     * @param shardId        bucket shard ID for this worker
     * @param taskId         unique task identifier
     * @param completionHook callback invoked when a task reaches its terminal state
     * @throws IllegalStateException if the task kind is unrecognized
     */
    public IndexMaintenanceWorker(Context context, DirectorySubspace subspace, int shardId, Versionstamp taskId, Consumer<Versionstamp> completionHook) {
        this.context = context;
        this.subspace = subspace;
        this.taskId = taskId;
        this.completionHook = completionHook;

        byte[] definition = TransactionUtil.execute(context, tr -> TaskStorage.getDefinition(tr, subspace, taskId));
        IndexMaintenanceTask base = JSONUtil.readValue(definition, IndexMaintenanceTask.class);
        switch (base.getKind()) {
            case BOUNDARY -> {
                IndexBoundaryTask task = JSONUtil.readValue(definition, IndexBoundaryTask.class);
                this.tag = task.getTag();
                this.namespace = task.getNamespace();
                if (task.getIndexType() == IndexType.COMPOUND) {
                    this.routine = new CompoundIndexBoundaryRoutine(context, subspace, taskId, task);
                } else if (task.getIndexType() == IndexType.VECTOR) {
                    this.routine = new VectorIndexBoundaryRoutine(context, subspace, taskId, task);
                } else {
                    this.routine = new IndexBoundaryRoutine(context, subspace, taskId, task);
                }
            }
            case BUILD -> {
                IndexBuildingTask task = JSONUtil.readValue(definition, IndexBuildingTask.class);
                this.tag = task.getTag();
                this.namespace = task.getNamespace();
                if (task.getIndexType() == IndexType.COMPOUND) {
                    this.routine = new CompoundIndexBuildingRoutine(context, subspace, shardId, taskId, task);
                } else if (task.getIndexType() == IndexType.VECTOR) {
                    this.routine = new VectorIndexBuildingRoutine(context, subspace, shardId, taskId, task);
                } else {
                    this.routine = new SingleFieldIndexBuildingRoutine(context, subspace, shardId, taskId, task);
                }
            }
            case DROP -> {
                IndexDropTask task = JSONUtil.readValue(definition, IndexDropTask.class);
                this.tag = task.getTag();
                this.namespace = task.getNamespace();
                this.routine = new IndexDropRoutine(context, subspace, taskId, task);
            }
            case ANALYZE -> {
                IndexAnalyzeTask task = JSONUtil.readValue(definition, IndexAnalyzeTask.class);
                this.tag = task.getTag();
                this.namespace = task.getNamespace();
                this.routine = new IndexAnalyzeRoutine(context, subspace, taskId, task);
            }
            default -> throw new IllegalStateException("Unknown task kind: " + base.getKind());
        }
    }

    public String getTag() {
        return tag;
    }

    public String getNamespace() {
        return namespace;
    }

    private IndexTaskStatus getRoutineStatus() {
        return switch (routine) {
            case IndexBoundaryRoutine ignored -> {
                IndexBoundaryTaskState state = TransactionUtil.execute(context, tr -> IndexBoundaryTaskState.load(tr, subspace, taskId));
                yield state.status();
            }
            case CompoundIndexBoundaryRoutine ignored -> {
                IndexBoundaryTaskState state = TransactionUtil.execute(context, tr -> IndexBoundaryTaskState.load(tr, subspace, taskId));
                yield state.status();
            }
            case VectorIndexBoundaryRoutine ignored -> {
                IndexBoundaryTaskState state = TransactionUtil.execute(context, tr -> IndexBoundaryTaskState.load(tr, subspace, taskId));
                yield state.status();
            }
            case SingleFieldIndexBuildingRoutine ignored -> {
                IndexBuildingTaskState state = TransactionUtil.execute(context, tr -> IndexBuildingTaskState.load(tr, subspace, taskId));
                yield state.status();
            }
            case CompoundIndexBuildingRoutine ignored -> {
                IndexBuildingTaskState state = TransactionUtil.execute(context, tr -> IndexBuildingTaskState.load(tr, subspace, taskId));
                yield state.status();
            }
            case VectorIndexBuildingRoutine ignored -> {
                IndexBuildingTaskState state = TransactionUtil.execute(context, tr -> IndexBuildingTaskState.load(tr, subspace, taskId));
                yield state.status();
            }
            case IndexDropRoutine ignored -> {
                IndexDropTaskState state = TransactionUtil.execute(context, tr -> IndexDropTaskState.load(tr, subspace, taskId));
                yield state.status();
            }
            case IndexAnalyzeRoutine ignored -> {
                IndexAnalyzeTaskState state = TransactionUtil.execute(context, tr -> IndexAnalyzeTaskState.load(tr, subspace, taskId));
                yield state.status();
            }
            default -> throw new IllegalStateException("Unexpected value: " + routine);
        };
    }

    /**
     * Determines if task status is terminal (COMPLETED, FAILED, or STOPPED).
     *
     * @param status task status to check
     * @return true if terminal, false otherwise
     */
    private boolean isTerminal(IndexTaskStatus status) {
        return status.equals(IndexTaskStatus.COMPLETED) || status.equals(IndexTaskStatus.FAILED) || status.equals(IndexTaskStatus.STOPPED);
    }

    /**
     * Executes the maintenance routine with retry logic until the task reaches terminal state or shutdown.
     *
     * <p>Always invokes the completion hook on exit (via {@code finally}) so the worker is
     * deregistered from the registry regardless of outcome. If the task state is still RUNNING
     * in FDB, the WatchDog can re-spawn a new worker for it later.
     */
    @Override
    public void run() {
        if (!state.compareAndSet(NEW, RUNNING)) {
            // Already SHUTDOWN before run() started — skip execution.
            latch.countDown();
            return;
        }
        try {
            RetryMethods.retry(RetryMethods.INDEX_MAINTENANCE_ROUTINE).executeRunnable(() -> {
                if (state.get() == SHUTDOWN) {
                    throw new IndexMaintenanceRoutineShutdownException();
                }
                routine.start();
                IndexTaskStatus status = getRoutineStatus();
                if (isTerminal(status)) {
                    shutdown();
                }
            });
        } catch (IndexMaintenanceRoutineShutdownException ignored) {
        } finally {
            completionHook.accept(taskId);
            latch.countDown();
        }
    }

    /**
     * Returns runtime metrics from the underlying routine for watchdog monitoring.
     *
     * @return routine metrics
     */
    public IndexMaintenanceRoutineMetrics getMetrics() {
        return routine.getMetrics();
    }

    /**
     * Initiates graceful shutdown by stopping the routine and signaling retry loop exit.
     *
     * <p>Thread-safe and idempotent.
     */
    public void shutdown() {
        if (state.compareAndSet(NEW, SHUTDOWN)) {
            // run() hasn't started — it either won't run (future cancelled)
            // or will see SHUTDOWN and count down immediately. Count down here
            // so await() never blocks when run() is never invoked.
            latch.countDown();
        } else {
            state.set(SHUTDOWN);
        }
        routine.stop();
    }

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }
}
