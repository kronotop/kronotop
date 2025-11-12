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

import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.RetryMethods;
import com.kronotop.bucket.index.statistics.IndexAnalyzeRoutine;
import com.kronotop.bucket.index.statistics.IndexAnalyzeTask;
import com.kronotop.bucket.index.statistics.IndexAnalyzeTaskState;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.internal.task.TaskStorage;

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
    private final Context context;
    private final IndexMaintenanceRoutine routine;
    private final DirectorySubspace subspace;
    private final Versionstamp taskId;
    private final Consumer<Versionstamp> completionHook;
    private volatile boolean shutdown;

    /**
     * Creates a worker for the specified task by loading its definition and instantiating the appropriate routine.
     *
     * @param context        application context for service and FoundationDB access
     * @param subspace       directory subspace containing task data
     * @param shardId        bucket shard ID for this worker
     * @param taskId         unique task identifier
     * @param completionHook callback invoked when task reaches terminal state
     * @throws IllegalStateException if task kind is unrecognized
     */
    public IndexMaintenanceWorker(Context context, DirectorySubspace subspace, int shardId, Versionstamp taskId, Consumer<Versionstamp> completionHook) {
        this.context = context;
        this.subspace = subspace;
        this.taskId = taskId;
        this.completionHook = completionHook;

        byte[] definition = TransactionUtils.execute(context, tr -> TaskStorage.getDefinition(tr, subspace, taskId));
        IndexMaintenanceTask base = JSONUtil.readValue(definition, IndexMaintenanceTask.class);
        switch (base.getKind()) {
            case BOUNDARY -> {
                IndexBoundaryTask task = JSONUtil.readValue(definition, IndexBoundaryTask.class);
                this.routine = new IndexBoundaryRoutine(context, subspace, taskId, task);
            }
            case BUILD -> {
                IndexBuildingTask task = JSONUtil.readValue(definition, IndexBuildingTask.class);
                this.routine = new IndexBuildingRoutine(context, subspace, shardId, taskId, task);
            }
            case DROP -> {
                IndexDropTask task = JSONUtil.readValue(definition, IndexDropTask.class);
                this.routine = new IndexDropRoutine(context, subspace, taskId, task);
            }
            case ANALYZE -> {
                IndexAnalyzeTask task = JSONUtil.readValue(definition, IndexAnalyzeTask.class);
                this.routine = new IndexAnalyzeRoutine(context, subspace, taskId, task);
            }
            default -> throw new IllegalStateException("Unknown task kind: " + base.getKind());
        }
    }

    private IndexTaskStatus getRoutineStatus() {
        return switch (routine) {
            case IndexBoundaryRoutine ignored -> {
                IndexBoundaryTaskState state = TransactionUtils.execute(context, tr -> IndexBoundaryTaskState.load(tr, subspace, taskId));
                yield state.status();
            }
            case IndexBuildingRoutine ignored -> {
                IndexBuildingTaskState state = TransactionUtils.execute(context, tr -> IndexBuildingTaskState.load(tr, subspace, taskId));
                yield state.status();
            }
            case IndexDropRoutine ignored -> {
                IndexDropTaskState state = TransactionUtils.execute(context, tr -> IndexDropTaskState.load(tr, subspace, taskId));
                yield state.status();
            }
            case IndexAnalyzeRoutine ignored -> {
                IndexAnalyzeTaskState state = TransactionUtils.execute(context, tr -> IndexAnalyzeTaskState.load(tr, subspace, taskId));
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
     * Executes the maintenance routine with retry logic until task reaches terminal state or shutdown.
     *
     * <p>Invokes completion hook and initiates shutdown upon task completion.
     */
    @Override
    public void run() {
        try {
            RetryMethods.retry(RetryMethods.INDEX_MAINTENANCE_ROUTINE).executeRunnable(() -> {
                if (shutdown) {
                    throw new IndexMaintenanceRoutineShutdownException();
                }
                routine.start();
                IndexTaskStatus status = getRoutineStatus();
                if (isTerminal(status)) {
                    // Run a callback to remove this task from the watchdog thread.
                    completionHook.accept(taskId);
                    shutdown();
                }
            });
        } catch (IndexMaintenanceRoutineShutdownException ignored) {
            // ignore it, this only signals the retry mech
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
        shutdown = true;
        routine.stop();
    }
}
