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
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;

import java.util.function.Consumer;

/**
 * Worker thread responsible for executing index maintenance tasks.
 *
 * <p>This class serves as the execution wrapper for index maintenance routines,
 * managing their lifecycle and coordinating with the watchdog service. Each worker:
 * <ul>
 *   <li>Loads the task definition from FoundationDB</li>
 *   <li>Creates the appropriate maintenance routine based on task kind:
 *       <ul>
 *         <li>{@link BackgroundIndexBuildingRoutine} for BUILD tasks</li>
 *         <li>{@link IndexDropRoutine} for DROP tasks</li>
 *       </ul>
 *   </li>
 *   <li>Executes the routine with retry logic for transient failures</li>
 *   <li>Notifies the watchdog upon task completion via a callback hook</li>
 *   <li>Handles graceful shutdown when requested</li>
 * </ul>
 *
 * <p>The worker implements a retry mechanism that continues execution until:
 * <ul>
 *   <li>The task reaches a terminal state (COMPLETED, FAILED, STOPPED)</li>
 *   <li>The worker is explicitly shut down</li>
 *   <li>An unrecoverable error occurs</li>
 * </ul>
 *
 * <p>Workers are managed by the {@link IndexMaintenanceWatchDog} which spawns them
 * for pending tasks and monitors their lifecycle. Upon completion, workers notify
 * the watchdog to remove them from the active worker pool and potentially spawn
 * new workers for other pending tasks.
 *
 * @see IndexMaintenanceRoutine
 * @see BackgroundIndexBuildingRoutine
 * @see IndexDropRoutine
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
     * Constructs a new IndexMaintenanceWorker for a specific task.
     *
     * <p>This constructor performs the following initialization:
     * <ul>
     *   <li>Loads the task definition from FoundationDB using the provided task ID</li>
     *   <li>Deserializes the task definition to determine the task kind (BUILD or DROP)</li>
     *   <li>Creates the appropriate maintenance routine based on task kind:
     *       <ul>
     *         <li>BUILD: Creates {@link BackgroundIndexBuildingRoutine} for index building</li>
     *         <li>DROP: Creates {@link IndexDropRoutine} for index removal</li>
     *       </ul>
     *   </li>
     * </ul>
     *
     * <p>The completion hook provided will be called when the task reaches a terminal
     * state, allowing the watchdog to clean up and manage the worker pool.
     *
     * @param context        the application context providing access to services and FoundationDB
     * @param subspace       the directory subspace containing the task data
     * @param shardId        the ID of the bucket shard this worker is operating on
     * @param taskId         the unique versionstamp identifier of the task to execute
     * @param completionHook callback to invoke when the task reaches a terminal state
     * @throws IllegalStateException if the task kind is not recognized
     */
    public IndexMaintenanceWorker(Context context, DirectorySubspace subspace, int shardId, Versionstamp taskId, Consumer<Versionstamp> completionHook) {
        this.context = context;
        this.subspace = subspace;
        this.taskId = taskId;
        this.completionHook = completionHook;

        byte[] definition = context.getFoundationDB().run(tr -> TaskStorage.getDefinition(tr, subspace, taskId));
        IndexMaintenanceTask base = JSONUtil.readValue(definition, IndexMaintenanceTask.class);
        switch (base.getKind()) {
            case BUILD -> {
                IndexBuildingTask task = JSONUtil.readValue(definition, IndexBuildingTask.class);
                this.routine = new BackgroundIndexBuildingRoutine(context, subspace, shardId, taskId, task);
            }
            case DROP -> {
                IndexDropTask task = JSONUtil.readValue(definition, IndexDropTask.class);
                this.routine = new IndexDropRoutine(context, subspace, taskId, task);
            }
            default -> throw new IllegalStateException("Unknown task kind: " + base.getKind());
        }
    }

    /**
     * Executes the index maintenance task with retry logic.
     *
     * <p>This method runs the maintenance routine within a retry wrapper that handles
     * transient failures and ensures robust execution. The execution flow:
     * <ol>
     *   <li>Checks if shutdown has been requested before starting</li>
     *   <li>Starts the maintenance routine (e.g., index building)</li>
     *   <li>Monitors the task state after execution</li>
     *   <li>If the task reaches a terminal state (COMPLETED, FAILED, STOPPED):
     *       <ul>
     *         <li>Invokes the completion hook to notify the watchdog</li>
     *         <li>Initiates worker shutdown</li>
     *       </ul>
     *   </li>
     * </ol>
     *
     * <p>The retry mechanism will continue attempting execution until either:
     * <ul>
     *   <li>The task completes successfully</li>
     *   <li>The worker is shut down</li>
     *   <li>An unrecoverable error occurs</li>
     * </ul>
     *
     * <p>IndexMaintenanceRoutineShutdownException is caught and ignored as it's
     * used solely as a signal to the retry mechanism to stop gracefully.
     */
    @Override
    public void run() {
        try {
            RetryMethods.retry(RetryMethods.INDEX_MAINTENANCE_ROUTINE).executeRunnable(() -> {
                if (shutdown) {
                    throw new IndexMaintenanceRoutineShutdownException();
                }
                routine.start();
                IndexBuildingTaskState state = context.getFoundationDB().run(tr -> IndexBuildingTaskState.load(tr, subspace, taskId));
                if (IndexBuildingTaskState.isTerminal(state.status())) {
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
     * Returns metrics for the underlying index maintenance routine.
     *
     * <p>Provides access to runtime metrics from the maintenance routine, including:
     * <ul>
     *   <li>Number of entries processed</li>
     *   <li>Timestamp of the latest execution</li>
     *   <li>Task initiation time</li>
     * </ul>
     *
     * <p>These metrics are used by the watchdog to monitor worker health and detect
     * stale workers that may need to be cleaned up.
     *
     * @return the {@link IndexMaintenanceRoutineMetrics} from the underlying routine
     */
    public IndexMaintenanceRoutineMetrics getMetrics() {
        return routine.getMetrics();
    }

    /**
     * Initiates graceful shutdown of this worker.
     *
     * <p>This method performs two key actions:
     * <ul>
     *   <li>Sets the shutdown flag to signal the retry mechanism to stop</li>
     *   <li>Stops the underlying maintenance routine</li>
     * </ul>
     *
     * <p>The shutdown flag ensures that:
     * <ul>
     *   <li>No new routine executions will be started</li>
     *   <li>The retry loop will exit on the next iteration</li>
     *   <li>The worker will terminate cleanly</li>
     * </ul>
     *
     * <p>This method is typically called by the watchdog when:
     * <ul>
     *   <li>The worker is identified as stale</li>
     *   <li>The watchdog itself is shutting down</li>
     *   <li>The task has reached a terminal state</li>
     * </ul>
     *
     * <p>The method is thread-safe and can be called from any thread.
     */
    public void shutdown() {
        shutdown = true;
        routine.stop();
    }
}
