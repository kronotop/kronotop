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
import com.kronotop.Context;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.RetryMethods;
import com.kronotop.bucket.index.*;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;
import io.github.resilience4j.retry.Retry;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Coordinates the finalization and cleanup of completed index maintenance tasks across all shards.
 *
 * <p>IndexMaintenanceTaskSweeper acts as a distributed task coordinator that monitors index
 * maintenance tasks (building, dropping) and performs cleanup when all shards have completed
 * their work. This ensures atomic transitions of index states and prevents resource leaks.</p>
 *
 * <p><b>Primary Responsibilities:</b></p>
 * <ul>
 *   <li>Monitors task completion status across all shards</li>
 *   <li>Transitions indexes from BUILDING → READY when all shards complete</li>
 *   <li>Cleans up BUILD tasks when indexes are DROPPED</li>
 *   <li>Removes task metadata from all shard subspaces atomically</li>
 *   <li>Handles orphaned tasks (e.g., when index is dropped during build)</li>
 * </ul>
 *
 * <p><b>Coordination Logic:</b></p>
 * <p>The sweeper implements a barrier synchronization pattern:</p>
 * <ol>
 *   <li>Triggered when a shard marks a task as COMPLETED</li>
 *   <li>Checks if ALL shards (0..numShards-1) report COMPLETED status</li>
 *   <li>If yes: Updates index status and cleans up task data atomically</li>
 *   <li>If no: Returns without action (other shards still working)</li>
 * </ol>
 *
 * <p><b>Index Status Transitions:</b></p>
 * <ul>
 *   <li><b>BUILDING → READY:</b> When all shards complete BUILD tasks successfully</li>
 *   <li><b>BUILDING + DROPPED:</b> Cleans up BUILD tasks, leaves DROP tasks for separate processing</li>
 *   <li><b>Index deleted:</b> Cleans up all task types (BUILD, DROP) to prevent leaks</li>
 * </ul>
 *
 * <p><b>Atomicity Guarantees:</b></p>
 * <p>All sweep operations are performed within a single FoundationDB transaction:</p>
 * <ul>
 *   <li>Index status update</li>
 *   <li>Task deletion across all shard subspaces</li>
 *   <li>Either all changes commit or none do (no partial state)</li>
 * </ul>
 *
 * <p><b>Retry Behavior:</b></p>
 * <p>The sweeper uses retry logic to handle transient FoundationDB conflicts during
 * the final transaction (reading task state, updating index, deleting tasks). This ensures
 * eventual completion despite concurrent operations.</p>
 *
 * <p><b>Example Workflow:</b></p>
 * <pre>{@code
 * // Shard 0 completes its BUILD task
 * taskState.updateStatus(IndexTaskStatus.COMPLETED);
 * // Triggers sweeper
 * sweeper.sweep(taskSubspace, taskId);
 * // Sweeper checks: Shard 0=COMPLETED, Shard 1=PROCESSING, Shard 2=PROCESSING
 * // Result: Returns without action
 *
 * // Later, Shard 1 completes
 * taskState.updateStatus(IndexTaskStatus.COMPLETED);
 * sweeper.sweep(taskSubspace, taskId);
 * // Sweeper checks: Shard 0=COMPLETED, Shard 1=COMPLETED, Shard 2=PROCESSING
 * // Result: Returns without action
 *
 * // Finally, Shard 2 completes
 * taskState.updateStatus(IndexTaskStatus.COMPLETED);
 * sweeper.sweep(taskSubspace, taskId);
 * // Sweeper checks: Shard 0=COMPLETED, Shard 1=COMPLETED, Shard 2=COMPLETED
 * // Result: Updates index BUILDING→READY, deletes all task data
 * }</pre>
 *
 * <p><b>Thread Safety:</b></p>
 * <p>This class is NOT thread-safe and should be used by a single background thread
 * (typically IndexMaintenanceWatchDog). Concurrent sweep operations on the same task
 * are safe due to FoundationDB transaction isolation, but may cause unnecessary retries.</p>
 *
 * @see IndexBuildingTask
 * @see IndexBuildingTaskState
 * @see IndexTaskStatus
 * @see IndexMaintenanceWatchDog
 */
public class IndexMaintenanceTaskSweeper {
    /**
     * Application context providing access to FoundationDB and BucketService.
     */
    private final Context context;

    /**
     * Total number of shards in the cluster (used for barrier synchronization).
     */
    private final int numShards;

    /**
     * Cache of shard ID to task subspace mappings to avoid repeated directory opens.
     */
    private final Map<Integer, DirectorySubspace> subspaces = new HashMap<>();

    /**
     * Constructs a new IndexMaintenanceTaskSweeper with the specified context.
     *
     * <p>Initializes the sweeper with the application context and determines the total
     * number of shards from the BucketService. The shard count is critical for the
     * barrier synchronization logic - the sweeper must verify that all N shards have
     * completed a task before finalizing it.</p>
     *
     * <p><b>Initialization:</b></p>
     * <ul>
     *   <li>Retrieves shard count from BucketService configuration</li>
     *   <li>Initializes empty subspace cache (populated lazily during sweep operations)</li>
     * </ul>
     *
     * @param context the application context providing access to services and FoundationDB
     */
    public IndexMaintenanceTaskSweeper(Context context) {
        this.context = context;
        BucketService service = context.getService(BucketService.NAME);
        this.numShards = service.getNumberOfShards();
    }

    /**
     * Checks if a task has been completed on all shards (barrier verification).
     *
     * <p>This method implements the core barrier synchronization logic by checking
     * the task state on every shard in the cluster. The task is considered fully
     * completed only when ALL shards report {@link IndexTaskStatus#COMPLETED}.</p>
     *
     * <p><b>Algorithm:</b></p>
     * <ol>
     *   <li>Initialize completion counter to 0</li>
     *   <li>Iterate through all shards (0..numShards-1)</li>
     *   <li>For each shard:
     *     <ul>
     *       <li>Load task state from the shard's task subspace</li>
     *       <li>If task state is null (missing): return false immediately</li>
     *       <li>If status != COMPLETED: break loop and return false</li>
     *       <li>If status == COMPLETED: increment counter and continue</li>
     *     </ul>
     *   </li>
     *   <li>Return true if numCompleted == numShards, false otherwise</li>
     * </ol>
     *
     * <p><b>Early Exit Optimization:</b></p>
     * <p>The method exits immediately in two cases:</p>
     * <ul>
     *   <li>When a shard is missing task state (returns false)</li>
     *   <li>When a shard has non-COMPLETED status (breaks loop, returns false)</li>
     * </ul>
     * <p>This avoids unnecessary reads from remaining shards when the outcome is already determined.</p>
     *
     * <p><b>Return Value:</b></p>
     * <ul>
     *   <li><b>true:</b> All shards have COMPLETED status (safe to finalize index)</li>
     *   <li><b>false:</b> At least one shard is not COMPLETED or missing (must wait)</li>
     * </ul>
     *
     * @param taskId the versionstamp identifier of the task to check
     * @return true if all shards have completed the task, false otherwise
     */
    private boolean isCompleted(Versionstamp taskId) {
        int numCompleted = 0;
        for (int shardId = 0; shardId < numShards; shardId++) {
            DirectorySubspace otherTaskSubspace = subspaces.computeIfAbsent(shardId,
                    (id) -> IndexTaskUtil.createOrOpenTasksSubspace(context, id));
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, otherTaskSubspace, taskId);
                if (state.status() != IndexTaskStatus.COMPLETED) {
                    break;
                }
                numCompleted++;
            }
        }
        return numCompleted == numShards;
    }

    /**
     * Sweeps and finalizes a completed index maintenance task across all shards.
     *
     * <p>This method performs a coordinated cleanup of an index building task by:
     * <ol>
     *   <li>Checking if the task is COMPLETED on all shards (fallback verification)</li>
     *   <li>If all shards have completed, retrieving the task definition</li>
     *   <li>Loading the bucket metadata and target index</li>
     *   <li>Updating the index status from BUILDING to READY if applicable</li>
     *   <li>Removing all task-related data from all shard subspaces</li>
     * </ol>
     *
     * <p>The sweep operation is atomic - either all changes are committed or none are.
     * This ensures the index is only marked as READY when fully built across all shards,
     * and that cleanup is consistent across the distributed system.
     *
     * <p>If the index no longer exists in the metadata (e.g., it was dropped), the method
     * still performs cleanup of task data to prevent storage leaks.
     *
     * <p>The method uses retry logic to handle transient FoundationDB conflicts during
     * the final update and cleanup transaction.
     *
     * @param taskSubspace the directory subspace containing the task definition
     * @param taskId       the unique versionstamp identifier of the task to sweep
     */
    public void sweep(DirectorySubspace taskSubspace, Versionstamp taskId) {
        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(() -> doSweep(taskSubspace, taskId));
    }

    /**
     * Performs the actual sweep operation within a transaction (called by retry wrapper).
     *
     * <p>This method executes the complete sweep workflow:</p>
     * <ol>
     *   <li>Loads the task definition from the task subspace</li>
     *   <li>Retrieves bucket metadata and the target index</li>
     *   <li>Handles three cases based on index state:
     *     <ul>
     *       <li><b>Index deleted:</b> Cleans up all task types (BUILD, DROP)</li>
     *       <li><b>Index BUILDING:</b> Verifies all shards completed, then transitions to READY</li>
     *       <li><b>Index DROPPED:</b> Cleans up BUILD tasks only (DROP tasks handled separately)</li>
     *     </ul>
     *   </li>
     *   <li>Commits the transaction atomically</li>
     * </ol>
     *
     * <p><b>Case 1: Index Deleted (index == null)</b></p>
     * <p>When an index is dropped and removed from metadata during an active build:</p>
     * <ul>
     *   <li>Cleanup both BUILD and DROP tasks to prevent resource leaks</li>
     *   <li>This handles the race condition where index is deleted before sweep runs</li>
     * </ul>
     *
     * <p><b>Case 2: Index BUILDING</b></p>
     * <p>Normal completion path for index builds:</p>
     * <ul>
     *   <li>Verifies ALL shards report COMPLETED (barrier check)</li>
     *   <li>If not all completed: return early without changes</li>
     *   <li>If all completed: Update index status BUILDING → READY</li>
     *   <li>Delete BUILD task metadata from all shard subspaces</li>
     * </ul>
     *
     * <p><b>Case 3: Index DROPPED</b></p>
     * <p>When index is dropped while BUILD tasks still exist:</p>
     * <ul>
     *   <li>Clean up BUILD tasks (no longer needed)</li>
     *   <li>Leave DROP tasks intact (processed by separate cleanup logic)</li>
     * </ul>
     *
     * <p><b>Transaction Isolation:</b></p>
     * <p>All operations within this method execute in a single FoundationDB transaction,
     * ensuring atomic visibility of index status changes and task cleanup across the cluster.</p>
     *
     * @param taskSubspace the directory subspace containing the task definition
     * @param taskId       the versionstamp identifier of the task to sweep
     */
    private void doSweep(DirectorySubspace taskSubspace, Versionstamp taskId) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] taskDef = TaskStorage.getDefinition(tr, taskSubspace, taskId);
            if (taskDef == null) {
                return;
            }
            IndexBuildingTask task = JSONUtil.readValue(taskDef, IndexBuildingTask.class);
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
            Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
            if (index == null) {
                dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.BUILD, IndexMaintenanceTaskKind.DROP);
            } else {
                IndexStatus status = index.definition().status();
                if (status == IndexStatus.BUILDING) {
                    if (!isCompleted(taskId)) {
                        return;
                    }
                    IndexDefinition definition = index.definition().updateStatus(IndexStatus.READY);
                    IndexUtil.saveIndexDefinition(tr, metadata, definition);
                    dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.BUILD);
                } else if (status == IndexStatus.DROPPED) {
                    // Drop the BUILD tasks if there is any.
                    // The DROP tasks will be dropped separately.
                    dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.BUILD);
                }
            }
            tr.commit().join();
        }
    }

    /**
     * Deletes task metadata from all shard subspaces for the specified task kinds.
     *
     * <p>This method iterates through all shards and removes task entries that match
     * any of the specified task kinds (BUILD, DROP). It's used during cleanup to ensure
     * task metadata is completely removed from the distributed system.</p>
     *
     * <p><b>Operation:</b></p>
     * <ol>
     *   <li>For each shard (0..numShards-1):
     *     <ul>
     *       <li>Open or retrieve cached task subspace</li>
     *       <li>Load task definition</li>
     *       <li>Check if task kind matches any of the specified kinds</li>
     *       <li>If match: Delete task using TaskStorage.drop()</li>
     *     </ul>
     *   </li>
     * </ol>
     *
     * <p><b>Example Usage:</b></p>
     * <pre>{@code
     * // Remove only BUILD tasks
     * dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.BUILD);
     *
     * // Remove both BUILD and DROP tasks (orphaned index cleanup)
     * dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.BUILD, IndexMaintenanceTaskKind.DROP);
     * }</pre>
     *
     * <p><b>Transaction Context:</b></p>
     * <p>This method MUST be called within an active transaction. All deletions are
     * batched within the provided transaction and committed atomically by the caller.</p>
     *
     * @param tr     the active transaction to use for task deletion
     * @param taskId the versionstamp identifier of the task to drop
     * @param kinds  variable-length array of task kinds to delete (BUILD, DROP, etc.)
     */
    private void dropIndexMaintenanceTask(Transaction tr, Versionstamp taskId, IndexMaintenanceTaskKind... kinds) {
        for (int shardId = 0; shardId < numShards; shardId++) {
            DirectorySubspace subspace = subspaces.computeIfAbsent(shardId,
                    (id) -> IndexTaskUtil.createOrOpenTasksSubspace(context, id));
            byte[] definition = TaskStorage.getDefinition(tr, subspace, taskId);
            if (definition == null) {
                continue;
            }
            IndexMaintenanceTask task = JSONUtil.readValue(definition, IndexMaintenanceTask.class);
            boolean exists = Stream.of(kinds).anyMatch(k -> k == task.getKind());
            if (exists) {
                TaskStorage.drop(tr, subspace, taskId);
            }
        }
    }
}
