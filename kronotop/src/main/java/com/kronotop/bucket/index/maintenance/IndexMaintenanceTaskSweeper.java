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

/**
 * Manages the cleanup and finalization of completed index maintenance tasks across all shards.
 *
 * <p>This class is responsible for monitoring index building tasks that have completed on all
 * shards and performing the necessary cleanup operations. When all shards report a task as
 * COMPLETED, the sweeper:
 * <ul>
 *   <li>Updates the index status from BUILDING to READY in the metadata</li>
 *   <li>Removes all task-related data from FoundationDB</li>
 *   <li>Ensures a consistent state across the distributed system</li>
 * </ul>
 *
 * <p>The sweeper acts as a coordinator that ensures index building tasks are properly
 * finalized only when all participating shards have successfully completed their portion
 * of the work. This prevents premature index activation and maintains data consistency.
 *
 * <p>Task sweeping is performed atomically within a FoundationDB transaction to ensure
 * that either all cleanup operations succeed or none do, preventing partial state updates.
 *
 * @see IndexBuilderTask
 * @see IndexBuilderTaskState
 * @see IndexTaskStatus
 */
public class IndexMaintenanceTaskSweeper {
    private final Context context;
    private final int numShards;
    private final Map<Integer, DirectorySubspace> subspaces = new HashMap<>();

    /**
     * Constructs a new IndexMaintenanceTaskSweeper with the specified context.
     *
     * <p>Initializes the sweeper with the application context and determines the total
     * number of shards from the BucketService. The shard count is used to verify that
     * all shards have completed a task before performing cleanup operations.
     *
     * @param context the application context providing access to services and FoundationDB
     */
    public IndexMaintenanceTaskSweeper(Context context) {
        this.context = context;
        BucketService service = context.getService(BucketService.NAME);
        this.numShards = service.getNumberOfShards();
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
        // Fallback check for the completed tasks
        int numCompleted = 0;
        for (int shardId = 0; shardId < numShards; shardId++) {
            DirectorySubspace otherTaskSubspace = subspaces.computeIfAbsent(shardId,
                    (id) -> IndexTaskUtil.createOrOpenTasksSubspace(context, id));
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexBuilderTaskState state = IndexBuilderTaskState.load(tr, otherTaskSubspace, taskId);
                if (state.status() != IndexTaskStatus.COMPLETED) {
                    break;
                }
                numCompleted++;
            }
        }
        if (numCompleted != numShards) {
            return;
        }
        // READY
        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                byte[] taskDef = TaskStorage.getDefinition(tr, taskSubspace, taskId);
                if (taskDef == null) {
                    return;
                }
                IndexBuilderTask task = JSONUtil.readValue(taskDef, IndexBuilderTask.class);
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
                Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.READWRITE);
                if (index == null) {
                    dropIndexBuildingTaskPieces(tr, taskId);
                    tr.commit().join();
                } else {
                    IndexStatus status = index.definition().status();
                    if (status == IndexStatus.BUILDING) {
                        IndexDefinition definition = index.definition().updateStatus(IndexStatus.READY);
                        IndexUtil.saveIndexDefinition(tr, metadata, definition);
                        dropIndexBuildingTaskPieces(tr, taskId);
                        tr.commit().join();
                    }
                }
            }
        });
    }

    /**
     * Removes all task-related data from all shard subspaces.
     *
     * <p>This helper method iterates through all cached shard subspaces and removes
     * the task data associated with the given task ID from each one. This ensures
     * complete cleanup of index building task artifacts across all shards in the
     * distributed system.
     *
     * <p>The removal operations are performed within the provided transaction,
     * making them part of the atomic sweep operation. If the transaction fails,
     * no data is removed, maintaining consistency.
     *
     * @param tr     the FoundationDB transaction to use for the removal operations
     * @param taskId the unique versionstamp identifier of the task to remove
     */
    private void dropIndexBuildingTaskPieces(Transaction tr, Versionstamp taskId) {
        for (DirectorySubspace subspace : subspaces.values()) {
            TaskStorage.drop(tr, subspace, taskId);
        }
    }
}
