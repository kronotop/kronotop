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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.RetryMethods;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.index.statistics.IndexAnalyzeTask;
import com.kronotop.bucket.index.statistics.IndexAnalyzeTaskState;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;
import io.github.resilience4j.retry.Retry;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Cleans up completed or orphaned index maintenance tasks and triggers index readiness checks.
 *
 * <p>IndexMaintenanceTaskSweeper removes completed task definitions from task subspaces and
 * their back pointers from index subspaces. For BUILD tasks, it triggers index readiness
 * validation via {@link IndexUtil#markIndexAsReadyIfBuildDone}.
 *
 * <p><strong>Core Responsibilities:</strong>
 * <ul>
 *   <li>Remove COMPLETED/STOPPED tasks from task subspaces</li>
 *   <li>Clear task back pointers from index subspaces</li>
 *   <li>Trigger index BUILDING → READY transition attempts</li>
 *   <li>Clean up orphaned tasks when indexes are deleted</li>
 * </ul>
 *
 * <p><strong>Task Processing by Type:</strong>
 * <ul>
 *   <li><strong>BOUNDARY:</strong> Remove if COMPLETED/STOPPED or index deleted</li>
 *   <li><strong>BUILD:</strong> Remove if COMPLETED/STOPPED or index deleted, then attempt index READY transition</li>
 *   <li><strong>DROP:</strong> Remove if index deleted or index directory removed from FoundationDB</li>
 *   <li><strong>ANALYZE:</strong> Remove if COMPLETED/STOPPED or index deleted</li>
 * </ul>
 *
 * <p><strong>Workflow:</strong>
 * <ol>
 *   <li>Load task definition from task subspace</li>
 *   <li>Dispatch to type-specific handler (sweepBuildTask, sweepDropTask, etc.)</li>
 *   <li>Check task completion status or index existence</li>
 *   <li>Remove task from all shard subspaces via {@link #dropIndexMaintenanceTask}</li>
 *   <li>Clear back pointer from index subspace</li>
 *   <li>For BUILD tasks: Separately call {@link IndexUtil#markIndexAsReadyIfBuildDone}</li>
 * </ol>
 *
 * <p><strong>Index Readiness:</strong> The sweeper does NOT directly check if all shards are
 * complete. Instead, it delegates to {@link IndexUtil#markIndexAsReadyIfBuildDone}, which
 * scans all back pointers and validates remaining tasks before marking index READY.
 *
 * <p><strong>Orphaned Task Cleanup:</strong> When an index is deleted from bucket metadata,
 * the sweeper removes all associated tasks across all shards to prevent resource leaks.
 *
 * <p><strong>Atomicity:</strong> Each sweep operation runs in two transactions:
 * <ol>
 *   <li>First transaction: Remove task definitions and back pointers</li>
 *   <li>Second transaction (BUILD only): Attempt index READY transition</li>
 * </ol>
 *
 * <p><strong>Retry Behavior:</strong> Both transactions use {@link RetryMethods} to handle
 * FoundationDB conflicts automatically.
 *
 * @see IndexUtil#markIndexAsReadyIfBuildDone
 * @see IndexMaintenanceWatchDog
 * @see IndexBuildingTask
 * @see IndexDropTask
 * @see IndexBoundaryTask
 * @see IndexAnalyzeTask
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
     * Creates a sweeper with the specified application context.
     *
     * <p>Retrieves shard count from BucketService for iterating all task subspaces during
     * cleanup operations. Initializes empty subspace cache populated lazily.
     *
     * @param context application context with FoundationDB and BucketService access
     */
    public IndexMaintenanceTaskSweeper(Context context) {
        this.context = context;
        BucketService service = context.getService(BucketService.NAME);
        this.numShards = service.getNumberOfShards();
    }

    /**
     * Cleans up completed index maintenance tasks with retry logic.
     *
     * <p>Entry point that wraps {@link #doSweep} with transaction retry handling.
     * Removes task definitions and back pointers, then attempts index readiness
     * transition for BUILD tasks.
     *
     * @param taskSubspace directory subspace containing the task definition
     * @param taskId versionstamp identifier of the task to sweep
     */
    public void sweep(DirectorySubspace taskSubspace, Versionstamp taskId) {
        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(() -> doSweep(taskSubspace, taskId));
    }

    /**
     * Handles BUILD task cleanup.
     *
     * <p>Removes BUILD task if: index deleted (null), task COMPLETED, or task STOPPED.
     * Clears back pointer when task is removed.
     *
     * @param tr transaction for cleanup operations
     * @param taskId task identifier
     * @param taskDef serialized BUILD task definition
     */
    private void sweepBuildTask(Transaction tr, Versionstamp taskId, byte[] taskDef) {
        IndexBuildingTask task = JSONUtil.readValue(taskDef, IndexBuildingTask.class);
        BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
        Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
        if (index == null) {
            dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.BUILD);
        } else {
            DirectorySubspace taskSubspace = getOrOpenTaskSubspace(task.getShardId());
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);
            if (state.status() == IndexTaskStatus.COMPLETED || state.status() == IndexTaskStatus.STOPPED) {
                dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.BUILD);
                IndexTaskUtil.clearTaskBackPointer(tr, index.subspace(), taskId);
            }
        }
    }

    /**
     * Handles DROP task cleanup.
     *
     * <p>Removes DROP task if: index deleted from metadata, or index directory
     * removed from FoundationDB. Returns early if index still has DROPPED status
     * but directory exists (drop in progress).
     *
     * @param tr transaction for cleanup operations
     * @param taskId task identifier
     * @param taskDef serialized DROP task definition
     */
    private void sweepDropTask(Transaction tr, Versionstamp taskId, byte[] taskDef) {
        IndexDropTask task = JSONUtil.readValue(taskDef, IndexDropTask.class);
        BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
        Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
        if (index == null) {
            dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.DROP);
        } else {
            if (index.definition().status() != IndexStatus.DROPPED) {
                // silently quit
                return;
            }
            try {
                IndexUtil.open(tr, metadata.subspace(), index.definition().name());
            } catch (NoSuchIndexException exp) {
                dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.DROP);
            }
        }
    }

    /**
     * Handles BOUNDARY task cleanup.
     *
     * <p>Removes BOUNDARY task if: index deleted (null), task COMPLETED, or task STOPPED.
     * Clears back pointer when task is removed.
     *
     * @param tr transaction for cleanup operations
     * @param taskId task identifier
     * @param taskDef serialized BOUNDARY task definition
     */
    private void sweepBoundaryTask(Transaction tr, Versionstamp taskId, byte[] taskDef) {
        IndexBoundaryTask task = JSONUtil.readValue(taskDef, IndexBoundaryTask.class);
        BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
        Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
        if (index == null) {
            dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.BOUNDARY);
        } else {
            DirectorySubspace taskSubspace = getOrOpenTaskSubspace(task.getShardId());
            IndexBoundaryTaskState state = IndexBoundaryTaskState.load(tr, taskSubspace, taskId);
            if (state.status() == IndexTaskStatus.COMPLETED || state.status() == IndexTaskStatus.STOPPED) {
                dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.BOUNDARY);
                IndexTaskUtil.clearTaskBackPointer(tr, index.subspace(), taskId);
            }
        }
    }

    /**
     * Handles ANALYZE task cleanup.
     *
     * <p>Removes ANALYZE task if: index deleted (null), task COMPLETED, or task STOPPED.
     * Clears back pointer when task is removed.
     *
     * @param tr transaction for cleanup operations
     * @param taskId task identifier
     * @param taskDef serialized ANALYZE task definition
     */
    private void sweepAnalyzeTask(Transaction tr, Versionstamp taskId, byte[] taskDef) {
        IndexAnalyzeTask task = JSONUtil.readValue(taskDef, IndexAnalyzeTask.class);
        BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
        Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
        if (index == null) {
            dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.ANALYZE);
        } else {
            DirectorySubspace taskSubspace = getOrOpenTaskSubspace(task.getShardId());
            IndexAnalyzeTaskState state = IndexAnalyzeTaskState.load(tr, taskSubspace, taskId);
            if (state.status() == IndexTaskStatus.COMPLETED || state.status() == IndexTaskStatus.STOPPED) {
                dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.ANALYZE);
                IndexTaskUtil.clearTaskBackPointer(tr, index.subspace(), taskId);
            }
        }
    }

    /**
     * Main sweep logic that routes tasks to type-specific handlers.
     *
     * <p>Loads task definition, dispatches to appropriate handler based on kind,
     * commits cleanup transaction, then attempts index READY transition for BUILD tasks
     * in a separate transaction.
     *
     * @param taskSubspace directory subspace containing the task definition
     * @param taskId versionstamp identifier of the task to sweep
     */
    private void doSweep(DirectorySubspace taskSubspace, Versionstamp taskId) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] definition = TaskStorage.getDefinition(tr, taskSubspace, taskId);
            if (definition == null) {
                return;
            }
            IndexMaintenanceTask base = JSONUtil.readValue(definition, IndexMaintenanceTask.class);
            switch (base.getKind()) {
                case IndexMaintenanceTaskKind.BOUNDARY:
                    sweepBoundaryTask(tr, taskId, definition);
                    break;
                case IndexMaintenanceTaskKind.BUILD:
                    sweepBuildTask(tr, taskId, definition);
                    break;
                case IndexMaintenanceTaskKind.DROP:
                    sweepDropTask(tr, taskId, definition);
                    break;
                case IndexMaintenanceTaskKind.ANALYZE:
                    sweepAnalyzeTask(tr, taskId, definition);
                    break;
                default:
                    throw new IllegalStateException("Unknown index maintenance task kind: " + base.getKind());
            }
            tr.commit().join();

            // Committed successfully
            if (base.getKind() == IndexMaintenanceTaskKind.BUILD) {
                RetryMethods.retry(RetryMethods.TRANSACTION).executeRunnable(() -> tryMarkIndexAsReady(definition));
            }
        }
    }

    /**
     * Attempts to mark index as READY after BUILD task cleanup.
     *
     * <p>Called in separate transaction after removing BUILD task. Delegates to
     * {@link IndexUtil#markIndexAsReadyIfBuildDone} which validates all tasks
     * complete before transitioning index to READY status.
     *
     * @param definition serialized BUILD task definition
     */
    private void tryMarkIndexAsReady(byte[] definition) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            IndexBuildingTask task = JSONUtil.readValue(definition, IndexBuildingTask.class);
            if (IndexUtil.markIndexAsReadyIfBuildDone(tx, task.getNamespace(), task.getBucket(), task.getIndexId())) {
                tr.commit().join();
            }
        }
    }

    /**
     * Removes task definitions from all shard subspaces matching specified kinds.
     *
     * <p>Iterates through all shards (0..numShards-1), loads task definitions, and
     * removes tasks whose kind matches any of the specified kinds.
     *
     * <p><strong>Operation:</strong>
     * <ol>
     *   <li>For each shard: Open/retrieve cached task subspace</li>
     *   <li>Load task definition</li>
     *   <li>Check if task kind matches any specified kind</li>
     *   <li>If match: Delete via {@link TaskStorage#drop}</li>
     * </ol>
     *
     * <p>Must be called within active transaction. All deletions batched in transaction.
     *
     * @param tr transaction for task deletion
     * @param taskId versionstamp identifier of task to drop
     * @param kinds task kinds to delete (BUILD, DROP, BOUNDARY, ANALYZE)
     */
    private void dropIndexMaintenanceTask(Transaction tr, Versionstamp taskId, IndexMaintenanceTaskKind... kinds) {
        for (int shardId = 0; shardId < numShards; shardId++) {
            DirectorySubspace subspace = getOrOpenTaskSubspace(shardId);
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

    /**
     * Retrieves or lazily opens task subspace for a shard.
     *
     * @param shardId shard identifier
     * @return cached or newly opened task subspace
     */
    private DirectorySubspace getOrOpenTaskSubspace(int shardId) {
        return subspaces.computeIfAbsent(shardId,
                (id) -> IndexTaskUtil.openTasksSubspace(context, id));
    }
}
