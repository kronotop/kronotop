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
import com.kronotop.Context;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.RetryMethods;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.index.statistics.IndexAnalyzeTask;
import com.kronotop.bucket.index.statistics.IndexAnalyzeTaskState;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;
import io.github.resilience4j.retry.Retry;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Removes completed or orphaned index maintenance tasks and triggers index readiness checks.
 *
 * <p>The sweeper deletes task definitions from task subspaces and clears their back pointers
 * from index subspaces. Cleanup depends on task kind:
 * <ul>
 *   <li><strong>BOUNDARY / ANALYZE:</strong> removed when COMPLETED, STOPPED, or the index is gone</li>
 *   <li><strong>BUILD:</strong> removed when COMPLETED, STOPPED, or the index is gone, then the
 *       readiness check for the index type (single-field, compound, or vector) runs in a second
 *       transaction to attempt the BUILDING to READY transition</li>
 *   <li><strong>DROP:</strong> removed when the index is gone from metadata, or the task is in a
 *       terminal state and the index directory no longer exists in FoundationDB</li>
 * </ul>
 *
 * <p>The sweeper does not check shard completion itself. The readiness check scans back pointers
 * and validates remaining tasks before marking the index READY. When an index is deleted, all its
 * tasks across every shard are removed. Transactions use {@link RetryMethods} to retry on conflict.
 *
 * @see SingleFieldIndexUtil#markIndexAsReadyIfBuildDone
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
     * Cache of shard ID to task subspace mappings to avoid repeated directory opens.
     */
    private final Map<Integer, DirectorySubspace> subspaces = new ConcurrentHashMap<>();

    /**
     * Creates a sweeper with the specified application context.
     *
     * @param context application context with FoundationDB and BucketService access
     */
    public IndexMaintenanceTaskSweeper(Context context) {
        this.context = context;
    }

    /**
     * Looks up the index subspace by type and ID from bucket metadata.
     *
     * @param metadata  bucket metadata containing index registries
     * @param indexType type of index (SINGLE_FIELD, COMPOUND, VECTOR)
     * @param indexId   index identifier
     * @return index subspace, or null if the index does not exist
     */
    private static DirectorySubspace findIndexSubspace(BucketMetadata metadata, IndexType indexType, long indexId) {
        return switch (indexType) {
            case COMPOUND -> {
                CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(indexId, IndexSelectionPolicy.ALL);
                yield compoundIndex != null ? compoundIndex.subspace() : null;
            }
            case VECTOR -> {
                VectorIndex vectorIndex = metadata.vectorIndexes().getIndexById(indexId, IndexSelectionPolicy.ALL);
                yield vectorIndex != null ? vectorIndex.subspace() : null;
            }
            default -> {
                SingleFieldIndex index = metadata.singleFieldIndexes().getIndexById(indexId, IndexSelectionPolicy.ALL);
                yield index != null ? index.subspace() : null;
            }
        };
    }

    /**
     * Sweeps a single task, retrying on FoundationDB conflict.
     *
     * <p>Wraps {@link #doSweep} with retry handling.
     *
     * @param taskSubspace directory subspace containing the task definition
     * @param taskId       versionstamp identifier of the task to sweep
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
     * @param tr      transaction for cleanup operations
     * @param taskId  task identifier
     * @param taskDef serialized BUILD task definition
     */
    private void sweepBuildTask(Transaction tr, Versionstamp taskId, byte[] taskDef) {
        IndexBuildingTask task = JSONUtil.readValue(taskDef, IndexBuildingTask.class);
        BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
        DirectorySubspace indexSubspace = findIndexSubspace(metadata, task.getIndexType(), task.getIndexId());

        if (indexSubspace == null) {
            dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.BUILD);
        } else {
            DirectorySubspace taskSubspace = getOrOpenTaskSubspace(task.getShardId());
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);
            if (state.status() == IndexTaskStatus.COMPLETED || state.status() == IndexTaskStatus.STOPPED) {
                dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.BUILD);
                IndexTaskUtil.clearTaskBackPointer(tr, indexSubspace, taskId);
            }
        }
    }

    /**
     * Handles DROP task cleanup.
     *
     * <p>Removes the DROP task if: index deleted from metadata, or task is in a terminal
     * state (COMPLETED/STOPPED) and the index directory no longer exists in FoundationDB.
     * Returns early if task is still running or the directory still exists.
     *
     * @param tr           transaction for cleanup operations
     * @param taskSubspace directory subspace containing the task state
     * @param taskId       task identifier
     * @param taskDef      serialized DROP task definition
     */
    private void sweepDropTask(Transaction tr, DirectorySubspace taskSubspace, Versionstamp taskId, byte[] taskDef) {
        IndexDropTask task = JSONUtil.readValue(taskDef, IndexDropTask.class);
        BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());

        // Search all three index registries for the target index.
        IndexDefinition definition = null;
        SingleFieldIndex index = metadata.singleFieldIndexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
        if (index != null) {
            definition = index.definition();
        } else {
            CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
            if (compoundIndex != null) {
                definition = compoundIndex.definition();
            } else {
                VectorIndex vectorIndex = metadata.vectorIndexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
                if (vectorIndex != null) {
                    definition = vectorIndex.definition();
                }
            }
        }

        if (definition == null) {
            // Index fully cleaned from metadata, so drop the orphaned task.
            dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.DROP);
            return;
        }

        if (definition.status() != IndexStatus.DROPPED) {
            // silently quit
            return;
        }
        IndexDropTaskState state = IndexDropTaskState.load(tr, taskSubspace, taskId);
        if (state.status() != IndexTaskStatus.COMPLETED && state.status() != IndexTaskStatus.STOPPED) {
            return;
        }
        try {
            IndexUtil.open(tr, metadata.subspace(), definition.name());
        } catch (NoSuchIndexException exp) {
            dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.DROP);
        }
    }

    /**
     * Handles BOUNDARY task cleanup.
     *
     * <p>Removes BOUNDARY task if: index deleted (null), task COMPLETED, or task STOPPED.
     * Clears back pointer when task is removed.
     *
     * @param tr      transaction for cleanup operations
     * @param taskId  task identifier
     * @param taskDef serialized BOUNDARY task definition
     */
    private void sweepBoundaryTask(Transaction tr, Versionstamp taskId, byte[] taskDef) {
        IndexBoundaryTask task = JSONUtil.readValue(taskDef, IndexBoundaryTask.class);
        BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
        DirectorySubspace indexSubspace = findIndexSubspace(metadata, task.getIndexType(), task.getIndexId());

        if (indexSubspace == null) {
            dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.BOUNDARY);
        } else {
            DirectorySubspace taskSubspace = getOrOpenTaskSubspace(task.getShardId());
            IndexBoundaryTaskState state = IndexBoundaryTaskState.load(tr, taskSubspace, taskId);
            if (state.status() == IndexTaskStatus.COMPLETED || state.status() == IndexTaskStatus.STOPPED) {
                dropIndexMaintenanceTask(tr, taskId, IndexMaintenanceTaskKind.BOUNDARY);
                IndexTaskUtil.clearTaskBackPointer(tr, indexSubspace, taskId);
            }
        }
    }

    /**
     * Handles ANALYZE task cleanup.
     *
     * <p>Removes ANALYZE task if: index deleted (null), task COMPLETED, or task STOPPED.
     * Clears back pointer when task is removed.
     *
     * @param tr      transaction for cleanup operations
     * @param taskId  task identifier
     * @param taskDef serialized ANALYZE task definition
     */
    private void sweepAnalyzeTask(Transaction tr, Versionstamp taskId, byte[] taskDef) {
        IndexAnalyzeTask task = JSONUtil.readValue(taskDef, IndexAnalyzeTask.class);
        BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
        SingleFieldIndex index = metadata.singleFieldIndexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
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
     * Loads the task definition and routes it to the handler for its kind.
     *
     * <p>Commits the cleanup transaction, then attempts the index READY transition for BUILD
     * tasks in a separate transaction.
     *
     * @param taskSubspace directory subspace containing the task definition
     * @param taskId       versionstamp identifier of the task to sweep
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
                    sweepDropTask(tr, taskSubspace, taskId, definition);
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
                IndexBuildingTask buildTask = JSONUtil.readValue(definition, IndexBuildingTask.class);
                if (buildTask.getIndexType() == IndexType.COMPOUND) {
                    RetryMethods.retry(RetryMethods.TRANSACTION).executeRunnable(() -> tryMarkCompoundIndexAsReady(definition));
                } else if (buildTask.getIndexType() == IndexType.VECTOR) {
                    RetryMethods.retry(RetryMethods.TRANSACTION).executeRunnable(() -> tryMarkVectorIndexAsReady(definition));
                } else {
                    RetryMethods.retry(RetryMethods.TRANSACTION).executeRunnable(() -> tryMarkSingleFieldIndexAsReady(definition));
                }
            }
        }
    }

    /**
     * Attempts to mark a single field index as READY after BUILD task cleanup.
     *
     * <p>Called in separate transaction after removing the BUILD task. Delegates to
     * {@link SingleFieldIndexUtil#markIndexAsReadyIfBuildDone} which validates all tasks
     * complete before transitioning index to READY status.
     *
     * @param definition serialized BUILD task definition
     */
    private void tryMarkSingleFieldIndexAsReady(byte[] definition) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            IndexBuildingTask task = JSONUtil.readValue(definition, IndexBuildingTask.class);
            if (SingleFieldIndexUtil.markIndexAsReadyIfBuildDone(tx, task.getNamespace(), task.getBucket(), task.getIndexId())) {
                tr.commit().join();
            }
        }
    }

    /**
     * Attempts to mark the vector index as READY after BUILD task cleanup.
     *
     * @param definition serialized vector BUILD task definition
     */
    private void tryMarkVectorIndexAsReady(byte[] definition) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            IndexBuildingTask task = JSONUtil.readValue(definition, IndexBuildingTask.class);
            if (VectorIndexUtil.markVectorIndexAsReadyIfBuildDone(tx, task.getNamespace(), task.getBucket(), task.getIndexId())) {
                tr.commit().join();
            }
        }
    }

    /**
     * Attempts to mark the compound index as READY after BUILD task cleanup.
     *
     * @param definition serialized compound BUILD task definition
     */
    private void tryMarkCompoundIndexAsReady(byte[] definition) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            IndexBuildingTask task = JSONUtil.readValue(definition, IndexBuildingTask.class);
            if (CompoundIndexUtil.markCompoundIndexAsReadyIfBuildDone(tx, task.getNamespace(), task.getBucket(), task.getIndexId())) {
                tr.commit().join();
            }
        }
    }

    /**
     * Removes the task from every bucket shard subspace where its kind matches one of the
     * given kinds.
     *
     * <p>Scans all bucket shards and drops the task via {@link TaskStorage#drop}. Must run
     * within an active transaction.
     *
     * @param tr     transaction for task deletion
     * @param taskId versionstamp identifier of the task to drop
     * @param kinds  task kinds to delete (BUILD, DROP, BOUNDARY, ANALYZE)
     */
    private void dropIndexMaintenanceTask(Transaction tr, Versionstamp taskId, IndexMaintenanceTaskKind... kinds) {
        List<Integer> shardIds = context.getShardRegistry().getShardIds(ShardKind.BUCKET);
        for (int shardId : shardIds) {
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

    private DirectorySubspace getOrOpenTaskSubspace(int shardId) {
        return subspaces.computeIfAbsent(shardId,
                (id) -> IndexTaskUtil.openTasksSubspace(context, id));
    }
}
