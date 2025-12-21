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

import com.apple.foundationdb.*;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.internal.task.TaskStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import static com.kronotop.bucket.BucketMetadataUtil.NULL_BYTES;


/**
 * Utility class for managing index maintenance task operations in FoundationDB.
 * Provides methods for opening task subspaces and listing task identifiers.
 *
 * @see IndexBuildingTask
 * @see IndexDropTask
 * @see com.kronotop.internal.task.TaskStorage
 */
public class IndexTaskUtil {
    /**
     * Opens the DirectorySubspace for index maintenance tasks for a specific shard.
     * <p>
     * This method retrieves the task subspace from the directory cache using the standard
     * Kronotop directory layout for shard-level index maintenance tasks. The subspace must
     * have been previously created during cluster initialization.
     *
     * <p>The directory path follows the structure:
     * <pre>
     * kronotop/{cluster}/metadata/shards/bucket/shard/{shardId}/maintenance/index/tasks
     * </pre>
     *
     * <p>Each shard maintains its own task queue in this subspace, allowing parallel
     * index maintenance operations across all shards in the cluster.
     *
     * @param context the application context providing access to cluster configuration and directory cache
     * @param shardId the ID of the shard whose task subspace should be opened
     * @return the DirectorySubspace for the shard's index maintenance tasks
     * @see com.kronotop.internal.task.TaskStorage
     * @see KronotopDirectory
     */
    public static DirectorySubspace openTasksSubspace(Context context, int shardId) {
        // Task subspace has already been created during initialization
        List<String> layout = KronotopDirectory.
                kronotop().cluster(context.getClusterName()).metadata().
                shards().bucket().shard(shardId).maintenance().
                index().tasks().toList();
        return context.getDirectorySubspaceCache().get(layout);
    }

    /**
     * Retrieves all task identifiers associated with a specific index.
     * <p>
     * This method scans the index subspace for task entries and returns their versionstamp
     * identifiers. Tasks are created during index operations such as background index building
     * and are stored in the index subspace with the {@link IndexSubspaceMagic#TASKS} prefix.
     *
     * @param tx        the transactional context providing access to both the transaction and application context
     * @param namespace the namespace containing the bucket
     * @param bucket    the name of the bucket containing the index
     * @param index     the name of the index whose tasks should be listed
     * @return a list of versionstamp identifiers for all tasks associated with the index
     * @see IndexUtil#create(TransactionalContext, String, String, com.kronotop.bucket.index.IndexDefinition)
     * @see IndexSubspaceMagic#TASKS
     */
    public static List<Versionstamp> getTaskIds(TransactionalContext tx, String namespace, String bucket, String index) {
        BucketMetadata metadata = BucketMetadataUtil.open(tx.context(), tx.tr(), namespace, bucket);
        DirectorySubspace indexSubspace = IndexUtil.open(tx.tr(), metadata.subspace(), index);

        // Check that a task key exists in the index subspace
        byte[] taskKeyPrefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.TASKS.getValue()));
        byte[] taskKeyEnd = ByteArrayUtil.strinc(taskKeyPrefix);

        List<Versionstamp> taskIds = new ArrayList<>();
        KeySelector begin = KeySelector.firstGreaterThan(taskKeyPrefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(taskKeyEnd);
        for (KeyValue kv : tx.tr().getRange(begin, end)) {
            Tuple unpacked = indexSubspace.unpack(kv.getKey());
            Versionstamp taskId = (Versionstamp) unpacked.get(1);
            taskIds.add(taskId);
        }
        return taskIds;
    }

    /**
     * Removes task back pointer from index subspace.
     *
     * <p>Clears all keys with prefix TASKS/{taskId} using range clear operation.
     * This removes the back pointer marker created during task creation.
     *
     * @param tr            transaction for clear operation
     * @param indexSubspace index subspace containing the back pointer
     * @param taskId        task identifier
     */
    public static void clearTaskBackPointer(Transaction tr, DirectorySubspace indexSubspace, Versionstamp taskId) {
        byte[] prefix = indexSubspace.pack(Tuple.from(
                IndexSubspaceMagic.TASKS.getValue(), taskId
        ));
        tr.clear(Range.startsWith(prefix));
    }

    /**
     * Creates a task back pointer in the index subspace using versionstamped keys.
     *
     * <p>Back pointers establish a link from the index subspace to tasks in shard-specific
     * task subspaces. They enable efficient validation of task completion status and index
     * readiness checking without scanning all shard task queues.
     *
     * <p><strong>Key Structure:</strong>
     * <pre>
     * Key: TASKS/{versionstamp}/{shardId}
     * Tuple: [TASKS, Versionstamp.incomplete(userVersion), shardId]
     * Value: Empty (NULL_BYTES)
     * </pre>
     *
     * <p><strong>Versionstamp Mechanics:</strong> The incomplete versionstamp is replaced
     * by FoundationDB with the actual transaction versionstamp during commit, ensuring
     * the taskId matches the task definition created in the same transaction.
     *
     * <p>Used by {@link IndexUtil#createIndexBuildingTasks} to establish back pointers
     * when creating BUILD tasks across multiple shards.
     *
     * @param tr            transaction for versionstamped mutation
     * @param indexSubspace index subspace where back pointer is stored
     * @param userVersion   user-defined component of the incomplete versionstamp
     * @param shardId       shard identifier for the task
     * @see #scanTaskBackPointers(Transaction, DirectorySubspace, BiFunction)
     * @see #clearTaskBackPointer(Transaction, DirectorySubspace, Versionstamp)
     */
    public static void setBackPointer(Transaction tr, DirectorySubspace indexSubspace, int userVersion, int shardId) {
        byte[] backPointer = indexSubspace.packWithVersionstamp(
                Tuple.from(IndexSubspaceMagic.TASKS.getValue(), Versionstamp.incomplete(userVersion), shardId)
        );
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, backPointer, NULL_BYTES);
    }

    /**
     * Scans all task back pointers in an index subspace and invokes a callback for each.
     *
     * <p>Iterates through all entries under the TASKS/ prefix in the index subspace,
     * unpacks the versionstamp taskId and shard identifier, and invokes the provided
     * callback function. The callback can control iteration by returning true (continue)
     * or false (stop).
     *
     * <p><strong>Back Pointer Structure:</strong>
     * <pre>
     * Key: TASKS/{taskId}/{shardId}
     * Unpacked tuple: [TASKS, versionstamp, shardId]
     * Position 0: TASKS magic value (ignored)
     * Position 1: Versionstamp taskId
     * Position 2: Long shardId (cast to int)
     * </pre>
     *
     * <p><strong>Callback Semantics:</strong> The BiFunction receives (taskId, shardId)
     * and returns a boolean:
     * <ul>
     *   <li>true: Continue scanning next back pointer</li>
     *   <li>false: Stop scanning immediately</li>
     * </ul>
     *
     * <p><strong>Usage Example:</strong> {@link IndexUtil#markIndexAsReadyIfBuildDone}
     * uses this method to validate all BUILD tasks have completed before marking an
     * index as READY. It stops scanning early if any incomplete task is found.
     *
     * <p>Range scan: (TASKS/, strinc(TASKS/)] to retrieve all back pointers.
     *
     * @param tr            transaction for range scan operation
     * @param indexSubspace index subspace containing back pointers
     * @param action        callback function receiving (taskId, shardId) and returning true to continue
     * @see IndexUtil#markIndexAsReadyIfBuildDone(TransactionalContext, String, String, long)
     * @see #setBackPointer(Transaction, DirectorySubspace, int, int)
     * @see #clearTaskBackPointer(Transaction, DirectorySubspace, Versionstamp)
     */
    public static void scanTaskBackPointers(Transaction tr, DirectorySubspace indexSubspace, BiFunction<Versionstamp, Integer, Boolean> action) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.TASKS.getValue()));
        KeySelector begin = KeySelector.firstGreaterThan(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
        for (KeyValue keyValue : tr.getRange(begin, end)) {
            Tuple unpacked = indexSubspace.unpack(keyValue.getKey());
            Versionstamp taskId = (Versionstamp) unpacked.get(1);
            long shardId = (long) unpacked.get(2);
            if (!action.apply(taskId, (int) shardId)) {
                break;
            }
        }
    }

    /**
     * Clears all index tasks associated with a bucket.
     *
     * <p>This method iterates through all indexes in the bucket, scans their task back pointers,
     * and removes both the tasks from shard task subspaces and the back pointers from index subspaces.
     *
     * <p>Used during bucket purge to clean up orphaned tasks that would otherwise remain
     * in shard task subspaces after the bucket directory is removed.
     *
     * @param tx       transactional context providing access to transaction and application context
     * @param metadata bucket metadata containing the subspace to scan for indexes
     */
    public static void clearBucketTasks(TransactionalContext tx, BucketMetadata metadata) {
        Transaction tr = tx.tr();

        List<String> indexNames;
        try {
            indexNames = IndexUtil.list(tr, metadata.subspace());
        } catch (KronotopException e) {
            // No indexes directory exists, nothing to clean
            return;
        }

        for (String indexName : indexNames) {
            DirectorySubspace indexSubspace = IndexUtil.open(tr, metadata.subspace(), indexName);

            scanTaskBackPointers(tr, indexSubspace, (taskId, shardId) -> {
                DirectorySubspace taskSubspace = openTasksSubspace(tx.context(), shardId);
                TaskStorage.drop(tr, taskSubspace, taskId);
                clearTaskBackPointer(tr, indexSubspace, taskId);
                return true;
            });
        }
    }

    public static void changeTaskStatus(Transaction tr,
                                        DirectorySubspace taskSubspace,
                                        IndexMaintenanceTaskKind kind,
                                        Versionstamp taskId,
                                        IndexTaskStatus status
    ) {
        switch (kind) {
            case BOUNDARY -> {
                IndexBoundaryTaskState state = IndexBoundaryTaskState.load(tr, taskSubspace, taskId);
                if (state.status() == IndexTaskStatus.COMPLETED) {
                    throw new IllegalStateException("task is already completed");
                }
                if (state.status() == IndexTaskStatus.STOPPED) {
                    throw new IllegalStateException("cannot change status of already stopped task");
                }
                IndexBuildingTaskState.setStatus(tr, taskSubspace, taskId, status);
            }
        }
    }
}
