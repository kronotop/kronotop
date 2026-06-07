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
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import static com.kronotop.bucket.BucketMetadataUtil.NULL_BYTES;

public class IndexTaskUtil {
    private static final Random random = new Random(System.nanoTime());

    /**
     * Opens the directory subspace used to store index maintenance tasks for a given shard.
     *
     * @param context the application context
     * @param shardId the shard identifier
     * @return the directory subspace for index maintenance tasks
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
     * Retrieves all task IDs associated with an index by scanning the TASKS subspace within that index.
     *
     * @param tx        the transactional context
     * @param namespace the namespace containing the bucket
     * @param bucket    the bucket name
     * @param index     the index name
     * @return list of versionstamp task IDs found in the index
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
     * Clears the back-pointer entry for a specific task from the index subspace.
     *
     * @param tr            the FoundationDB transaction
     * @param indexSubspace the directory subspace of the index
     * @param taskId        the versionstamp identifying the task
     */
    public static void clearTaskBackPointer(Transaction tr, DirectorySubspace indexSubspace, Versionstamp taskId) {
        byte[] prefix = indexSubspace.pack(Tuple.from(
                IndexSubspaceMagic.TASKS.getValue(), taskId
        ));
        tr.clear(Range.startsWith(prefix));
    }

    /**
     * Writes a back-pointer into the index subspace that links the index to its maintenance task.
     * Uses an incomplete versionstamp that FoundationDB resolves at commit time.
     *
     * @param tr            the FoundationDB transaction
     * @param indexSubspace the directory subspace of the index
     * @param userVersion   the user version for the incomplete versionstamp
     * @param shardId       the shard where the task is stored
     */
    public static void setBackPointer(Transaction tr, DirectorySubspace indexSubspace, int userVersion, int shardId) {
        byte[] backPointer = indexSubspace.packWithVersionstamp(
                Tuple.from(IndexSubspaceMagic.TASKS.getValue(), Versionstamp.incomplete(userVersion), shardId)
        );
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, backPointer, NULL_BYTES);
    }

    /**
     * Scans all task back-pointers in the index subspace and invokes the given action for each one.
     * The action receives the task ID and shard ID, and returns {@code true} to continue scanning
     * or {@code false} to stop early.
     *
     * @param tr            the FoundationDB transaction
     * @param indexSubspace the directory subspace of the index
     * @param action        callback invoked per back-pointer; return false to stop iteration
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
     * Removes all index maintenance tasks for every index in the given bucket.
     * Drops each task from the task storage and clears its corresponding back-pointer.
     *
     * @param tx       the transactional context
     * @param metadata the bucket metadata identifying the bucket and its indexes
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

    /**
     * Checks whether the index has any active maintenance tasks that are not of kind BOUNDARY.
     *
     * @param tx            the transactional context
     * @param indexSubspace the directory subspace of the index
     * @return {@code true} if at least one active non-boundary task exists
     */
    public static boolean hasActiveNonBoundaryTasks(TransactionalContext tx, DirectorySubspace indexSubspace) {
        AtomicBoolean found = new AtomicBoolean();
        scanTaskBackPointers(tx.tr(), indexSubspace, (taskId, shardId) -> {
            DirectorySubspace taskSubspace = openTasksSubspace(tx.context(), shardId);
            byte[] definition = TaskStorage.getDefinition(tx.tr(), taskSubspace, taskId);
            if (definition == null) {
                return true;
            }
            IndexMaintenanceTask task = JSONUtil.readValue(definition, IndexMaintenanceTask.class);
            if (task.getKind() == IndexMaintenanceTaskKind.BOUNDARY) {
                return true;
            }
            found.set(true);
            return false;
        });
        return found.get();
    }

    /**
     * Schedules an index drop task on a randomly selected shard. Creates the task in the task storage,
     * sets a back-pointer in the index subspace, and triggers task watchers to pick it up.
     *
     * @param tx            the transactional context
     * @param metadata      the bucket metadata
     * @param indexId       the unique identifier of the index to drop
     * @param indexSubspace the directory subspace of the index
     */
    public static void scheduleDropTask(TransactionalContext tx, BucketMetadata metadata, long indexId, DirectorySubspace indexSubspace) {
        BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);

        IndexDropTask task = new IndexDropTask(metadata.namespace(), metadata.name(), indexId);
        byte[] definition = JSONUtil.writeValueAsBytes(task);

        int shardId = metadata.shards().get(random.nextInt(metadata.shards().size()));

        DirectorySubspace taskSubspace = openTasksSubspace(tx.context(), shardId);
        int userVersion = tx.getAndIncreaseUserVersion();
        TaskStorage.create(tx.tr(), userVersion, taskSubspace, definition);
        setBackPointer(tx.tr(), indexSubspace, userVersion, shardId);

        TaskStorage.triggerWatchers(tx.tr(), taskSubspace);
    }
}
