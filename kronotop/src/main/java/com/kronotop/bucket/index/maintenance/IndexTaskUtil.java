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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.directory.KronotopDirectory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for managing index maintenance task subspaces and task counters in FoundationDB.
 * <p>
 * This class provides methods for:
 * <ul>
 *   <li>Opening task subspaces for specific shards</li>
 *   <li>Managing task counters to track task completion across shards</li>
 *   <li>Reading task counter values for monitoring purposes</li>
 * </ul>
 *
 * <p>Task counters are used to coordinate index maintenance operations across multiple shards.
 * When a task affects multiple shards, the counter tracks how many shards have completed the task,
 * enabling efficient coordination without requiring a central coordinator.
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
     * Atomically modifies a task counter by the specified delta value.
     * <p>
     * This method uses FoundationDB's atomic ADD mutation to increment or decrement
     * a counter associated with a specific task. The counter is stored in a global
     * maintenance subspace and is used to track task completion across multiple shards.
     *
     * <p>The directory path for counters follows the structure:
     * <pre>
     * kronotop/{cluster}/metadata/buckets/maintenance/index/counter
     * </pre>
     *
     * <p>Common use cases:
     * <ul>
     *   <li>Increment by +1 when a shard completes its portion of a task</li>
     *   <li>Decrement by -1 when a task needs to be retried</li>
     *   <li>Initialize counter when creating a distributed task</li>
     * </ul>
     *
     * <p><strong>Atomicity:</strong> This operation is atomic and thread-safe, using
     * FoundationDB's {@link MutationType#ADD} mutation. Multiple shards can safely modify
     * the same counter concurrently.
     *
     * @param context the application context providing access to cluster configuration
     * @param tr      the transaction in which to perform the atomic mutation
     * @param taskId  the versionstamp identifying the task whose counter should be modified
     * @param delta   the value to add to the counter (can be negative for decrement)
     * @see #readTaskCounter(Context, Versionstamp)
     */
    public static void modifyTaskCounter(Context context, Transaction tr, Versionstamp taskId, int delta) {
        List<String> layout = KronotopDirectory.
                kronotop().cluster(context.getClusterName()).metadata().
                buckets().maintenance().index().counter().toList();
        DirectorySubspace subspace = context.getDirectorySubspaceCache().get(layout);
        byte[] data = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(delta).array();
        byte[] key = subspace.pack(taskId);
        tr.mutate(MutationType.ADD, key, data);
    }

    /**
     * Reads the current value of a task counter.
     * <p>
     * This method retrieves the counter value for a specific task from the global
     * maintenance counter subspace. The counter tracks how many shards have completed
     * their portion of a distributed task.
     *
     * <p>The directory path for counters follows the structure:
     * <pre>
     * kronotop/{cluster}/metadata/buckets/maintenance/index/counter
     * </pre>
     *
     * <p>This method creates its own transaction for reading the counter value,
     * providing a consistent snapshot of the counter at the time of the read.
     *
     * <p><strong>Note:</strong> The returned value represents a point-in-time snapshot.
     * The actual counter value may change immediately after this method returns if other
     * shards are concurrently modifying it.
     *
     * @param context the application context providing access to cluster configuration and FoundationDB
     * @param taskId  the versionstamp identifying the task whose counter should be read
     * @return the current counter value as a 32-bit integer
     * @see #modifyTaskCounter(Context, Transaction, Versionstamp, int)
     */
    public static int readTaskCounter(Context context, Versionstamp taskId) {
        List<String> layout = KronotopDirectory.
                kronotop().cluster(context.getClusterName()).metadata().
                buckets().maintenance().index().counter().toList();
        DirectorySubspace subspace = context.getDirectorySubspaceCache().get(layout);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] key = subspace.pack(taskId);
            byte[] data = tr.get(key).join();
            if (data == null) {
                return 0;
            }
            return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt();
        }
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
    public static List<Versionstamp> listTasks(TransactionalContext tx, String namespace, String bucket, String index) {
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
}
