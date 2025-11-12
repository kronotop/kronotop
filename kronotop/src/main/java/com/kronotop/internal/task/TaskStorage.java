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

package com.kronotop.internal.task;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.index.maintenance.IndexMaintenanceTaskSweeper;
import com.kronotop.bucket.index.maintenance.IndexMaintenanceWatchDog;
import com.kronotop.bucket.index.maintenance.IndexMaintenanceWorker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * TaskStorage provides low-level FoundationDB operations for managing background tasks in Kronotop.
 *
 * <p>This utility class handles task creation, storage, state management, and deletion using
 * FoundationDB's tuple-based key structure and versionstamping. It serves as the persistence
 * layer for the background task system, particularly used for index building operations.
 *
 * <h2>Key Structure</h2>
 * The class uses a hierarchical key structure based on magic bytes:
 * <ul>
 *   <li><b>Trigger Key:</b> {@code [TRIGGER_MAGIC]} - Used to notify watchers of new tasks</li>
 *   <li><b>Definition Key:</b> {@code [TASKS_MAGIC, DEFINITION, Versionstamp]} - Stores task definition</li>
 *   <li><b>State Key:</b> {@code [TASKS_MAGIC, STATE, Versionstamp, field_name]} - Stores task state fields</li>
 * </ul>
 *
 * <h2>Versionstamping</h2>
 * Tasks are uniquely identified by FoundationDB versionstamps, which provide:
 * <ul>
 *   <li>Globally unique task IDs across the cluster</li>
 *   <li>Automatic chronological ordering</li>
 *   <li>Atomic ID generation without coordination</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 * All operations are transactional through FoundationDB, providing ACID guarantees.
 * The class itself is stateless and thread-safe.
 *
 * @see IndexMaintenanceWatchDog
 * @see IndexMaintenanceWorker
 * @see IndexMaintenanceTaskSweeper
 */
public class TaskStorage {
    /**
     * Magic byte prefix for all task-related keys.
     */
    public static final byte TASKS_MAGIC = 0x23;

    /**
     * Magic byte prefix for watcher trigger keys.
     */
    private static final byte TRIGGER_MAGIC = 0x21;

    /**
     * Magic byte suffix for task definition keys.
     */
    private static final byte DEFINITION = 0x44;

    /**
     * Magic byte suffix for task state keys.
     */
    private static final byte STATE = 0x53;

    /**
     * Little-endian representation of 1L for atomic ADD mutations.
     */
    private static final byte[] POSITIVE_DELTA_ONE = new byte[]{1, 0, 0, 0, 0, 0, 0, 0}; // 1L, little-endian

    /**
     * Constructs the trigger key for watching task changes.
     *
     * <p>This key is used with FoundationDB's watch mechanism to detect when new tasks
     * are created. Watchers can monitor this key to be notified when tasks are added.
     *
     * @param subspace the directory subspace for task storage
     * @return the trigger key as a byte array
     */
    public static byte[] trigger(DirectorySubspace subspace) {
        return subspace.pack(Tuple.from(TRIGGER_MAGIC));
    }

    /**
     * Triggers watchers by incrementing the trigger counter.
     *
     * <p>This method uses an atomic ADD mutation to increment the trigger key,
     * notifying any watchers that a new task has been created.
     *
     * @param tr       the transaction instance
     * @param subspace the directory subspace for task storage
     */
    public static void triggerWatchers(Transaction tr, DirectorySubspace subspace) {
        tr.mutate(MutationType.ADD, trigger(subspace), POSITIVE_DELTA_ONE);
    }

    /**
     * Creates a new task within the given transaction.
     *
     * <p>This method stores a task definition using FoundationDB's versionstamped keys,
     * which ensures that each task receives a unique, monotonically increasing ID.
     * The versionstamp is incomplete at the time of mutation and will be completed
     * when the transaction commits.
     *
     * <p>The userVersion parameter allows for task ordering within the same transaction.
     * Tasks with higher userVersion values will sort after tasks with lower values
     * when they have the same transaction version.
     *
     * <p><b>Important:</b> The returned CompletableFuture will only complete after
     * the transaction is committed. Callers must commit the transaction and join
     * the future to obtain the complete versionstamp.
     *
     * @param tr          the transaction instance to use for the operation
     * @param userVersion the user-defined version for task ordering (0-1023)
     * @param subspace    the directory subspace for task storage
     * @param definition  the serialized task definition (typically JSON-encoded)
     * @return a CompletableFuture containing the complete versionstamp after commit
     */
    public static CompletableFuture<byte[]> create(Transaction tr, int userVersion, DirectorySubspace subspace, byte[] definition) {
        byte[] key = subspace.packWithVersionstamp(Tuple.from(TASKS_MAGIC, DEFINITION, Versionstamp.incomplete(userVersion)));
        CompletableFuture<byte[]> future = tr.getVersionstamp();
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, key, definition);

        triggerWatchers(tr, subspace);
        return future;
    }

    /**
     * Creates a new task with auto-commit.
     *
     * <p>This convenience method creates its own transaction, stores the task definition,
     * commits the transaction, and returns the complete versionstamp. Use this method
     * when you don't need to coordinate the task creation with other operations in a
     * larger transaction.
     *
     * <p>For task creation within an existing transaction, use
     * {@link #create(Transaction, int, DirectorySubspace, byte[])} instead.
     *
     * @param context    the application context providing database access
     * @param subspace   the directory subspace for task storage
     * @param definition the serialized task definition (typically JSON-encoded)
     * @return the complete versionstamp identifying the created task
     */
    public static Versionstamp create(Context context, DirectorySubspace subspace, byte[] definition) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            CompletableFuture<byte[]> future = create(tr, 0, subspace, definition);
            tr.commit().join();

            byte[] trVersion = future.join();
            return Versionstamp.complete(trVersion);
        }
    }

    /**
     * Retrieves the task definition for the specified task ID.
     *
     * <p>The task definition contains the serialized task information, typically
     * JSON-encoded, that describes what work the task should perform.
     *
     * @param tr       the transaction instance to use for the operation
     * @param subspace the directory subspace where the task is stored
     * @param taskId   the complete versionstamp identifying the task
     * @return the serialized task definition, or null if the task doesn't exist
     */
    public static byte[] getDefinition(Transaction tr, DirectorySubspace subspace, Versionstamp taskId) {
        byte[] key = subspace.pack(Tuple.from(TASKS_MAGIC, DEFINITION, taskId));
        return tr.get(key).join();
    }

    /**
     * Deletes a task and all its associated data.
     *
     * <p>This method removes the task definition and all state fields for the specified
     * task. The operation is atomic within the transaction.
     *
     * <p>The method performs two separate clear operations:
     * <ul>
     *   <li>Clears the definition key: {@code [TASKS_MAGIC, DEFINITION, taskId]}</li>
     *   <li>Range clears all state keys: {@code [TASKS_MAGIC, STATE, taskId, *]}</li>
     * </ul>
     *
     * @param tr       the transaction instance to use for the operation
     * @param subspace the directory subspace where the task is stored
     * @param taskId   the complete versionstamp identifying the task to delete
     */
    public static void drop(Transaction tr, DirectorySubspace subspace, Versionstamp taskId) {
        // Drop the task definition
        tr.clear(subspace.pack(Tuple.from(TASKS_MAGIC, DEFINITION, taskId)));

        // Then drop the task state
        byte[] begin = subspace.pack(Tuple.from(TASKS_MAGIC, STATE, taskId));
        byte[] end = ByteArrayUtil.strinc(begin);
        tr.clear(begin, end);
    }

    /**
     * Sets a state field for the specified task.
     *
     * <p>Task state is stored as key-value pairs, allowing flexible state management
     * for different task types. State fields are independent of the task definition
     * and can be updated as the task progresses.
     *
     * <p>Common state fields include:
     * <ul>
     *   <li>progress - Current progress information</li>
     *   <li>status - Task execution status</li>
     *   <li>error - Error information if the task failed</li>
     *   <li>result - Task execution result</li>
     * </ul>
     *
     * @param tr       the transaction instance to use for the operation
     * @param subspace the directory subspace where the task is stored
     * @param taskId   the complete versionstamp identifying the task
     * @param field    the name of the state field to set
     * @param value    the serialized field value
     */
    public static void setStateField(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, String field, byte[] value) {
        byte[] key = subspace.pack(Tuple.from(TASKS_MAGIC, STATE, taskId, field));
        tr.set(key, value);
    }

    /**
     * Retrieves a single state field for the specified task.
     *
     * @param tr       the transaction instance to use for the operation
     * @param subspace the directory subspace where the task is stored
     * @param taskId   the complete versionstamp identifying the task
     * @param field    the name of the state field to retrieve
     * @return the serialized field value, or null if the field doesn't exist
     */
    public static byte[] getStateField(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, String field) {
        byte[] key = subspace.pack(Tuple.from(TASKS_MAGIC, STATE, taskId, field));
        return tr.get(key).join();
    }

    /**
     * Retrieves all state fields for the specified task.
     *
     * <p>This method performs a range scan to retrieve all state fields associated
     * with the task. The returned map contains field names as keys and serialized
     * values as byte arrays.
     *
     * <p>This is useful for:
     * <ul>
     *   <li>Retrieving a complete task state for inspection or debugging</li>
     *   <li>Migrating or archiving task state</li>
     *   <li>Displaying task progress and status information</li>
     * </ul>
     *
     * @param tr       the transaction instance to use for the operation
     * @param subspace the directory subspace where the task is stored
     * @param taskId   the complete versionstamp identifying the task
     * @return a map of field names to serialized values, empty if no state fields exist
     */
    public static Map<String, byte[]> getStateFields(Transaction tr, DirectorySubspace subspace, Versionstamp taskId) {
        byte[] begin = subspace.pack(Tuple.from(TASKS_MAGIC, STATE, taskId));
        byte[] end = ByteArrayUtil.strinc(begin);
        Map<String, byte[]> entries = new HashMap<>();
        for (KeyValue entry : tr.getRange(begin, end)) {
            Tuple tuple = subspace.unpack(entry.getKey());
            String key = tuple.get(3).toString();
            entries.put(key, entry.getValue());
        }
        return entries;
    }

    /**
     * Iterates over all task definitions in chronological order, applying an action to each task ID.
     *
     * <p>This method performs a range scan over all keys with the prefix
     * {@code [TASKS_MAGIC, DEFINITION]} and extracts task IDs (versionstamps) from
     * each key. Tasks are iterated in chronological order due to the monotonically
     * increasing nature of versionstamps.
     *
     * <p>Iteration can be terminated early by returning {@code false} from the action
     * function. Common use cases include:
     * <ul>
     *   <li>Finding a specific task and stopping when found</li>
     *   <li>Processing a limited number of tasks</li>
     *   <li>Implementing conditional task processing</li>
     * </ul>
     *
     * <p><b>Note:</b> Only task definition keys are scanned, ensuring each task is
     * processed exactly once. Task state entries are excluded from iteration.
     *
     * @param tr       the transaction to use for the range scan
     * @param subspace the directory subspace containing task data
     * @param action   a function that receives each task ID and returns {@code true} to
     *                 continue iteration or {@code false} to stop
     */
    public static void tasks(Transaction tr, DirectorySubspace subspace, Function<Versionstamp, Boolean> action) {
        byte[] beginKey = subspace.pack(Tuple.from(TASKS_MAGIC, DEFINITION));
        byte[] endKey = ByteArrayUtil.strinc(beginKey);

        KeySelector begin = KeySelector.firstGreaterThan(beginKey);
        KeySelector end = KeySelector.firstGreaterOrEqual(endKey);
        for (KeyValue entry : tr.getRange(begin, end)) {
            Tuple tuple = subspace.unpack(entry.getKey());
            Versionstamp taskId = (Versionstamp) tuple.get(2);
            if (!action.apply(taskId)) {
                break;
            }
        }
    }
}
