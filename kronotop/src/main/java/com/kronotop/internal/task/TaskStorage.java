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
 * Stores background tasks in FoundationDB. A task has a definition and a set of state fields.
 * The task id is the versionstamp of the transaction that created it, so ids are unique and
 * ordered by creation time.
 *
 * <p>Key layout inside the given subspace:
 * <ul>
 *   <li>{@code [TRIGGER_MAGIC]}: counter watched by task consumers</li>
 *   <li>{@code [TASKS_MAGIC, DEFINITION, taskId]}: task definition</li>
 *   <li>{@code [TASKS_MAGIC, STATE, taskId, field]}: one state field</li>
 * </ul>
 *
 * <p>All methods are static and run inside the caller's transaction.
 *
 * @see IndexMaintenanceWatchDog
 * @see IndexMaintenanceWorker
 * @see IndexMaintenanceTaskSweeper
 */
public class TaskStorage {
    /**
     * Prefix of all task keys.
     */
    public static final byte TASKS_MAGIC = 0x23;

    /**
     * Key of the watch trigger counter.
     */
    private static final byte TRIGGER_MAGIC = 0x21;

    /**
     * Second element of definition keys.
     */
    private static final byte DEFINITION = 0x44;

    /**
     * Second element of state keys.
     */
    private static final byte STATE = 0x53;

    /**
     * 1L in little-endian, used by the ADD mutation on the trigger key.
     */
    private static final byte[] POSITIVE_DELTA_ONE = new byte[]{1, 0, 0, 0, 0, 0, 0, 0}; // 1L, little-endian

    /**
     * Returns the trigger key of the subspace. Consumers watch this key to learn about new tasks.
     */
    public static byte[] trigger(DirectorySubspace subspace) {
        return subspace.pack(Tuple.from(TRIGGER_MAGIC));
    }

    /**
     * Increments the trigger counter so that watchers wake up.
     */
    public static void triggerWatchers(Transaction tr, DirectorySubspace subspace) {
        tr.mutate(MutationType.ADD, trigger(subspace), POSITIVE_DELTA_ONE);
    }

    /**
     * Creates a task in the given transaction and wakes up watchers. The returned future holds
     * the transaction versionstamp and completes only after the caller commits. Tasks created in
     * the same transaction are ordered by {@code userVersion}.
     *
     * @param tr          the transaction
     * @param userVersion order among tasks of the same transaction, 0 to 1023
     * @param subspace    the task subspace
     * @param definition  the serialized task definition
     * @return the transaction versionstamp, available after commit
     */
    public static CompletableFuture<byte[]> create(Transaction tr, int userVersion, DirectorySubspace subspace, byte[] definition) {
        byte[] key = subspace.packWithVersionstamp(Tuple.from(TASKS_MAGIC, DEFINITION, Versionstamp.incomplete(userVersion)));
        CompletableFuture<byte[]> future = tr.getVersionstamp();
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, key, definition);

        triggerWatchers(tr, subspace);
        return future;
    }

    /**
     * Creates a task in its own transaction and returns its id. Use
     * {@link #create(Transaction, int, DirectorySubspace, byte[])} to create a task as part of
     * a larger transaction.
     *
     * @param context    the context that provides the transaction manager
     * @param subspace   the task subspace
     * @param definition the serialized task definition
     * @return the id of the new task
     */
    public static Versionstamp create(Context context, DirectorySubspace subspace, byte[] definition) {
        try (Transaction tr = context.getTransactionManager().create()) {
            CompletableFuture<byte[]> future = create(tr, 0, subspace, definition);
            tr.commit().join();

            byte[] trVersion = future.join();
            return Versionstamp.complete(trVersion);
        }
    }

    /**
     * Returns the definition of the task, or null if the task does not exist.
     */
    public static byte[] getDefinition(Transaction tr, DirectorySubspace subspace, Versionstamp taskId) {
        byte[] key = subspace.pack(Tuple.from(TASKS_MAGIC, DEFINITION, taskId));
        return tr.get(key).join();
    }

    /**
     * Removes the definition and all state fields of the task.
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
     * Sets one state field of the task. State fields are separate from the definition and change
     * while the task runs.
     *
     * @param tr       the transaction
     * @param subspace the task subspace
     * @param taskId   the task id
     * @param field    the field name
     * @param value    the serialized field value
     */
    public static void setStateField(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, String field, byte[] value) {
        byte[] key = subspace.pack(Tuple.from(TASKS_MAGIC, STATE, taskId, field));
        tr.set(key, value);
    }

    /**
     * Returns one state field of the task, or null if the field is not set.
     */
    public static byte[] getStateField(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, String field) {
        byte[] key = subspace.pack(Tuple.from(TASKS_MAGIC, STATE, taskId, field));
        return tr.get(key).join();
    }

    /**
     * Returns all state fields of the task by name. The map is empty if no field is set.
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
     * Calls {@code action} with each task id in creation order. Iteration stops when the action
     * returns {@code false}. Only definition keys are scanned, so each task is visited once.
     *
     * @param tr       the transaction
     * @param subspace the task subspace
     * @param action   receives a task id and returns {@code true} to continue
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
