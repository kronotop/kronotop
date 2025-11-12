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
import com.kronotop.internal.task.TaskStorage;

import java.util.Map;

/**
 * Represents the runtime state of an index builder task executing on a single shard.
 *
 * <p>IndexBuildingTaskState tracks the progress and status of background index building
 * operations. Each shard maintains its own task state in FoundationDB, allowing distributed
 * coordination and monitoring of index construction across the cluster.</p>
 *
 * <p><b>State Components:</b></p>
 * <ul>
 *   <li><b>cursorVersionstamp:</b> The current position in the index building process (last processed entry)</li>
 *   <li><b>status:</b> Current execution status (WAITING, RUNNING, COMPLETED, FAILED, STOPPED)</li>
 *   <li><b>error:</b> Error message if the task failed (null for successful tasks)</li>
 * </ul>
 *
 * <p><b>State Lifecycle:</b></p>
 * <ol>
 *   <li><b>WAITING:</b> Task created, waiting for execution (initial state)</li>
 *   <li><b>RUNNING:</b> Task actively building index entries</li>
 *   <li><b>COMPLETED:</b> Task finished successfully (terminal state)</li>
 *   <li><b>FAILED:</b> Task encountered an error (terminal state, error field populated)</li>
 *   <li><b>STOPPED:</b> Task was manually stopped (terminal state)</li>
 * </ol>
 *
 * <p><b>Persistence:</b></p>
 * <p>State fields are stored separately in FoundationDB using {@link TaskStorage}, allowing
 * atomic updates to individual fields without reading/writing the entire state. This enables
 * efficient progress tracking during long-running index builds.</p>
 *
 * <p><b>Coordination:</b></p>
 * <p>The {@link IndexMaintenanceTaskSweeper} monitors all shard states and only transitions
 * the index from BUILDING to READY when all shards report COMPLETED status, implementing
 * a distributed barrier synchronization pattern.</p>
 *
 * <p><b>Example Usage:</b></p>
 * <pre>{@code
 * // Load current state
 * IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, subspace, taskId);
 *
 * // Update cursor position during processing
 * IndexBuildingTaskState.setCursorVersionstamp(tr, subspace, taskId, newCursor);
 *
 * // Mark task as completed
 * IndexBuildingTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.COMPLETED);
 *
 * // Check if terminal
 * if (IndexBuildingTaskState.isTerminal(state.status())) {
 *     // Task finished (success or failure)
 * }
 * }</pre>
 *
 * @param cursorVersionstamp the current processing position (null if not started)
 * @param status             the current execution status of the task
 * @param error              error message if status is FAILED, null otherwise
 * @see IndexBuildingTask
 * @see IndexTaskStatus
 * @see IndexMaintenanceTaskSweeper
 * @see TaskStorage
 */
public class IndexBuildingTaskState extends AbstractTaskState {
    /**
     * Field key for the cursor versionstamp in TaskStorage.
     */
    private static final String CURSOR_VERSIONSTAMP = "cv";
    /**
     * Field key for bootstrapped flag in TaskStorage.
     */
    private static final String BOOTSTRAPPED = "b";
    private final Versionstamp cursorVersionstamp;
    private final boolean bootstrapped;

    protected IndexBuildingTaskState(Versionstamp cursorVersionstamp, boolean bootstrapped, IndexTaskStatus status, String error) {
        super(status, error);
        this.cursorVersionstamp = cursorVersionstamp;
        this.bootstrapped = bootstrapped;
    }

    /**
     * Loads the task state from FoundationDB for a specific task.
     *
     * <p>This method retrieves all state fields atomically within the given transaction
     * and reconstructs the IndexBuildingTaskState object. If state fields don't exist
     * (new task), default values are used.</p>
     *
     * <p><b>Default Values:</b></p>
     * <ul>
     *   <li><b>cursorVersionstamp:</b> null (task hasn't started processing)</li>
     *   <li><b>status:</b> WAITING (initial status for new tasks)</li>
     *   <li><b>error:</b> null (no error initially)</li>
     * </ul>
     *
     * <p><b>Field Deserialization:</b></p>
     * <p>Each field is deserialized from bytes stored in TaskStorage:</p>
     * <ul>
     *   <li>Versionstamps: Deserialized using Versionstamp.fromBytes()</li>
     *   <li>Status: Deserialized as enum via IndexTaskStatus.valueOf()</li>
     *   <li>Error: Deserialized as UTF-8 string</li>
     * </ul>
     *
     * @param tr       the transaction to use for loading state
     * @param subspace the directory subspace containing the task
     * @param taskId   the versionstamp identifier of the task
     * @return the loaded IndexBuildingTaskState with current field values or defaults
     */
    public static IndexBuildingTaskState load(Transaction tr, DirectorySubspace subspace, Versionstamp taskId) {
        Map<String, byte[]> entries = TaskStorage.getStateFields(tr, subspace, taskId);

        TaskStateFields fields = loadCommonFields(entries);

        boolean bootstrapped = false;
        byte[] rawBootstrapped = entries.get(BOOTSTRAPPED);
        if (rawBootstrapped != null) {
            bootstrapped = rawBootstrapped[0] != 0;
        }

        Versionstamp cursorVersionstamp = null;
        byte[] rawCursorVs = entries.get(CURSOR_VERSIONSTAMP);
        if (rawCursorVs != null) {
            cursorVersionstamp = Versionstamp.fromBytes(rawCursorVs);
        }

        return new IndexBuildingTaskState(cursorVersionstamp, bootstrapped, fields.status(), fields.error());
    }

    /**
     * Updates the cursor versionstamp to track progress during index building.
     *
     * <p>The cursor versionstamp represents the last successfully processed entry.
     * This is updated periodically as the index builder processes batches of entries,
     * allowing the task to resume from this position if interrupted.</p>
     *
     * <p><b>Usage Pattern:</b></p>
     * <pre>{@code
     * // Process batch of entries
     * for (Entry entry : batch) {
     *     // Add entry to index
     * }
     * // Update cursor to last processed entry
     * IndexBuildingTaskState.setCursorVersionstamp(tr, subspace, taskId, lastVersionstamp);
     * tr.commit();
     * }</pre>
     *
     * @param tr       the transaction to use for the update
     * @param subspace the directory subspace containing the task
     * @param taskId   the versionstamp identifier of the task
     * @param value    the new cursor position (versionstamp of last processed entry)
     */
    public static void setCursorVersionstamp(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, Versionstamp value) {
        TaskStorage.setStateField(tr, subspace, taskId, CURSOR_VERSIONSTAMP, value.getBytes());
    }

    /**
     * Checks if the given status is a terminal state (task cannot transition further).
     *
     * <p>Terminal states indicate that the task has finished execution and no further
     * processing will occur. Tasks in terminal states are eligible for cleanup by the
     * {@link IndexMaintenanceTaskSweeper}.</p>
     *
     * <p><b>Terminal States:</b></p>
     * <ul>
     *   <li><b>COMPLETED:</b> Task finished successfully</li>
     *   <li><b>FAILED:</b> Task encountered a fatal error</li>
     *   <li><b>STOPPED:</b> Task was manually stopped</li>
     * </ul>
     *
     * <p><b>Non-Terminal States:</b></p>
     * <ul>
     *   <li><b>WAITING:</b> Task can transition to RUNNING</li>
     *   <li><b>RUNNING:</b> Task can transition to COMPLETED, FAILED, or STOPPED</li>
     * </ul>
     *
     * @param status the status to check
     * @return true if the status is terminal (COMPLETED, FAILED, or STOPPED), false otherwise
     */
    public static boolean isTerminal(IndexTaskStatus status) {
        return status.equals(IndexTaskStatus.COMPLETED) || status.equals(IndexTaskStatus.FAILED) || status.equals(IndexTaskStatus.STOPPED);
    }

    /**
     * Marks the index building task as bootstrapped or resets its bootstrap status.
     *
     * <p>This method updates the {@code BOOTSTRAPPED} flag in the task's state record within
     * FoundationDB. When set to {@code true}, it indicates that the first batch of index
     * entries has been successfully processed and that subsequent executions should use
     * exclusive range selectors (e.g., {@code firstGreaterThan}) to continue from the
     * last known cursor.
     *
     * @param tr           the FoundationDB transaction used to modify the task state
     * @param subspace     the directory subspace where the task state is stored
     * @param taskId       the unique identifier of the index building task
     * @param bootstrapped {@code true} to mark the task as bootstrapped, {@code false} to reset it
     */
    public static void setBootstrapped(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, boolean bootstrapped) {
        TaskStorage.setStateField(tr, subspace, taskId, BOOTSTRAPPED, new byte[]{(byte) (bootstrapped ? 1 : 0)});
    }

    public Versionstamp cursorVersionstamp() {
        return cursorVersionstamp;
    }

    public boolean bootstrapped() {
        return bootstrapped;
    }
}
