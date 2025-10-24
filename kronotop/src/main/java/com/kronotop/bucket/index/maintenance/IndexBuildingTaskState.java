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

import java.nio.charset.StandardCharsets;
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
 *   <li><b>highestVersionstamp:</b> The upper bound versionstamp when the task started (defines end of range)</li>
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
 * <p><b>Versionstamp Progress Tracking:</b></p>
 * <p>The task processes entries in versionstamp order from cursorVersionstamp to highestVersionstamp:</p>
 * <pre>{@code
 * while (cursorVersionstamp < highestVersionstamp) {
 *     // Process next batch of entries
 *     // Update cursorVersionstamp
 * }
 * // Mark status as COMPLETED
 * }</pre>
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
 * @param highestVersionstamp the upper bound versionstamp defining the end of the range to process
 * @param status the current execution status of the task
 * @param error error message if status is FAILED, null otherwise
 *
 * @see IndexBuildingTask
 * @see IndexTaskStatus
 * @see IndexMaintenanceTaskSweeper
 * @see TaskStorage
 */
public record IndexBuildingTaskState(Versionstamp cursorVersionstamp, Versionstamp highestVersionstamp,
                                     IndexTaskStatus status, String error) {
    /** Field key for the cursor versionstamp in TaskStorage. */
    public static final String CURSOR_VERSIONSTAMP = "cv";

    /** Field key for the highest versionstamp in TaskStorage. */
    public static final String HIGHEST_VERSIONSTAMP = "hv";

    /** Field key for an error message in TaskStorage. */
    public static final String ERROR = "e";

    /** Field key for task status in TaskStorage. */
    public static final String STATUS = "s";

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
     *   <li><b>highestVersionstamp:</b> null (will be set when task starts)</li>
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
     * @param tr the transaction to use for loading state
     * @param subspace the directory subspace containing the task
     * @param taskId the versionstamp identifier of the task
     * @return the loaded IndexBuildingTaskState with current field values or defaults
     */
    public static IndexBuildingTaskState load(Transaction tr, DirectorySubspace subspace, Versionstamp taskId) {
        Map<String, byte[]> entries = TaskStorage.getStateFields(tr, subspace, taskId);
        Versionstamp cursorVersionstamp = null;
        byte[] rawCursorVs = entries.get(CURSOR_VERSIONSTAMP);
        if (rawCursorVs != null) {
            cursorVersionstamp = Versionstamp.fromBytes(rawCursorVs);
        }

        Versionstamp highestVersionstamp = null;
        byte[] rawHighestVs = entries.get(HIGHEST_VERSIONSTAMP);
        if (rawHighestVs != null) {
            highestVersionstamp = Versionstamp.fromBytes(rawHighestVs);
        }

        String error = null;
        byte[] rawError = entries.get(ERROR);
        if (rawError != null) {
            error = new String(rawError, StandardCharsets.UTF_8);
        }

        IndexTaskStatus status = IndexTaskStatus.WAITING; // Initial status should be WAITING
        byte[] rawStatus = entries.get(STATUS);
        if (rawStatus != null) {
            status = IndexTaskStatus.valueOf(new String(rawStatus));
        }
        return new IndexBuildingTaskState(cursorVersionstamp, highestVersionstamp, status, error);
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
     * @param tr the transaction to use for the update
     * @param subspace the directory subspace containing the task
     * @param taskId the versionstamp identifier of the task
     * @param value the new cursor position (versionstamp of last processed entry)
     */
    public static void setCursorVersionstamp(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, Versionstamp value) {
        TaskStorage.setStateField(tr, subspace, taskId, CURSOR_VERSIONSTAMP, value.getBytes());
    }

    /**
     * Sets the highest versionstamp defining the upper bound of entries to process.
     *
     * <p>The highest versionstamp is set when the task starts and represents the snapshot
     * point of the bucket. Only entries with versionstamps <= highestVersionstamp are
     * processed by this task. This ensures the index build operates on a consistent snapshot
     * even as new documents are inserted.</p>
     *
     * <p><b>Typical Usage:</b></p>
     * <pre>{@code
     * // When starting task, capture current highest versionstamp
     * Versionstamp snapshot = getLatestVersionstamp(tr, bucket);
     * IndexBuildingTaskState.setHighestVersionstamp(tr, subspace, taskId, snapshot);
     * }</pre>
     *
     * @param tr the transaction to use for the update
     * @param subspace the directory subspace containing the task
     * @param taskId the versionstamp identifier of the task
     * @param value the highest versionstamp to process (snapshot boundary)
     */
    public static void setHighestVersionstamp(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, Versionstamp value) {
        TaskStorage.setStateField(tr, subspace, taskId, HIGHEST_VERSIONSTAMP, value.getBytes());
    }

    /**
     * Records an error message when a task fails.
     *
     * <p>This method should be called when a task encounters a fatal error that prevents
     * it from completing. The error message is stored for debugging and monitoring purposes.
     * Typically called in conjunction with {@link #setStatus} to mark the task as FAILED.</p>
     *
     * <p><b>Usage Pattern:</b></p>
     * <pre>{@code
     * try {
     *     // Build index
     * } catch (Exception e) {
     *     IndexBuildingTaskState.setError(tr, subspace, taskId, e.getMessage());
     *     IndexBuildingTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.FAILED);
     *     tr.commit();
     * }
     * }</pre>
     *
     * @param tr the transaction to use for the update
     * @param subspace the directory subspace containing the task
     * @param taskId the versionstamp identifier of the task
     * @param error the error message describing the failure
     */
    public static void setError(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, String error) {
        TaskStorage.setStateField(tr, subspace, taskId, ERROR, error.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Updates the task status to reflect current execution state.
     *
     * <p>Status transitions should follow the lifecycle:</p>
     * <ul>
     *   <li>WAITING → RUNNING (task starts execution)</li>
     *   <li>RUNNING → COMPLETED (task finishes successfully)</li>
     *   <li>RUNNING → FAILED (task encounters error)</li>
     *   <li>RUNNING → STOPPED (task manually stopped)</li>
     * </ul>
     *
     * <p><b>Important:</b> Once a task reaches a terminal status (COMPLETED, FAILED, STOPPED),
     * it should not transition to any other status. Use {@link #isTerminal} to check.</p>
     *
     * @param tr the transaction to use for the update
     * @param subspace the directory subspace containing the task
     * @param taskId the versionstamp identifier of the task
     * @param status the new status to set
     */
    public static void setStatus(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, IndexTaskStatus status) {
        TaskStorage.setStateField(tr, subspace, taskId, STATUS, status.name().getBytes());
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
}
