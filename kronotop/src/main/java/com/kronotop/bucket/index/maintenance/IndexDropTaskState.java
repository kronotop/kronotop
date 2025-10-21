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

public record IndexDropTaskState(IndexTaskStatus status, String error) {
    /** Field key for an error message in TaskStorage. */
    public static final String ERROR = "e";

    /** Field key for task status in TaskStorage. */
    public static final String STATUS = "s";

    public static IndexDropTaskState load(Transaction tr, DirectorySubspace subspace, Versionstamp taskId) {
        Map<String, byte[]> entries = TaskStorage.getStateFields(tr, subspace, taskId);

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
        return new IndexDropTaskState(status, error);
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
