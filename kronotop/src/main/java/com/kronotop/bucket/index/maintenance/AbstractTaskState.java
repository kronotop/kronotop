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
import com.kronotop.bucket.index.statistics.IndexAnalyzeTaskState;
import com.kronotop.internal.task.TaskStorage;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Base class for index maintenance task state stored in FoundationDB.
 *
 * <p>Tracks the shared status and error fields for all index maintenance tasks (BUILD, DROP,
 * BOUNDARY, ANALYZE) and validates status transitions. State is persisted through
 * {@link TaskStorage}. Subclasses add their own fields, such as a cursor position, and provide
 * a load() method.
 *
 * <p>Status lifecycle:
 * <pre>
 * WAITING -> RUNNING -> COMPLETED (success)
 *                  -> FAILED (error, terminal)
 *                  -> STOPPED (manual, terminal)
 * </pre>
 *
 * <p>Transition rules enforced by {@link #setStatus}:
 * <ul>
 *   <li>Same-state transitions are accepted (idempotent).</li>
 *   <li>COMPLETED is terminal.</li>
 *   <li>STOPPED is terminal, except a move to FAILED is allowed to record the error.</li>
 *   <li>WAITING cannot go straight to COMPLETED or FAILED, it must reach RUNNING first.</li>
 * </ul>
 *
 * <p>Fields are stored under key "s" (status enum name) and "e" (UTF-8 error message).
 *
 * @see IndexBuildingTaskState
 * @see IndexBoundaryTaskState
 * @see IndexDropTaskState
 * @see IndexAnalyzeTaskState
 * @see TaskStorage
 * @see IndexTaskStatus
 */
public abstract class AbstractTaskState {
    /**
     * Field key for an error message in TaskStorage.
     */
    protected static final String ERROR = "e";
    /**
     * Field key for task status in TaskStorage.
     */
    protected static final String STATUS = "s";
    private final IndexTaskStatus status;
    private final String error;

    protected AbstractTaskState(IndexTaskStatus status, String error) {
        this.status = status;
        this.error = error;
    }

    /**
     * Loads the shared status and error fields from a raw TaskStorage field map.
     *
     * <p>A missing status defaults to WAITING, and a missing error defaults to null.
     *
     * @param entries raw state field map from TaskStorage
     * @return status and error as a {@link TaskStateFields} record
     */
    public static TaskStateFields loadCommonFields(Map<String, byte[]> entries) {
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
        return new TaskStateFields(status, error);
    }

    /**
     * Records an error message for a task, stored as UTF-8. Usually called before moving the
     * status to FAILED. Read it back via {@link #error()}.
     *
     * @param tr       transaction for state update
     * @param subspace task subspace
     * @param taskId   task identifier
     * @param error    error message describing the failure
     */
    public static void setError(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, String error) {
        TaskStorage.setStateField(tr, subspace, taskId, ERROR, error.getBytes(StandardCharsets.UTF_8));
    }

    private static void invalidTransition(IndexTaskStatus current, IndexTaskStatus target, String reason) {
        throw new InvalidTaskStateException(
                String.format("Cannot transition from %s to %s: %s", current, target, reason)
        );
    }

    /**
     * Reads the current status and rejects a transition that breaks the state machine rules.
     * If the task was already purged by the watchdog, throws {@link TaskPurgedException}.
     *
     * @param tr       transaction for reading current state
     * @param subspace task subspace
     * @param taskId   task identifier
     * @param target   desired target status
     * @throws InvalidTaskStateException if transition violates state machine rules
     */
    private static void checkStatusTransitionRules(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, IndexTaskStatus target) {
        Map<String, byte[]> entries = TaskStorage.getStateFields(tr, subspace, taskId);

        byte[] rawStatus = entries.get(STATUS);
        IndexTaskStatus current;
        if (rawStatus == null) {
            // No status field found. Check if the task was purged by the watchdog.
            byte[] definition = TaskStorage.getDefinition(tr, subspace, taskId);
            if (definition == null) {
                // Task has been purged, so there is no state left to update
                throw new TaskPurgedException();
            }
            current = IndexTaskStatus.WAITING; // initial
        } else {
            current = IndexTaskStatus.valueOf(new String(rawStatus));
        }

        if (current == target) {
            return;
        }

        if (current == IndexTaskStatus.COMPLETED) {
            throw new InvalidTaskStateException(
                    String.format("Cannot update status: task is already %s.", current)
            );
        }

        if (current == IndexTaskStatus.STOPPED && target != IndexTaskStatus.FAILED) {
            // Accept the failed status to see the error in the records
            throw new InvalidTaskStateException(
                    String.format("Cannot update status: task is in terminal state (%s).", current)
            );
        }

        if (current == IndexTaskStatus.WAITING && target == IndexTaskStatus.COMPLETED) {
            invalidTransition(current, target, "task has not started yet.");
        }

        if (current == IndexTaskStatus.WAITING && target == IndexTaskStatus.FAILED) {
            invalidTransition(current, target, "task has not started yet.");
        }
    }

    /**
     * Validates the transition via {@link #checkStatusTransitionRules}, then stores the new status
     * as its enum name. If the task was already purged, does nothing.
     *
     * @param tr       transaction for status update
     * @param subspace task subspace
     * @param taskId   task identifier
     * @param target   new status to set
     * @throws InvalidTaskStateException if transition violates state machine rules
     */
    public static void setStatus(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, IndexTaskStatus target) {
        try {
            checkStatusTransitionRules(tr, subspace, taskId, target);
        } catch (TaskPurgedException e) {
            // Task was purged by watchdog, so there is nothing to update
            return;
        }
        TaskStorage.setStateField(tr, subspace, taskId, STATUS, target.name().getBytes());
    }

    /**
     * Returns the current task status.
     *
     * @return current status (WAITING, RUNNING, COMPLETED, FAILED, or STOPPED)
     */
    public IndexTaskStatus status() {
        return status;
    }

    /**
     * Returns the error message if task failed.
     *
     * @return error message if status is FAILED, null otherwise
     */
    public String error() {
        return error;
    }

    /**
     * Record holding common task state fields loaded from FoundationDB.
     *
     * @param status current task execution status
     * @param error  error message if failed, null otherwise
     */
    public record TaskStateFields(IndexTaskStatus status, String error) {
    }
}
