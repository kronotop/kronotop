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
import com.kronotop.bucket.index.statistics.IndexAnalyzeTaskState;
import com.kronotop.internal.task.TaskStorage;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Base class for index maintenance task state management in FoundationDB.
 *
 * <p>AbstractTaskState provides common state tracking functionality for all index maintenance
 * tasks including BUILD, DROP, BOUNDARY, and ANALYZE operations. It manages status transitions,
 * error tracking, and persistence through {@link TaskStorage}.
 *
 * <p><strong>Task State Model:</strong>
 * <ul>
 *   <li>Status: Current execution state (WAITING, RUNNING, COMPLETED, FAILED, STOPPED)</li>
 *   <li>Error: Optional error message when status is FAILED</li>
 * </ul>
 *
 * <p><strong>Status Lifecycle:</strong>
 * <pre>
 * WAITING → RUNNING → COMPLETED (success)
 *                  → FAILED (error, terminal)
 *                  → STOPPED (manual, terminal)
 * </pre>
 *
 * <p><strong>State Transition Rules:</strong>
 * <ol>
 *   <li>COMPLETED is terminal - no further transitions allowed</li>
 *   <li>STOPPED is terminal - only FAILED transition permitted (to record error)</li>
 *   <li>WAITING → COMPLETED: Invalid (task hasn't started)</li>
 *   <li>WAITING → FAILED: Invalid (task hasn't started)</li>
 *   <li>Same-state transitions: Silently accepted (idempotent)</li>
 * </ol>
 *
 * <p><strong>Field Storage:</strong>
 * <pre>
 * Field Key | Value Type | Description
 * ----------|------------|-------------
 * "s"       | String     | Status enum name (e.g., "RUNNING")
 * "e"       | UTF-8      | Error message (only when FAILED)
 * </pre>
 *
 * <p><strong>Subclass Responsibilities:</strong>
 * <ul>
 *   <li>Define additional state fields (e.g., cursor position, boundaries)</li>
 *   <li>Implement load() method to deserialize full state</li>
 *   <li>Provide field-specific update methods</li>
 * </ul>
 *
 * <p><strong>Usage Pattern:</strong>
 * <pre>{@code
 * // Load current state
 * IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, subspace, taskId);
 *
 * // Check status
 * if (state.status() == IndexTaskStatus.RUNNING) {
 *     // Process task
 * }
 *
 * // Update status
 * AbstractTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.COMPLETED);
 *
 * // Record error
 * AbstractTaskState.setError(tr, subspace, taskId, "Index build failed: reason");
 * AbstractTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.FAILED);
 * }</pre>
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
     * Loads common state fields (status and error) from TaskStorage entries.
     *
     * <p>Deserializes status and error fields from the raw byte map returned by
     * {@link TaskStorage#getStateFields}. Provides default values for missing fields.
     *
     * <p><strong>Default Values:</strong>
     * <ul>
     *   <li>status: WAITING (initial state for new tasks)</li>
     *   <li>error: null (no error message)</li>
     * </ul>
     *
     * @param entries raw state field map from TaskStorage
     * @return TaskStateFields record with deserialized status and error
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
     * Records an error message for a task in FoundationDB.
     *
     * <p>Stores the error message in UTF-8 encoding. Typically called before
     * transitioning status to FAILED. The error message can be retrieved later
     * via the {@link #error()} accessor method.
     *
     * @param tr transaction for state update
     * @param subspace task subspace
     * @param taskId task identifier
     * @param error error message describing the failure
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
     * Validates status transition rules before updating task status.
     *
     * <p>Enforces the state machine transition rules to prevent invalid state changes.
     * Reads the current status from FoundationDB and validates the target status.
     *
     * <p><strong>Validation Rules:</strong>
     * <ul>
     *   <li>Same-state transitions: Allowed (idempotent)</li>
     *   <li>From COMPLETED: Always rejected (terminal)</li>
     *   <li>From STOPPED: Only FAILED allowed (to record error)</li>
     *   <li>WAITING → COMPLETED: Rejected (must be RUNNING first)</li>
     *   <li>WAITING → FAILED: Rejected (must be RUNNING first)</li>
     * </ul>
     *
     * @param tr transaction for reading current state
     * @param subspace task subspace
     * @param taskId task identifier
     * @param target desired target status
     * @throws InvalidTaskStateException if transition violates state machine rules
     */
    private static void checkStatusTransitionRules(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, IndexTaskStatus target) {
        Map<String, byte[]> entries = TaskStorage.getStateFields(tr, subspace, taskId);

        byte[] rawStatus = entries.get(STATUS);
        IndexTaskStatus current = rawStatus == null
                ? IndexTaskStatus.WAITING // initial
                : IndexTaskStatus.valueOf(new String(rawStatus));

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
     * Updates the task status with validation.
     *
     * <p>Performs atomic status update after validating transition rules via
     * {@link #checkStatusTransitionRules}. Stores the new status as enum name.
     *
     * <p><strong>Usage:</strong>
     * <pre>{@code
     * // Start task execution
     * AbstractTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.RUNNING);
     *
     * // Mark successful completion
     * AbstractTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.COMPLETED);
     *
     * // Record failure
     * AbstractTaskState.setError(tr, subspace, taskId, errorMsg);
     * AbstractTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.FAILED);
     * }</pre>
     *
     * @param tr transaction for status update
     * @param subspace task subspace
     * @param taskId task identifier
     * @param target new status to set
     * @throws InvalidTaskStateException if transition violates state machine rules
     */
    public static void setStatus(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, IndexTaskStatus target) {
        checkStatusTransitionRules(tr, subspace, taskId, target);
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
     * @param error error message if failed, null otherwise
     */
    public record TaskStateFields(IndexTaskStatus status, String error) {
    }
}
