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
import com.kronotop.internal.task.TaskStorage;

import java.util.Map;

/**
 * Runtime state of an index building task on a single shard.
 *
 * <p>Tracks the progress of a background index build. Each shard keeps its own state in
 * FoundationDB so builds can be monitored and resumed across the cluster.
 *
 * <p>Adds two fields to the status and error from {@link AbstractTaskState}:
 * <ul>
 *   <li>cursorVersionstamp: last processed entry, the position a build resumes from</li>
 *   <li>bootstrapped: whether the first batch of entries has been processed</li>
 * </ul>
 *
 * <p>Fields are stored separately in FoundationDB via {@link TaskStorage}, so a single field
 * can be updated without rewriting the whole state.
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
     * Loads task state from FoundationDB within the given transaction. Missing fields fall
     * back to defaults: cursorVersionstamp null, bootstrapped false, status WAITING, error null.
     *
     * @param tr       the transaction to use for loading state
     * @param subspace the directory subspace containing the task
     * @param taskId   the versionstamp identifier of the task
     * @return the loaded state with stored values or defaults
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
     * Updates the cursor versionstamp to the last processed entry so the build can resume
     * from this position after an interruption.
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
     * Returns whether the status is terminal, meaning the task has finished execution.
     *
     * @param status the status to check
     * @return true if the status is COMPLETED, FAILED, or STOPPED, false otherwise
     */
    public static boolean isTerminal(IndexTaskStatus status) {
        return status.equals(IndexTaskStatus.COMPLETED) || status.equals(IndexTaskStatus.FAILED) || status.equals(IndexTaskStatus.STOPPED);
    }

    /**
     * Sets or clears the {@code BOOTSTRAPPED} flag. When {@code true}, the first batch has been
     * processed, and later runs continue after the cursor with exclusive selectors
     * (e.g., {@code firstGreaterThan}).
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
