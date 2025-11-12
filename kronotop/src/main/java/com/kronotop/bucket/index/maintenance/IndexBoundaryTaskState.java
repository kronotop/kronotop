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
 * State tracker for index boundary detection tasks.
 *
 * <p>IndexBoundaryTaskState manages the lifecycle of boundary detection operations that
 * determine the versionstamp scan range for index building. This is a short-lived task
 * that runs once per index creation to calculate lower and upper boundaries.
 *
 * <p><strong>Boundary Task Purpose:</strong>
 * <ul>
 *   <li>Wait for cluster-wide bucket metadata convergence</li>
 *   <li>Scan primary and secondary indexes to determine versionstamp boundaries</li>
 *   <li>Create BUILD tasks for each shard with calculated boundaries</li>
 *   <li>Mark index as READY if bucket is empty (optimization)</li>
 * </ul>
 *
 * <p><strong>State Lifecycle:</strong>
 * <pre>
 * WAITING → RUNNING → COMPLETED (success)
 *                  → FAILED (error during boundary detection)
 * </pre>
 *
 * <p><strong>State Fields:</strong>
 * <ul>
 *   <li>status: Task execution status (inherited from AbstractTaskState)</li>
 *   <li>error: Error message if failed (inherited from AbstractTaskState)</li>
 * </ul>
 *
 * <p>Unlike {@link IndexBuildingTaskState}, this task state has no additional fields
 * beyond status and error. Boundary tasks complete quickly and don't require cursor
 * tracking or progress persistence.
 *
 * <p><strong>Cleanup:</strong> Completed or failed boundary tasks are removed by
 * {@link IndexMaintenanceTaskSweeper} after execution.
 *
 * @see IndexBoundaryRoutine
 * @see IndexBoundaryTask
 * @see BoundaryLocator
 * @see AbstractTaskState
 */
public class IndexBoundaryTaskState extends AbstractTaskState {
    /**
     * Creates a new boundary task state with the specified status and error.
     *
     * @param status current task status
     * @param error error message if failed, null otherwise
     */
    public IndexBoundaryTaskState(IndexTaskStatus status, String error) {
        super(status, error);
    }

    /**
     * Loads the boundary task state from FoundationDB.
     *
     * <p>Retrieves common state fields (status and error) using the parent class
     * {@link AbstractTaskState#loadCommonFields}. No additional fields are loaded
     * as boundary tasks only track execution status.
     *
     * @param tr transaction for reading state
     * @param subspace task subspace
     * @param taskId task identifier
     * @return loaded boundary task state
     */
    public static IndexBoundaryTaskState load(Transaction tr, DirectorySubspace subspace, Versionstamp taskId) {
        Map<String, byte[]> entries = TaskStorage.getStateFields(tr, subspace, taskId);
        TaskStateFields fields = loadCommonFields(entries);
        return new IndexBoundaryTaskState(fields.status(), fields.error());
    }
}
