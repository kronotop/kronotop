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
 * State for an index boundary detection task.
 *
 * <p>A boundary task runs once per index creation. It waits for cluster-wide bucket metadata
 * convergence, scans the primary index to find the versionstamp scan range, creates a build task
 * per shard, and marks the index READY early if the bucket is empty.
 *
 * <p>Unlike {@link IndexBuildingTaskState}, this state carries only the inherited status and error
 * fields. Boundary tasks are short-lived and need no cursor or progress tracking. Completed or
 * failed tasks are removed by {@link IndexMaintenanceTaskSweeper}.
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
     * @param error  error message if failed, null otherwise
     */
    public IndexBoundaryTaskState(IndexTaskStatus status, String error) {
        super(status, error);
    }

    /**
     * Loads the boundary task state from FoundationDB. Only the shared status and error fields are
     * read, via {@link AbstractTaskState#loadCommonFields}.
     *
     * @param tr       transaction for reading state
     * @param subspace task subspace
     * @param taskId   task identifier
     * @return loaded boundary task state
     */
    public static IndexBoundaryTaskState load(Transaction tr, DirectorySubspace subspace, Versionstamp taskId) {
        Map<String, byte[]> entries = TaskStorage.getStateFields(tr, subspace, taskId);
        TaskStateFields fields = loadCommonFields(entries);
        return new IndexBoundaryTaskState(fields.status(), fields.error());
    }
}
