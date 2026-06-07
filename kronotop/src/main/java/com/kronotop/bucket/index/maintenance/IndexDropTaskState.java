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
 * State tracker for index drop tasks that physically remove index data from FoundationDB.
 *
 * <p>IndexDropTaskState manages the lifecycle of index deletion operations. Drop tasks
 * are created across all shards when an index is marked as DROPPED, and each shard
 * independently clears its portion of the index directory.
 *
 * <p><strong>Drop Task Purpose:</strong>
 * <ul>
 *   <li>Remove index directory and all subspace data from FoundationDB</li>
 *   <li>Clean up index statistics from bucket metadata</li>
 *   <li>Coordinate distributed deletion across multiple shards</li>
 * </ul>
 *
 * <p><strong>State Lifecycle:</strong>
 * <pre>
 * WAITING → RUNNING → COMPLETED (success)
 *                  → FAILED (error during deletion)
 * </pre>
 *
 * <p><strong>State Fields:</strong>
 * <ul>
 *   <li>status: Task execution status (inherited from AbstractTaskState)</li>
 *   <li>error: Error message if failed (inherited from AbstractTaskState)</li>
 * </ul>
 *
 * <p>Like {@link IndexBoundaryTaskState}, drop tasks only track status and error with no
 * additional state fields. Deletion operations are atomic and don't require cursor tracking.
 *
 * <p><strong>Cleanup:</strong> The {@link IndexMaintenanceTaskSweeper} removes completed
 * drop tasks when the index directory no longer exists in FoundationDB. Once all drop tasks
 * complete, the index is fully removed from bucket metadata.
 *
 * @see IndexDropRoutine
 * @see IndexDropTask
 * @see IndexMaintenanceTaskSweeper
 * @see AbstractTaskState
 */
public class IndexDropTaskState extends AbstractTaskState {

    /**
     * Creates a new drop task state with the specified status and error.
     *
     * @param status current task status
     * @param error  error message if failed, null otherwise
     */
    public IndexDropTaskState(IndexTaskStatus status, String error) {
        super(status, error);
    }

    /**
     * Loads the drop task state from FoundationDB.
     *
     * <p>Retrieves common state fields (status and error) using the parent class
     * {@link AbstractTaskState#loadCommonFields}. No additional fields are loaded
     * as drop tasks only track execution status.
     *
     * @param tr       transaction for reading state
     * @param subspace task subspace
     * @param taskId   task identifier
     * @return loaded drop task state
     */
    public static IndexDropTaskState load(Transaction tr, DirectorySubspace subspace, Versionstamp taskId) {
        Map<String, byte[]> entries = TaskStorage.getStateFields(tr, subspace, taskId);
        TaskStateFields fields = loadCommonFields(entries);
        return new IndexDropTaskState(fields.status(), fields.error());
    }
}
