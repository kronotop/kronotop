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
 * State tracker for an index drop task that removes index data from FoundationDB.
 *
 * <p>A drop task clears the index entries and publishes a metadata update so readers stop
 * using the index. It runs as a single task, not one per shard. Only status and error are
 * tracked, with no cursor, since the clear runs within a transaction.
 *
 * <p>State lifecycle:
 * <pre>
 * WAITING -> RUNNING -> COMPLETED (success)
 *                  -> FAILED (error during deletion)
 *                  -> STOPPED (manually stopped)
 * </pre>
 *
 * <p>State fields, both inherited from {@link AbstractTaskState}:
 * <ul>
 *   <li>status: task execution status</li>
 *   <li>error: error message if failed</li>
 * </ul>
 *
 * <p>The {@link IndexMaintenanceTaskSweeper} removes the completed task once the index no
 * longer exists.
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
