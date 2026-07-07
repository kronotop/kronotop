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

package com.kronotop.bucket.index.statistics;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.index.maintenance.AbstractTaskState;
import com.kronotop.bucket.index.maintenance.IndexTaskStatus;
import com.kronotop.internal.task.TaskStorage;

import java.util.Map;

/**
 * State tracker for index analyze tasks. An analyze task samples the index hint space,
 * builds a value distribution histogram, and stores it in bucket metadata for the query
 * planner to estimate selectivity.
 *
 * <p>State lifecycle:
 * <pre>
 * WAITING -> RUNNING -> COMPLETED (success)
 *                  -> FAILED (error during analysis)
 *                  -> STOPPED (manual cancellation)
 * </pre>
 *
 * <p>Only status and error are tracked, both inherited from {@link AbstractTaskState}. Unlike
 * {@link com.kronotop.bucket.index.maintenance.IndexBuildingTaskState}, no cursor position is
 * persisted, because analysis samples the hint space instead of scanning the whole index.
 * Completed or failed tasks are removed by
 * {@link com.kronotop.bucket.index.maintenance.IndexMaintenanceTaskSweeper}.
 *
 * @see IndexAnalyzeRoutine
 * @see IndexAnalyzeTask
 * @see HistogramCodec
 * @see AbstractTaskState
 */
public class IndexAnalyzeTaskState extends AbstractTaskState {

    /**
     * Creates a new analyze task state with the specified status and error.
     *
     * @param status current task status
     * @param error  error message if failed, null otherwise
     */
    public IndexAnalyzeTaskState(IndexTaskStatus status, String error) {
        super(status, error);
    }

    /**
     * Loads the analyze task state from FoundationDB. Reads only the common status and
     * error fields via {@link AbstractTaskState#loadCommonFields}.
     *
     * @param tr       transaction for reading state
     * @param subspace task subspace
     * @param taskId   task identifier
     * @return loaded analyze task state
     */
    public static IndexAnalyzeTaskState load(Transaction tr, DirectorySubspace subspace, Versionstamp taskId) {
        Map<String, byte[]> entries = TaskStorage.getStateFields(tr, subspace, taskId);
        TaskStateFields fields = loadCommonFields(entries);
        return new IndexAnalyzeTaskState(fields.status(), fields.error());
    }
}