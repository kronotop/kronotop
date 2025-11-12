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

package com.kronotop.bucket.index.statistics;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.index.maintenance.AbstractTaskState;
import com.kronotop.bucket.index.maintenance.IndexTaskStatus;
import com.kronotop.internal.task.TaskStorage;

import java.util.Map;

/**
 * State tracker for index statistics collection tasks.
 *
 * <p>IndexAnalyzeTaskState manages the lifecycle of index analysis operations that build
 * histogram statistics for query optimization. Analyze tasks scan index entries and
 * construct value distribution histograms to help the query planner estimate selectivity.
 *
 * <p><strong>Analyze Task Purpose:</strong>
 * <ul>
 *   <li>Build histogram statistics from index hint space</li>
 *   <li>Calculate value distribution for query optimizer</li>
 *   <li>Store histogram in bucket metadata for planner access</li>
 *   <li>Update index cardinality statistics</li>
 * </ul>
 *
 * <p><strong>State Lifecycle:</strong>
 * <pre>
 * WAITING → RUNNING → COMPLETED (success)
 *                  → FAILED (error during analysis)
 *                  → STOPPED (manual cancellation)
 * </pre>
 *
 * <p><strong>State Fields:</strong>
 * <ul>
 *   <li>status: Task execution status (inherited from AbstractTaskState)</li>
 *   <li>error: Error message if failed (inherited from AbstractTaskState)</li>
 * </ul>
 *
 * <p>Analyze tasks track only status and error. Unlike {@link com.kronotop.bucket.index.maintenance.IndexBuildingTaskState},
 * they don't persist cursor position as analysis operations complete quickly by sampling
 * the hint space rather than scanning the entire index.
 *
 * <p><strong>Cleanup:</strong> Completed or failed analyze tasks are removed by
 * {@link com.kronotop.bucket.index.maintenance.IndexMaintenanceTaskSweeper} after
 * histogram storage.
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
     * @param error error message if failed, null otherwise
     */
    public IndexAnalyzeTaskState(IndexTaskStatus status, String error) {
        super(status, error);
    }

    /**
     * Loads the analyze task state from FoundationDB.
     *
     * <p>Retrieves common state fields (status and error) using the parent class
     * {@link AbstractTaskState#loadCommonFields}. No additional fields are loaded
     * as analyze tasks only track execution status.
     *
     * @param tr transaction for reading state
     * @param subspace task subspace
     * @param taskId task identifier
     * @return loaded analyze task state
     */
    public static IndexAnalyzeTaskState load(Transaction tr, DirectorySubspace subspace, Versionstamp taskId) {
        Map<String, byte[]> entries = TaskStorage.getStateFields(tr, subspace, taskId);
        TaskStateFields fields = loadCommonFields(entries);
        return new IndexAnalyzeTaskState(fields.status(), fields.error());
    }
}