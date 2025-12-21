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
import com.kronotop.Context;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.*;
import com.kronotop.bucket.index.*;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.internal.task.TaskStorage;
import io.github.resilience4j.retry.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routine that determines scan boundaries for index building and either marks the index as READY
 * or creates building tasks for all shards.
 *
 * <p>This routine runs as the first step in the background index creation workflow. It waits for
 * bucket metadata convergence across the cluster, locates boundary versionstamps in the primary
 * index, and decides the next action based on whether data exists in the bucket.
 *
 * <p><strong>Purpose:</strong> Coordinates the transition from index creation to index building
 * by determining scan boundaries and creating per-shard building tasks.
 *
 * <p><strong>Workflow:</strong>
 * <ol>
 *   <li>Wait for bucket metadata convergence via {@link BucketMetadataConvergence}</li>
 *   <li>Locate lower and upper boundary versionstamps via {@link BoundaryLocator}</li>
 *   <li>If boundaries are null (empty bucket): Mark index as READY immediately</li>
 *   <li>If boundaries exist: Create {@link IndexBuildingTask} for each shard</li>
 *   <li>Mark boundary task as COMPLETED</li>
 * </ol>
 *
 * <p><strong>Metadata Convergence:</strong> Waits up to 60 seconds for all cluster nodes to
 * synchronize their cached bucket metadata. This ensures all nodes see the new index definition
 * before building begins.
 *
 * <p><strong>Boundary Semantics:</strong>
 * <ul>
 *   <li>Lower boundary: First versionstamp in primary index (inclusive)</li>
 *   <li>Upper boundary: One position beyond last versionstamp (exclusive)</li>
 *   <li>Both null: No documents exist, index immediately READY</li>
 *   <li>Both non-null: Document range to scan across all shards</li>
 * </ul>
 *
 * <p><strong>Task Orchestration:</strong> Creates one building task per shard with identical
 * boundaries. Each shard processes its portion of the volume independently using the same
 * versionstamp range.
 *
 * <p><strong>Error Handling:</strong>
 * <ul>
 *   <li>InterruptedException: Does not mark task as failed, allows retry on restart</li>
 *   <li>IndexMaintenanceRoutineException: Marks task as FAILED with error message</li>
 *   <li>Transaction conflicts: Automatically retried via {@link RetryMethods}</li>
 * </ul>
 *
 * @see IndexBoundaryTask
 * @see BoundaryLocator
 * @see BucketMetadataConvergence
 * @see IndexBuildingRoutine
 * @see AbstractIndexMaintenanceRoutine
 */
public class IndexBoundaryRoutine extends AbstractIndexMaintenanceRoutine {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexBoundaryRoutine.class);
    private final IndexBoundaryTask task;

    public IndexBoundaryRoutine(Context context,
                                DirectorySubspace subspace,
                                Versionstamp taskId,
                                IndexBoundaryTask task) {
        super(context, subspace, taskId);
        this.task = task;
    }

    /**
     * Marks the boundary task as FAILED with an error message.
     *
     * <p>Updates task state to FAILED and records the error message in a new transaction.
     * Does not use retry logic since it's called from exception handlers.
     *
     * @param th throwable containing the error message to record
     */
    private void markIndexBoundaryTaskFailed(Throwable th) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBoundaryTaskState.setError(tr, subspace, taskId, th.getMessage());
            IndexBoundaryTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.FAILED);
            commit(tr);
        }
    }

    /**
     * Marks the boundary task as COMPLETED within the given transaction.
     *
     * @param tr transaction for state update
     */
    private void markIndexBoundaryTaskCompleted(Transaction tr) {
        IndexBoundaryTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.COMPLETED);
    }

    private void initialize() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] definition = TaskStorage.getDefinition(tr, subspace, taskId);
            if (definition == null) {
                IndexBoundaryTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.STOPPED);
            } else {
                IndexBoundaryTaskState state = IndexBoundaryTaskState.load(tr, subspace, taskId);
                if (state.status() == IndexTaskStatus.STOPPED || state.status() == IndexTaskStatus.COMPLETED) {
                    stopped = true;
                    // Already completed or stopped
                    return;
                }
                IndexBoundaryTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.RUNNING);
            }
            commit(tr);
        }
    }

    /**
     * Core logic that determines scan boundaries and orchestrates next steps.
     *
     * <p>Performs the boundary detection workflow with early exit conditions for completed
     * or stopped tasks. Waits for metadata convergence, locates boundaries, and either
     * marks the index as READY or creates building tasks.
     *
     * <p><strong>Execution Steps:</strong>
     * <ol>
     *   <li>Exit early if routine already stopped</li>
     *   <li>Check task definition exists and status is not COMPLETED</li>
     *   <li>Update task status to RUNNING</li>
     *   <li>Wait for bucket metadata convergence (up to 60 seconds)</li>
     *   <li>Force-load bucket metadata to get latest index definitions</li>
     *   <li>Validate index exists in metadata</li>
     *   <li>Locate scan boundaries in primary index</li>
     *   <li>If boundaries null: Mark index READY and complete task</li>
     *   <li>If boundaries exist: Create building tasks for all shards</li>
     * </ol>
     *
     * <p><strong>Empty Bucket Optimization:</strong> When both boundaries are null, the bucket
     * contains no documents. The routine immediately marks the index as READY, saves the updated
     * definition, and publishes a metadata event—bypassing the building phase entirely.
     *
     * <p><strong>Building Task Creation:</strong> Creates one {@link IndexBuildingTask} per shard
     * with identical lower/upper boundaries. Each shard independently processes its volume
     * partition within the specified versionstamp range.
     *
     * <p><strong>Exception Handling:</strong>
     * <ul>
     *   <li>InterruptedException: Preserves interrupt status, throws shutdown exception without
     *       marking task as failed to allow retry after restart</li>
     *   <li>IndexMaintenanceRoutineException: Logs error and marks task as FAILED</li>
     * </ul>
     *
     * @throws IndexMaintenanceRoutineShutdownException if interrupted during execution
     */
    private void startInternal() {
        if (stopped || Thread.currentThread().isInterrupted()) {
            return;
        }

        try {
            BucketMetadataConvergence.await(context, task.getNamespace(), task.getBucket());
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = BucketMetadataUtil.openUncached(context, tr, task.getNamespace(), task.getBucket());

                Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
                if (index == null) {
                    throw new IndexMaintenanceRoutineException("Index could not be found");
                }

                Boundaries boundaries = BoundaryLocator.locate(tr, metadata, task.getIndexId());
                if (boundaries.lower() == null && boundaries.upper() == null) {
                    // Empty bucket: mark index READY immediately
                    IndexDefinition definition = index.definition().updateStatus(IndexStatus.READY);
                    IndexUtil.saveIndexDefinition(tr, metadata, definition);
                    BucketMetadataUtil.publishBucketMetadataUpdatedEvent(new TransactionalContext(context, tr), metadata);
                } else {
                    // Non-empty bucket: create building tasks for all shards
                    TransactionalContext tx = new TransactionalContext(context, tr);
                    IndexUtil.createIndexBuildingTasks(tx, metadata, task.getIndexId(), boundaries);
                }
                markIndexBoundaryTaskCompleted(tr);
                commit(tr);
            }
        } catch (InvalidTaskStateException e) {
            LOGGER.error("Failed due to invalid task state: {}", e.getMessage());
        } catch (InterruptedException exp) {
            // Do not mark task as failed—allow retry after restart
            Thread.currentThread().interrupt();
            throw new IndexMaintenanceRoutineShutdownException();
        } catch (IndexMaintenanceRoutineException | BucketMetadataConvergenceException exp) {
            LOGGER.error("TaskId: {} has failed due to an error: '{}'",
                    VersionstampUtil.base32HexEncode(taskId),
                    exp.getMessage()
            );
            markIndexBoundaryTaskFailed(exp);
        } finally {
            metrics.setLatestExecution(System.currentTimeMillis());
        }
    }

    /**
     * Starts the boundary detection routine with automatic retry logic.
     *
     * <p>Resets the stopped flag to allow resumption after {@link #stop()}, then executes
     * the boundary detection workflow via {@link #startInternal()}. Transaction conflicts are
     * automatically retried using {@link RetryMethods}.
     *
     * <p>Logs debug messages at start and completion to track routine execution in the
     * maintenance task pipeline.
     */
    @Override
    public void start() {
        LOGGER.debug("Running IndexBoundaryRoutine for {}/{}/{}",
                task.getNamespace(), task.getBucket(), task.getIndexId()
        );
        stopped = false; // also means a restart
        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(this::initialize);
        if (!stopped || !Thread.currentThread().isInterrupted()) {
            retry.executeRunnable(this::startInternal);
        }
        LOGGER.debug("IndexBoundaryRoutine for {}/{}/{} has been completed",
                task.getNamespace(), task.getBucket(), task.getIndexId()
        );
    }
}
