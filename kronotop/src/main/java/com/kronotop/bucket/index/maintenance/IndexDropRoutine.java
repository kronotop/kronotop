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
import com.kronotop.Context;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.*;
import com.kronotop.bucket.index.*;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.namespace.NamespaceBeingRemovedException;
import io.github.resilience4j.retry.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background routine that executes index drop operations asynchronously.
 *
 * <p>This routine removes all index entries from FoundationDB storage and manages
 * task state transitions through the drop lifecycle. It ensures transactional
 * consistency and handles failures by updating task status appropriately.
 */
public class IndexDropRoutine extends AbstractIndexMaintenanceRoutine {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexDropRoutine.class);
    private final IndexDropTask task;

    /**
     * Creates a new index drop routine.
     *
     * @param context  the Kronotop context providing access to services
     * @param subspace the directory subspace containing task metadata
     * @param taskId   the unique versionstamp identifier for this task
     * @param task     the drop task definition specifying the target index
     */
    public IndexDropRoutine(Context context,
                            DirectorySubspace subspace,
                            Versionstamp taskId,
                            IndexDropTask task) {
        super(context, subspace, taskId);
        this.task = task;
    }

    /**
     * Marks the index drop task as failed and records the error message.
     *
     * @param th the throwable that caused the task to fail
     */
    private void markIndexDropTaskFailed(Throwable th) {
        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexDropTaskState.setError(tr, subspace, taskId, th.getMessage());
                IndexDropTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.FAILED);
                commit(tr);
            }
        });
    }

    /**
     * Marks the index drop task as completed within the provided transaction.
     *
     * @param tr the transaction to use for updating task status
     */
    private void markIndexDropTaskCompleted(Transaction tr) {
        IndexDropTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.COMPLETED);
    }

    /**
     * Removes all index entries from storage. Handles both single-field and vector indexes.
     * No-op if the index no longer exists.
     *
     * @param tr       the transaction to use for the clear operation
     * @param metadata the bucket metadata containing index definitions
     */
    private void clearIndex(Transaction tr, BucketMetadata metadata) {
        TransactionalContext tx = new TransactionalContext(context, tr);

        Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
        if (index != null) {
            SingleFieldIndexUtil.clear(tr, metadata.subspace(), index.definition().name());
            BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);
            return;
        }

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
        if (compoundIndex != null) {
            CompoundIndexUtil.clear(tr, metadata.subspace(), compoundIndex.definition().name());
            BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);
            return;
        }

        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
        if (vectorIndex != null) {
            VectorIndexUtil.clear(tr, metadata.subspace(), vectorIndex.definition().name());
            BucketMetadataUtil.publishVectorIndexDroppedEvent(tx, metadata, task.getIndexId());
            BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);
        }
    }

    /**
     * Executes the index drop operation with transaction isolation and error handling.
     *
     * <p>This method validates the task state, refreshes metadata to ensure transaction isolation,
     * removes all index entries, and updates task status to reflect completion or failure.
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
                BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, task.getNamespace(), task.getBucket());
                Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
                CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
                VectorIndex vectorIndex = metadata.vectorIndexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
                if (index == null && compoundIndex == null && vectorIndex == null) {
                    throw new IndexMaintenanceRoutineException("Index could not be found");
                }
                clearIndex(tr, metadata);
                markIndexDropTaskCompleted(tr);
                commit(tr);
            }
            LOGGER.debug(
                    "Index={} on namespace={}, bucket={} has been dropped",
                    task.getIndexId(),
                    task.getNamespace(),
                    task.getBucket()
            );
        } catch (InterruptedException exp) {
            // Do not mark the task as failed. Program has stopped and this task
            // can be retried.
            Thread.currentThread().interrupt();
            throw new IndexMaintenanceRoutineShutdownException();
        } catch (NoSuchBucketException | BucketBeingRemovedException |
                 NamespaceBeingRemovedException exp) {
            LOGGER.debug("TaskId: {} has failed due to bucket/namespace removal: '{}'",
                    VersionstampUtil.base32HexEncode(taskId),
                    exp.getMessage()
            );
            // Watchdog will detect the bucket is gone and drop the orphaned task.
        } catch (IndexMaintenanceRoutineException | BucketMetadataConvergenceException exp) {
            LOGGER.error("TaskId: {} has failed due to an error: '{}'",
                    VersionstampUtil.base32HexEncode(taskId),
                    exp.getMessage()
            );
            try {
                markIndexDropTaskFailed(exp);
            } catch (Exception suppressed) {
                LOGGER.debug("Could not mark task {} as FAILED: {}",
                        VersionstampUtil.base32HexEncode(taskId),
                        suppressed.getMessage()
                );
                throw new IndexMaintenanceRoutineShutdownException();
            }
        } finally {
            metrics.setLatestExecution(System.currentTimeMillis());
        }
    }

    private void initialize() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] definition = TaskStorage.getDefinition(tr, subspace, taskId);
            if (definition == null) {
                stopped = true;
                return;
            }
            IndexDropTaskState state = IndexDropTaskState.load(tr, subspace, taskId);
            if (state.status() == IndexTaskStatus.STOPPED || state.status() == IndexTaskStatus.COMPLETED) {
                // Already completed or stopped
                stopped = true;
                return;
            }
            IndexDropTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.RUNNING);
            commit(tr);
        }
    }

    /**
     * Initiates the index drop routine with automatic retry on transient failures.
     *
     * <p>This method resets the stopped flag to enable restarts and delegates execution
     * to {@link #startInternal()}. Transient failures are retried at the worker level.
     */
    @Override
    public void start() {
        LOGGER.debug(
                "Dropping index={} on namespace={}, bucket={}",
                task.getIndexId(),
                task.getNamespace(),
                task.getBucket()
        );
        stopped = false; // also means a restart
        initialize();
        if (!stopped && !Thread.currentThread().isInterrupted()) {
            startInternal();
        }
    }
}
