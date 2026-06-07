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
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexHolder;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.namespace.NamespaceBeingRemovedException;
import io.github.resilience4j.retry.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for boundary routines that determine scan boundaries for index building
 * and create {@link IndexBuildingTask}s for all shards.
 *
 * <p>Subclasses provide the index lookup, definition persistence, and building task creation
 * for their specific index type.
 *
 * @see IndexBoundaryRoutine
 * @see CompoundIndexBoundaryRoutine
 * @see BoundaryLocator
 */
public abstract class AbstractBoundaryRoutine extends AbstractIndexMaintenanceRoutine {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBoundaryRoutine.class);
    protected final IndexBoundaryTask task;

    protected AbstractBoundaryRoutine(Context context,
                                      DirectorySubspace subspace,
                                      Versionstamp taskId,
                                      IndexBoundaryTask task) {
        super(context, subspace, taskId);
        this.task = task;
    }

    /**
     * Looks up the index from bucket metadata.
     *
     * @return the index holder, or null if not found
     */
    protected abstract IndexHolder<?> lookupIndex(BucketMetadata metadata);

    /**
     * Saves the updated index definition.
     */
    protected abstract void saveDefinition(Transaction tr, BucketMetadata metadata, IndexDefinition definition);

    /**
     * Creates building tasks for each shard.
     */
    protected abstract void createBuildingTasks(TransactionalContext tx, BucketMetadata metadata, Boundaries boundaries);

    private void markTaskFailed(Throwable th) {
        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexBoundaryTaskState.setError(tr, subspace, taskId, th.getMessage());
                IndexBoundaryTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.FAILED);
                commit(tr);
            }
        });
    }

    private void markTaskCompleted(Transaction tr) {
        IndexBoundaryTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.COMPLETED);
    }

    private void initialize() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] definition = TaskStorage.getDefinition(tr, subspace, taskId);
            if (definition == null) {
                stopped = true;
                return;
            }
            IndexBoundaryTaskState state = IndexBoundaryTaskState.load(tr, subspace, taskId);
            if (state.status() == IndexTaskStatus.STOPPED || state.status() == IndexTaskStatus.COMPLETED) {
                stopped = true;
                return;
            }
            IndexBoundaryTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.RUNNING);
            commit(tr);
        }
    }

    private void startInternal() {
        if (stopped || Thread.currentThread().isInterrupted()) {
            return;
        }

        try {
            BucketMetadataConvergence.await(context, task.getNamespace(), task.getBucket());

            boolean emptyBucket;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = BucketMetadataUtil.reload(context, tr.snapshot(), task.getNamespace(), task.getBucket());

                IndexHolder<?> holder = lookupIndex(metadata);
                if (holder == null) {
                    throw new IndexMaintenanceRoutineException("Index could not be found");
                }

                Boundaries boundaries = BoundaryLocator.locate(tr.snapshot(), metadata, holder.subspace());
                emptyBucket = (boundaries.lower() == null && boundaries.upper() == null);

                // Always transition to BUILDING first — even for empty buckets.
                // This ensures all nodes observe a READWRITE-visible state before READY.
                saveDefinition(tr, metadata, holder.definition().updateStatus(IndexStatus.BUILDING));
                TransactionalContext tx = new TransactionalContext(context, tr);
                BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);

                if (!emptyBucket) {
                    createBuildingTasks(tx, metadata, boundaries);
                }
                markTaskCompleted(tr);
                commit(tr);
            }

            BucketMetadataConvergence.await(context, task.getNamespace(), task.getBucket());

            if (emptyBucket) {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    BucketMetadata fresh = BucketMetadataUtil.reload(context, tr.snapshot(), task.getNamespace(), task.getBucket());
                    IndexHolder<?> freshHolder = lookupIndex(fresh);
                    if (freshHolder != null) {
                        saveDefinition(tr, fresh, freshHolder.definition().updateStatus(IndexStatus.READY));
                        BucketMetadataUtil.publishBucketMetadataUpdatedEvent(new TransactionalContext(context, tr), fresh);
                        commit(tr);
                    }
                }
            }
        } catch (InvalidTaskStateException e) {
            LOGGER.error("Failed due to invalid task state: {}", e.getMessage());
        } catch (InterruptedException exp) {
            // Do not mark the task as failed -- allow retry after restart
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
                markTaskFailed(exp);
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

    @Override
    public void start() {
        String routineName = getClass().getSimpleName();
        LOGGER.debug("Running {} for {}/{}/{}",
                routineName, task.getNamespace(), task.getBucket(), task.getIndexId()
        );
        stopped = false; // also means a restart
        initialize();
        if (!stopped && !Thread.currentThread().isInterrupted()) {
            startInternal();
        }
        LOGGER.debug("{} for {}/{}/{} has been completed",
                routineName, task.getNamespace(), task.getBucket(), task.getIndexId()
        );
    }
}
