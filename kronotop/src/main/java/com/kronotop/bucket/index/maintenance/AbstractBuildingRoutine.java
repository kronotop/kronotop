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
import com.kronotop.bucket.*;
import com.kronotop.bucket.index.IndexHolder;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.namespace.NamespaceBeingRemovedException;
import com.kronotop.transaction.TransactionUtil;
import io.github.resilience4j.retry.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for index building routines that process existing bucket data in the background.
 *
 * <p>Handles the common lifecycle: initialization, cursor management, batch scanning loop,
 * status transitions, and error handling. Subclasses provide index lookup and per-document
 * entry insertion logic.
 *
 * @see SingleFieldIndexBuildingRoutine
 * @see CompoundIndexBuildingRoutine
 */
public abstract class AbstractBuildingRoutine extends AbstractIndexMaintenanceRoutine {
    protected static final int INDEX_SCAN_BATCH_SIZE = 100;
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBuildingRoutine.class);
    protected final boolean strictTypes = context.getConfig().getBoolean("bucket.index.strict_types");
    protected final int shardId;
    protected final IndexBuildingTask task;
    protected final BucketService service;

    protected AbstractBuildingRoutine(
            Context context,
            DirectorySubspace subspace,
            int shardId,
            Versionstamp taskId,
            IndexBuildingTask task
    ) {
        super(context, subspace, taskId);
        this.shardId = shardId;
        this.task = task;
        this.service = context.getService(BucketService.NAME);
    }

    /**
     * Looks up the index from bucket metadata. Returns null if not found.
     */
    protected abstract IndexHolder<?> lookupIndex(BucketMetadata metadata);

    /**
     * Processes one batch of volume entries and inserts index entries.
     *
     * @return number of documents processed
     */
    protected abstract int indexBucketEntries(Transaction tr, BucketShard shard,
                                              BucketMetadata metadata, IndexBuildingTaskState state);

    private void setIndexTaskStatus(IndexTaskStatus status) {
        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexBuildingTaskState.setStatus(tr, subspace, taskId, status);
                commit(tr);
            }
        });
    }

    private void markTaskFailed(Throwable th) {
        RetryMethods.retry(RetryMethods.TRANSACTION).executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexBuildingTaskState.setError(tr, subspace, taskId, th.getMessage());
                IndexBuildingTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.FAILED);
                commit(tr);
            }
        });
    }

    protected void setCursor(Transaction tr, Versionstamp cursor) {
        if (cursor != null) {
            IndexBuildingTaskState.setCursorVersionstamp(tr, subspace, taskId, cursor);
        }
    }

    private void initialize() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] definition = TaskStorage.getDefinition(tr, subspace, taskId);
            if (definition == null) {
                stopped = true;
                return;
            }
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, subspace, taskId);
            if (state.status() == IndexTaskStatus.STOPPED || state.status() == IndexTaskStatus.COMPLETED) {
                stopped = true;
                return;
            }
            if (state.cursorVersionstamp() == null) {
                IndexBuildingTaskState.setCursorVersionstamp(tr, subspace, taskId, task.getLower());
            }
            IndexBuildingTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.RUNNING);
            commit(tr);
        }
    }

    private void buildIndex() {
        BucketShard shard = service.getShard(shardId);
        while (!stopped && !Thread.currentThread().isInterrupted()) {
            // Fetch the metadata with an independent TX to prevent conflicts during commit time.
            BucketMetadata metadata = TransactionUtil.execute(context, tr ->
                    BucketMetadataUtil.reload(context, tr, task.getNamespace(), task.getBucket())
            );
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                tr.options().setPriorityBatch();
                IndexHolder<?> holder = lookupIndex(metadata);
                if (holder == null) {
                    throw new IndexMaintenanceRoutineException("no index found with id " + task.getIndexId());
                }

                if (holder.definition().status() == IndexStatus.READY) {
                    throw new IndexMaintenanceRoutineException(String.format(
                            "Index with id=%d in namespace=%s, bucket=%s is already ready to query",
                            holder.definition().id(),
                            task.getNamespace(),
                            task.getBucket()
                    ));
                } else if (holder.definition().status() == IndexStatus.DROPPED) {
                    LOGGER.debug("Index with namespace={}, bucket={}, id={} is dropped",
                            task.getNamespace(),
                            task.getBucket(),
                            holder.definition().id()
                    );
                    stopped = true;
                    break;
                }

                IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, subspace, taskId);
                if (state.status() == IndexTaskStatus.COMPLETED) {
                    LOGGER.debug(
                            "Background index builder for namespace={}, bucket={}, index={} on Bucket shard: {} has been completed",
                            task.getNamespace(),
                            task.getBucket(),
                            task.getIndexId(),
                            shardId
                    );
                    break;
                } else if (state.status() == IndexTaskStatus.STOPPED) {
                    LOGGER.debug(
                            "Background index builder for namespace={}, bucket={}, index={} has been stopped by the operator",
                            task.getNamespace(),
                            task.getBucket(),
                            task.getIndexId()
                    );
                    stopped = true;
                    break;
                }

                if (state.cursorVersionstamp() == null) {
                    LOGGER.debug(
                            "Background index builder for namespace={}, bucket={}, index={} on Bucket shard: {} has been completed, no items found",
                            task.getNamespace(),
                            task.getBucket(),
                            task.getIndexId(),
                            shardId
                    );
                    setIndexTaskStatus(IndexTaskStatus.COMPLETED);
                    break;
                }
                if (state.cursorVersionstamp().equals(task.getUpper())) {
                    LOGGER.debug(
                            "Background index builder for namespace={}, bucket={}, index={} on Bucket shard: {} has been completed",
                            task.getNamespace(),
                            task.getBucket(),
                            task.getIndexId(),
                            shardId
                    );
                    setIndexTaskStatus(IndexTaskStatus.COMPLETED);
                    break;
                }

                int processedEntries = indexBucketEntries(tr, shard, metadata, state);
                commit(tr);
                if (processedEntries == 0) {
                    setIndexTaskStatus(IndexTaskStatus.COMPLETED);
                    break;
                } else {
                    metrics.incrementProcessedEntries(processedEntries);
                }
            } catch (NoSuchBucketException | BucketBeingRemovedException |
                     NamespaceBeingRemovedException exp) {
                LOGGER.debug("TaskId: {} on Bucket shard: {} has failed due to bucket/namespace removal: '{}'",
                        VersionstampUtil.base32HexEncode(taskId),
                        shardId,
                        exp.getMessage()
                );
                // Watchdog will detect the bucket is gone and drop the orphaned task.
                stopped = true;
                break;
            } catch (IndexMaintenanceRoutineException | IndexTypeMismatchException exp) {
                LOGGER.error("TaskId: {} on Bucket shard: {} has failed due to an error: '{}'",
                        VersionstampUtil.base32HexEncode(taskId),
                        shardId,
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
                stopped = true;
                break;
            } finally {
                metrics.setLatestExecution(System.currentTimeMillis());
            }
        }
    }

    @Override
    public void start() {
        String routineName = getClass().getSimpleName();
        LOGGER.debug(
                "Starting {} for namespace={}, bucket={}, index={} on Bucket shard={} at the background",
                routineName,
                task.getNamespace(),
                task.getBucket(),
                task.getIndexId(),
                shardId
        );
        stopped = false; // also means a restart
        initialize();
        if (!stopped && !Thread.currentThread().isInterrupted()) {
            buildIndex();
        }
        LOGGER.debug("{} for {}/{}/{} on Bucket shard={} has been completed",
                routineName, task.getNamespace(), task.getBucket(), task.getIndexId(), shardId
        );
    }
}
