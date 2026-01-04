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
import com.kronotop.bucket.index.statistics.IndexStatsBuilder;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.volume.VersionstampedKeySelector;
import com.kronotop.volume.VolumeEntry;
import com.kronotop.volume.VolumeSession;
import io.github.resilience4j.retry.Retry;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routine for building secondary indexes on existing bucket data in the background.
 *
 * <p>Implements incremental index building by processing documents in batches while tracking
 * progress through a cursor-based state mechanism. The routine operates asynchronously without
 * blocking normal bucket operations.
 *
 * <p><strong>Process Flow:</strong>
 * <ol>
 *   <li>Initialize cursor to lower boundary versionstamp from task</li>
 *   <li>Process documents in batches of 100 from volume storage</li>
 *   <li>Extract index values using selector and insert index entries</li>
 *   <li>Update cursor position after each batch</li>
 *   <li>Complete when cursor reaches upper boundary or no more documents</li>
 * </ol>
 *
 * <p><strong>State Management:</strong> Task state persisted through {@link IndexBuildingTaskState}:
 * <ul>
 *   <li>Cursor versionstamp for resumable processing</li>
 *   <li>Task status: RUNNING, COMPLETED, FAILED, STOPPED</li>
 *   <li>Bootstrap flag to track initial cursor positioning</li>
 *   <li>Error messages for failed tasks</li>
 * </ul>
 *
 * <p><strong>Index Status Lifecycle:</strong> Updates index status in bucket metadata:
 * <ul>
 *   <li>WAITING → BUILDING when routine starts processing</li>
 *   <li>BUILDING → READY when task completes (handled by IndexMaintenanceWatchDog)</li>
 *   <li>DROPPED causes routine to exit early</li>
 * </ul>
 *
 * <p><strong>Error Handling:</strong>
 * <ul>
 *   <li>Transaction conflicts automatically retried via {@link RetryMethods}</li>
 *   <li>IndexMaintenanceRoutineException marks task as FAILED with error message</li>
 *   <li>Documents with type mismatches or missing selectors silently skipped</li>
 *   <li>Routine can be stopped via {@link #stop()} method</li>
 * </ul>
 *
 * <p><strong>Batch Processing:</strong> Each iteration processes up to 100 documents in a single
 * transaction. The cursor advances after each successful batch, enabling resumption after
 * interruption or failure.
 *
 * @see IndexMaintenanceRoutine
 * @see IndexBuildingTask
 * @see IndexBuildingTaskState
 * @see AbstractIndexMaintenanceRoutine
 */
public class IndexBuildingRoutine extends AbstractIndexMaintenanceRoutine {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexBuildingRoutine.class);
    private final static int INDEX_SCAN_BATCH_SIZE = 100;
    private final boolean strictTypes = context.getConfig().getBoolean("bucket.index.maintenance.strict_types");
    private final int shardId;
    private final IndexBuildingTask task;
    private final BucketService service;

    public IndexBuildingRoutine(
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
     * Updates the index task status in FoundationDB with retry logic.
     *
     * <p>This method atomically updates the task's status. The operation is
     * wrapped in a retry mechanism to handle transient FoundationDB conflicts.
     *
     * @param status the new status to set for the index task
     */
    private void setIndexTaskStatus(IndexTaskStatus status) {
        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexBuildingTaskState.setStatus(tr, subspace, taskId, status);
                commit(tr);
            }
        });
    }

    /**
     * Marks the index building task as failed with an error message.
     *
     * <p>Atomically updates task state to FAILED and records the error message.
     * Uses retry logic to handle transient FoundationDB conflicts during state updates.
     *
     * @param th the throwable containing the error message to record
     */
    private void markIndexBuildTaskFailed(Throwable th) {
        RetryMethods.retry(RetryMethods.TRANSACTION).executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexBuildingTaskState.setError(tr, subspace, taskId, th.getMessage());
                IndexBuildingTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.FAILED);
                commit(tr);
            }
        });
    }

    /**
     * Transitions index status to BUILDING if currently in WAITING or FAILED state.
     *
     * <p>Loads the latest index definition and updates its status to BUILDING, then publishes
     * a bucket metadata updated event. Skips update if index is already BUILDING or DROPPED.
     *
     * @param metadata      bucket metadata containing the index
     * @param indexSubspace directory subspace of the target index
     */
    private void setIndexStatusAsBuilding(BucketMetadata metadata, DirectorySubspace indexSubspace) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexDefinition latestVersion = IndexUtil.loadIndexDefinition(tr, indexSubspace);
            if (latestVersion.status() == IndexStatus.DROPPED || latestVersion.status() == IndexStatus.BUILDING) {
                return;
            }
            IndexDefinition definition = latestVersion.updateStatus(IndexStatus.BUILDING);
            IndexUtil.saveIndexDefinition(tr, metadata, definition);
            BucketMetadataUtil.publishBucketMetadataUpdatedEvent(new TransactionalContext(context, tr), metadata);
            commit(tr);
        }
    }

    /**
     * Core loop that scans volume storage and builds the secondary index incrementally.
     *
     * <p>Runs continuously until completion or interruption, processing documents in batches
     * of 100 per transaction. Each iteration:
     * <ol>
     *   <li>Loads fresh bucket metadata to detect index changes</li>
     *   <li>Validates index exists and is not READY or DROPPED</li>
     *   <li>Transitions index status to BUILDING if needed</li>
     *   <li>Loads task state and checks for completion conditions</li>
     *   <li>Processes one batch via {@link #indexBucketEntries}</li>
     *   <li>Updates metrics and continues to next batch</li>
     * </ol>
     *
     * <p><strong>Completion Conditions:</strong>
     * <ul>
     *   <li>Task status already COMPLETED or STOPPED</li>
     *   <li>Cursor reaches upper boundary versionstamp</li>
     *   <li>No cursor versionstamp found (empty shard)</li>
     *   <li>Batch processing returns 0 entries</li>
     *   <li>Index status becomes DROPPED</li>
     *   <li>stopped flag set via {@link #stop()}</li>
     * </ul>
     *
     * <p><strong>Error Handling:</strong> Catches {@link IndexMaintenanceRoutineException}
     * to mark task as FAILED and exit loop. Transaction conflicts handled by outer retry logic.
     *
     * @throws IndexMaintenanceRoutineException if index not found or already READY
     */
    private void buildSecondaryIndex() {
        BucketShard shard = service.getShard(shardId);
        while (!stopped || !Thread.currentThread().isInterrupted()) {
            // Fetch the metadata with an independent TX due to prevent conflicts during commit time.
            BucketMetadata metadata = TransactionUtils.execute(context, tr ->
                    BucketMetadataUtil.openUncached(context, tr, task.getNamespace(), task.getBucket())
            );
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                tr.options().setPriorityBatch();
                Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
                if (index == null) {
                    throw new IndexMaintenanceRoutineException("no index found with id " + task.getIndexId());
                }

                if (index.definition().status() == IndexStatus.READY) {
                    throw new IndexMaintenanceRoutineException(String.format(
                            "Index with selector=%s, id=%d is already ready to query",
                            index.definition().selector(),
                            index.definition().id()
                    ));
                } else if (index.definition().status() == IndexStatus.DROPPED) {
                    LOGGER.debug("Index with namespace={}, bucket={}, selector={}, id={} is dropped",
                            task.getNamespace(),
                            task.getBucket(),
                            index.definition().selector(),
                            index.definition().id()
                    );
                    break;
                }

                // Three possibilities for IndexStatus: WAITING, BUILDING, FAILED
                IndexStatus status = index.definition().status();
                if (status == IndexStatus.WAITING || status == IndexStatus.FAILED) {
                    Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
                    retry.executeRunnable(() -> setIndexStatusAsBuilding(metadata, index.subspace()));
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
                    // The operator marked the task as STOPPED manually.
                    LOGGER.debug(
                            "Background index builder for namespace={}, bucket={}, index={} has been stopped by the operator",
                            task.getNamespace(),
                            task.getBucket(),
                            task.getIndexId()
                    );
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
                    // All entries are processed. End of the task.
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
                    // All entries are processed. End of the task.
                    setIndexTaskStatus(IndexTaskStatus.COMPLETED);
                    break;
                }

                int processedEntries = indexBucketEntries(tr, shard, metadata, state);
                commit(tr);
                if (processedEntries == 0) {
                    // everything went well and processed zero entry.
                    setIndexTaskStatus(IndexTaskStatus.COMPLETED);
                    break;
                } else {
                    metrics.incrementProcessedEntries(processedEntries);
                }
            } catch (IndexMaintenanceRoutineException | IndexTypeMismatchException exp) {
                LOGGER.error("TaskId: {} on Bucket shard: {} has failed due to an error: '{}'",
                        VersionstampUtil.base32HexEncode(taskId),
                        shardId,
                        exp.getMessage()
                );
                markIndexBuildTaskFailed(exp);
                break;
            } finally {
                metrics.setLatestExecution(System.currentTimeMillis());
            }
        }
    }

    /**
     * Processes a batch of up to 100 documents from volume storage and creates index entries.
     *
     * <p>Reads documents from the volume using cursor-based range scan, extracts index values
     * via selector matching, and inserts index entries into FoundationDB. Updates cursor and
     * bootstrap flag within the same transaction.
     *
     * <p><strong>Cursor Positioning:</strong>
     * <ul>
     *   <li>First batch (not bootstrapped): Uses firstGreaterOrEqual to include cursor document</li>
     *   <li>Subsequent batches: Uses firstGreaterThan to skip cursor document</li>
     *   <li>Range ends at task upper boundary (exclusive)</li>
     * </ul>
     *
     * <p><strong>Document Processing:</strong>
     * <ul>
     *   <li>Matches selector against document to extract BSON value</li>
     *   <li>Converts BSON value to Java object based on index BSON type</li>
     *   <li>Skips document if selector doesn't match or type conversion fails</li>
     *   <li>Inserts index entry with shard ID and document metadata</li>
     *   <li>Inserts statistics hint for index optimization</li>
     * </ul>
     *
     * @param tr       transaction for index writes and state updates
     * @param shard    bucket shard containing the volume to scan
     * @param metadata bucket metadata with index definitions
     * @param state    current task state with cursor and bootstrap flag
     * @return number of documents processed (may exceed entries created due to skipped docs)
     */
    private int indexBucketEntries(Transaction tr, BucketShard shard, BucketMetadata metadata, IndexBuildingTaskState state) {
        int total = 0;

        VersionstampedKeySelector begin = !state.bootstrapped() ?
                VersionstampedKeySelector.firstGreaterOrEqual(state.cursorVersionstamp()) :
                VersionstampedKeySelector.firstGreaterThan(state.cursorVersionstamp());

        VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterOrEqual(task.getUpper());
        VolumeSession session = new VolumeSession(tr, metadata.volumePrefix());

        Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.READWRITE);
        Iterable<VolumeEntry> entries = shard.volume().getRange(session, begin, end, INDEX_SCAN_BATCH_SIZE);
        Versionstamp versionstamp = null;
        for (VolumeEntry pair : entries) {
            checkForShutdown();

            total++;
            versionstamp = pair.key();
            Object indexValue = null;
            BsonValue bsonValue = SelectorMatcher.match(index.definition().selector(), pair.entry());
            if (bsonValue != null && !bsonValue.equals(BsonNull.VALUE)) {
                indexValue = BSONUtil.toObject(bsonValue, index.definition().bsonType());
                if (indexValue == null) {
                    if (!strictTypes) {
                        // Type mismatch, continue
                        continue;
                    }
                    throw new IndexTypeMismatchException(index.definition(), bsonValue);
                }
            }

            IndexBuilder.insertIndexEntry(tr, index.definition(), metadata, versionstamp, indexValue, shardId, pair.metadata());
            IndexStatsBuilder.insertHintForStats(tr, versionstamp, index, bsonValue);
        }

        if (!state.bootstrapped()) {
            IndexBuildingTaskState.setBootstrapped(tr, subspace, taskId, true);
        }

        setCursor(tr, versionstamp);
        return total;
    }

    /**
     * Persists cursor versionstamp to task state for resumable processing.
     *
     * <p>Updates cursor only if non-null. The cursor represents the last processed
     * document's versionstamp, enabling the routine to resume from this position
     * after interruption or failure.
     *
     * @param tr     transaction for state update
     * @param cursor versionstamp of last processed document, or null to skip update
     */
    private void setCursor(Transaction tr, Versionstamp cursor) {
        if (cursor != null) {
            IndexBuildingTaskState.setCursorVersionstamp(tr, subspace, taskId, cursor);
        }
    }

    private void initialize() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] definition = TaskStorage.getDefinition(tr, subspace, taskId);
            if (definition == null) {
                IndexBuildingTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.STOPPED);
            } else {
                IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, subspace, taskId);
                if (state.status() == IndexTaskStatus.STOPPED || state.status() == IndexTaskStatus.COMPLETED) {
                    stopped = true;
                    // Already completed or stopped
                    return;
                }
                if (state.cursorVersionstamp() == null) {
                    IndexBuildingTaskState.setCursorVersionstamp(tr, subspace, taskId, task.getLower());
                }
                IndexBuildingTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.RUNNING);
            }
            commit(tr);
        }
    }

    /**
     * Initiates the index building process for this shard.
     *
     * <p>Sets task status to RUNNING, initializes cursor to lower boundary if needed,
     * then begins the index scan loop via {@link #buildSecondaryIndex()}. The method is
     * idempotent and can be called multiple times to resume from saved cursor position.
     *
     * <p><strong>Initialization Steps:</strong>
     * <ol>
     *   <li>Reset stopped flag to allow resumption after {@link #stop()}</li>
     *   <li>Update task status to RUNNING with retry logic</li>
     *   <li>Initialize cursor to task lower boundary if null</li>
     *   <li>Start scanning loop that processes batches until completion</li>
     * </ol>
     *
     * <p><strong>Retry Behavior:</strong> Both cursor initialization and scan loop wrapped
     * in retry logic to handle FoundationDB transaction conflicts automatically.
     */
    public void start() {
        LOGGER.debug(
                "Starting to build namespace={}, bucket={}, index={} on Bucket shard={} at the background",
                task.getNamespace(),
                task.getBucket(),
                task.getIndexId(),
                shardId
        );
        stopped = false; // also means a restart
        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);

        retry.executeRunnable(this::initialize);
        if (!stopped) {
            retry.executeRunnable(this::buildSecondaryIndex);
        }
    }
}
