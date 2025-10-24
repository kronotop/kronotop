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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.*;
import com.kronotop.bucket.index.*;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.volume.VersionstampedKeySelector;
import com.kronotop.volume.VolumeEntry;
import com.kronotop.volume.VolumeSession;
import io.github.resilience4j.retry.Retry;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Background routine for building secondary indexes on existing bucket data.
 *
 * <p>This routine implements the core logic for asynchronously building secondary indexes
 * without blocking normal bucket operations. It performs incremental index building by:
 * <ul>
 *   <li>Establishing scan boundaries from the primary index</li>
 *   <li>Processing documents in batches to manage memory and transaction sizes</li>
 *   <li>Tracking progress using cursor-based pagination</li>
 *   <li>Handling failures and retries with proper state management</li>
 * </ul>
 *
 * <p>The routine ensures transaction isolation by:
 * <ul>
 *   <li>Using a 6-second sleep to allow existing transactions to expire</li>
 *   <li>Operating with stable read versions from FoundationDB</li>
 *   <li>Retrying on transaction conflicts (codes 1007, 1020)</li>
 * </ul>
 *
 * <p>Progress is tracked through {@link IndexBuildingTaskState} which persists:
 * <ul>
 *   <li>Current cursor position for resumable processing</li>
 *   <li>Highest versionstamp boundary to detect completion</li>
 *   <li>Task status (RUNNING, COMPLETED, FAILED, STOPPED)</li>
 *   <li>Error messages for failed tasks</li>
 * </ul>
 *
 * @see IndexMaintenanceRoutine
 * @see IndexBuildingTask
 * @see IndexBuildingTaskState
 */
public class BackgroundIndexBuildingRoutine extends AbstractIndexMaintenanceRoutine {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackgroundIndexBuildingRoutine.class);
    private final static int INDEX_SCAN_BATCH_SIZE = 100;
    private final int shardId;
    private final IndexBuildingTask task;
    private final BucketService service;

    public BackgroundIndexBuildingRoutine(
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
     * <p>This method atomically updates the task's status and handles the special case
     * of COMPLETED status by incrementing the global task counter. The operation is
     * wrapped in a retry mechanism to handle transient FoundationDB conflicts.
     *
     * @param status the new status to set for the index task
     */
    private void setIndexTaskStatus(IndexTaskStatus status) {
        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexBuildingTaskState.setStatus(tr, subspace, taskId, status);
                if (status == IndexTaskStatus.COMPLETED) {
                    IndexTaskUtil.modifyTaskCounter(context, tr, taskId, 1);
                }
                tr.commit().join();
            }
        });
    }

    /**
     * Marks the index building task as failed with an error message.
     *
     * <p>This method atomically updates the task state to FAILED and records the
     * error message from the provided throwable. Unlike normal status updates,
     * this method does not use retry logic as it's typically called from
     * exception handlers where further retries would be inappropriate.
     *
     * @param th the throwable containing the error message to record
     */
    private void markIndexBuildTaskFailed(Throwable th) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setError(tr, subspace, taskId, th.getMessage());
            IndexBuildingTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.FAILED);
            tr.commit().join();
        }
    }

    /**
     * Finds the starting cursor position by locating the first versionstamp in the primary index.
     *
     * <p>This method performs a single-item range scan on the primary index to find the
     * lowest versionstamp, which becomes the starting cursor position for the index
     * building process. Returns null if the primary index is empty.
     *
     * @param tr           the FoundationDB transaction to use for the scan
     * @param primaryIndex the primary index to scan
     * @param begin        the starting byte array for the range scan
     * @param end          the ending byte array for the range scan
     * @return the first versionstamp found, or null if no entries exist
     */
    private Versionstamp findOutCursorVersionstamp(Transaction tr, Index primaryIndex, byte[] begin, byte[] end) {
        List<KeyValue> entries = tr.getRange(begin, end, 1).asList().join();
        if (entries.isEmpty()) {
            return null;
        }
        KeyValue entry = entries.getFirst();
        Tuple parsedKey = primaryIndex.subspace().unpack(entry.getKey());
        return (Versionstamp) parsedKey.get(1);
    }

    /**
     * Finds the ending boundary by locating the highest versionstamp in the primary index.
     *
     * <p>This method performs a reverse single-item range scan on the primary index to find
     * the highest versionstamp, which defines the end boundary for the index building
     * process. Returns null if the primary index is empty.
     *
     * @param tr           the FoundationDB transaction to use for the scan
     * @param primaryIndex the primary index to scan
     * @param begin        the starting byte array for the range scan
     * @param end          the ending byte array for the range scan
     * @return the highest versionstamp found, or null if no entries exist
     */
    private Versionstamp findOutHighestVersionstamp(Transaction tr, Index primaryIndex, byte[] begin, byte[] end) {
        List<KeyValue> entries = tr.getRange(begin, end, 1, true).asList().join();
        if (entries.isEmpty()) {
            return null;
        }
        KeyValue entry = entries.getFirst();
        Tuple parsedKey = primaryIndex.subspace().unpack(entry.getKey());
        return (Versionstamp) parsedKey.get(1);
    }

    /**
     * Determines and sets the cursor and highest versionstamp boundaries for index scanning.
     *
     * <p>This method establishes the range boundaries for the background index building process by:
     * <ul>
     *   <li>Checking if boundaries are already set in the task state</li>
     *   <li>If not set, refreshing bucket metadata and locating the primary index</li>
     *   <li>Finding the lowest versionstamp (cursor start position) from the primary index</li>
     *   <li>Finding the highest versionstamp (scan end position) from the primary index</li>
     *   <li>Persisting both boundaries to the IndexBuildTaskState for progress tracking</li>
     * </ul>
     *
     * <p>The boundaries define the complete range of documents that need to be processed
     * during index building. The cursor versionstamp tracks current progress, while the
     * highest versionstamp defines when the scan is complete.
     *
     * <p>If boundaries are already set in the task state, this method returns immediately
     * without any database operations, making it safe to call multiple times.
     *
     * @throws InterruptedException if the thread is interrupted during the 6-second sleep
     *                              in refreshAndLoadBucketMetadata()
     */
    private void findOutBoundaries() throws InterruptedException {
        IndexBuildingTaskState state = context.getFoundationDB().run(tr -> IndexBuildingTaskState.load(tr, subspace, taskId));
        if (state.cursorVersionstamp() != null && state.highestVersionstamp() != null) {
            return;
        }

        // Refresh bucket metadata, it's important to fetch the latest indexes before building the secondary index
        // We want that all threads have the latest view of the bucket metadata.
        refreshBucketMetadata(task.getNamespace(), task.getBucket(), task.getIndexId());

        // Fetch the up-to-date version of BucketMetadata
        BucketMetadata metadata = context.getFoundationDB().run(
                tr -> BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket())
        );
        Index primaryIndex = metadata.indexes().getIndex(DefaultIndexDefinition.ID.selector(), IndexSelectionPolicy.ALL);
        byte[] begin = primaryIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        byte[] end = ByteArrayUtil.strinc(begin);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Versionstamp cursor = findOutCursorVersionstamp(tr, primaryIndex, begin, end);
            if (cursor != null) {
                IndexBuildingTaskState.setCursorVersionstamp(tr, subspace, taskId, cursor);
            }

            Versionstamp highest = findOutHighestVersionstamp(tr, primaryIndex, begin, end);
            if (highest != null) {
                IndexBuildingTaskState.setHighestVersionstamp(tr, subspace, taskId, highest);
            }

            tr.commit().join();
        }
    }

    private void setIndexStatusAsBuilding(BucketMetadata metadata, DirectorySubspace indexSubspace) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // This will retry in the case of conflict
            IndexDefinition latestVersion = IndexUtil.loadIndexDefinition(tr, indexSubspace);
            if (latestVersion.status() == IndexStatus.DROPPED || latestVersion.status() == IndexStatus.BUILDING) {
                return;
            }
            IndexDefinition definition = latestVersion.updateStatus(IndexStatus.BUILDING);
            IndexUtil.saveIndexDefinition(tr, metadata, definition);
            tr.commit().join();
        }
    }

    /**
     * Scans the primary index and builds the target secondary index incrementally.
     *
     * <p>This method performs the core work of background index building by:
     * <ul>
     *   <li>Loading bucket metadata and validating the target index status</li>
     *   <li>Updating index status to BUILDING if not already set</li>
     *   <li>Reading documents from the volume in batches using cursor-based pagination</li>
     *   <li>Extracting index values using the index selector and inserting index entries</li>
     *   <li>Updating the cursor position for incremental progress tracking</li>
     * </ul>
     *
     * <p>The method uses transaction retries to handle FoundationDB conflicts (codes 1007, 1020)
     * and processes documents in batches of 1000 to balance memory usage and transaction size.
     *
     * <p>The scan continues until either:
     * <ul>
     *   <li>All documents have been processed (cursor reaches the highest versionstamp)</li>
     *   <li>The task is manually stopped (status set to STOPPED)</li>
     *   <li>The index is found to be in READY or DROPPED status</li>
     * </ul>
     *
     * @throws IndexMaintenanceRoutineException if the index is not found, already ready, or dropped
     */
    private void scanPrimaryIndex() {
        BucketShard shard = service.getShard(shardId);
        while (!stopped) {
            // Fetch the metadata with an independent TX due to prevent conflicts during commit time.
            BucketMetadata metadata = context.getFoundationDB().run(tr ->
                    BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket())
            );
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
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

                if (state.cursorVersionstamp() == null || state.highestVersionstamp() == null) {
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
                if (state.cursorVersionstamp().equals(state.highestVersionstamp())) {
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
                tr.commit().join();
                if (processedEntries == 0) {
                    // everything went well and processed zero entry.
                    setIndexTaskStatus(IndexTaskStatus.COMPLETED);
                    break;
                } else {
                    metrics.incrementProcessedEntries(processedEntries);
                }
            } catch (IndexMaintenanceRoutineException exp) {
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
     * Processes a batch of bucket entries and builds corresponding index entries.
     *
     * <p>This method reads documents from the volume storage in batches, extracts index
     * values using the index selector, and inserts the corresponding index entries into
     * FoundationDB. It handles:
     * <ul>
     *   <li>Reading documents from volume using cursor-based pagination</li>
     *   <li>Extracting index values based on the index definition selector</li>
     *   <li>Type validation and conversion based on the index's BSON type</li>
     *   <li>Inserting index entries with proper metadata</li>
     *   <li>Updating the cursor position after each batch</li>
     * </ul>
     *
     * <p>Documents that don't match the index selector or have type mismatches are
     * silently skipped, allowing the index build to continue for valid documents.
     *
     * @param tr       the FoundationDB transaction to use for index operations
     * @param shard    the bucket shard containing the volume to read from
     * @param metadata the bucket metadata containing index definitions
     * @param state    the current task state with cursor position
     * @return the number of entries processed in this batch
     */
    private int indexBucketEntries(Transaction tr, BucketShard shard, BucketMetadata metadata, IndexBuildingTaskState state) {
        int total = 0;
        VersionstampedKeySelector begin = VersionstampedKeySelector.firstGreaterOrEqual(state.cursorVersionstamp());
        VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterThan(state.highestVersionstamp());
        VolumeSession session = new VolumeSession(tr, metadata.volumePrefix());

        Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.READWRITE);
        Iterable<VolumeEntry> entries = shard.volume().getRange(session, begin, end, INDEX_SCAN_BATCH_SIZE);
        Versionstamp cursor = null;
        for (VolumeEntry pair : entries) {
            total++;
            Object indexValue = null;
            cursor = pair.key();
            BsonValue bsonValue = SelectorMatcher.match(index.definition().selector(), pair.entry());
            if (bsonValue != null && !bsonValue.equals(BsonNull.VALUE)) {
                indexValue = BSONUtil.toObject(bsonValue, index.definition().bsonType());
                if (indexValue == null) {
                    // Type mismatch, continue
                    continue;
                }
            }
            IndexBuilder.insertIndexEntry(tr, index.definition(), metadata, pair.key(), indexValue, shardId, pair.metadata());
        }
        setCursor(tr, cursor);
        return total;
    }

    /**
     * Updates the cursor position in the task state.
     *
     * <p>This method persists the current cursor position to the task state,
     * allowing the index building process to resume from this point if interrupted.
     * Only updates if a non-null cursor is provided.
     *
     * @param tr     the FoundationDB transaction to use for the update
     * @param cursor the new cursor versionstamp to set, or null to skip update
     */
    private void setCursor(Transaction tr, Versionstamp cursor) {
        if (cursor != null) {
            IndexBuildingTaskState.setCursorVersionstamp(tr, subspace, taskId, cursor);
        }
    }

    /**
     * Starts the background index building routine.
     *
     * <p>This method initiates the index building process by:
     * <ol>
     *   <li>Setting the task status to RUNNING</li>
     *   <li>Finding and persisting the scan boundaries (cursor and highest versionstamp)</li>
     *   <li>Beginning the primary index scan to build the secondary index</li>
     * </ol>
     *
     * <p>The method uses retry logic for both boundary detection and index scanning to handle
     * FoundationDB conflicts. If interrupted during boundary detection (e.g., during shutdown),
     * the method throws {@link IndexMaintenanceRoutineShutdownException} without marking the
     * task as failed, allowing it to be retried later.
     *
     * <p>This method is idempotent and can be called multiple times (e.g., after a stop())
     * to restart the index building process from the last saved cursor position.
     *
     * @throws IndexMaintenanceRoutineShutdownException if interrupted during boundary detection
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
        setIndexTaskStatus(IndexTaskStatus.RUNNING);
        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(() -> {
            try {
                findOutBoundaries();
            } catch (InterruptedException e) {
                // Do not mark the task as failed. Program has stopped and this task
                // can be retried.
                Thread.currentThread().interrupt();
                throw new IndexMaintenanceRoutineShutdownException();
            }
        });
        retry.executeRunnable(this::scanPrimaryIndex);
    }
}
