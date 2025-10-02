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

package com.kronotop.bucket.index;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.*;
import com.kronotop.volume.VersionstampedKeySelector;
import com.kronotop.volume.VolumeEntry;
import com.kronotop.volume.VolumeSession;
import io.github.resilience4j.retry.Retry;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BackgroundIndexBuilderRoutine implements IndexMaintenanceRoutine {
    protected static final Logger LOGGER = LoggerFactory.getLogger(BackgroundIndexBuilderRoutine.class);
    private final static int INDEX_SCAN_BATCH_SIZE = 100;
    private final Context context;
    private final DirectorySubspace subspace;
    private final int shardId;
    private final Versionstamp taskId;
    private final IndexBuilderTask task;
    private final BucketService service;
    private final IndexMaintenanceRoutineMetrics metrics;
    private volatile boolean stopped;

    public BackgroundIndexBuilderRoutine(
            Context context,
            DirectorySubspace subspace,
            int shardId,
            Versionstamp taskId,
            IndexBuilderTask task
    ) {
        this.context = context;
        this.subspace = subspace;
        this.taskId = taskId;
        this.shardId = shardId;
        this.task = task;
        this.metrics = new IndexMaintenanceRoutineMetrics();
        this.service = context.getService(BucketService.NAME);
    }

    /**
     * Refreshes bucket metadata and validates the target index while ensuring transaction isolation.
     *
     * <p>This method performs a critical initialization step for background index building by:
     * <ul>
     *   <li>Creating a new FoundationDB transaction with a stable read version</li>
     *   <li>Loading bucket metadata and refreshing internal caches</li>
     *   <li>Validating that the target index exists in the metadata</li>
     *   <li>Introducing a 6-second sleep to ensure all previous transactions expire</li>
     * </ul>
     *
     * <p>The 6-second sleep is a critical safety mechanism that ensures transaction isolation.
     * Since FoundationDB transactions cannot live beyond 5 seconds, sleeping for 6 seconds
     * guarantees that any previously opened transactions have either committed or expired.
     * This prevents potential conflicts during the index building process.
     *
     * @throws InterruptedException             if the thread is interrupted during the 6-second sleep
     * @throws IndexMaintenanceRoutineException if the target index is not found in the metadata
     */
    private void refreshBucketMetadata() throws InterruptedException {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Open the BucketMetadata and refresh the caches
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
            Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
            if (index == null) {
                throw new IndexMaintenanceRoutineException("index with id '" + task.getIndexId() + "' could not be found", true);
            }

            /*
             * A potential stop-the-world pause (e.g., JVM GC) during the sleep interval
             * does not break the logic here. Once the transaction is created, it already
             * holds a stable read version from FoundationDB. If the pause extends beyond
             * the transaction lifetime, this transaction will simply fail with "too old"
             * and the task will be marked as failed. In that case, a manual or KCP trigger
             * is required to retry. This design ensures correctness is preserved even under
             * GC pauses; the worst case is a delayed or failed task, never inconsistent state.
             */
            String txLimitConfigPath = "__test__.background_index_builder.skip_wait_transaction_limit";
            boolean skipWaitTxLimit = context.getConfig().hasPath(txLimitConfigPath) && context.getConfig().getBoolean(txLimitConfigPath);
            if (!skipWaitTxLimit) {
                // FoundationDB transactions cannot live beyond 5s.
                // Sleeping 6s ensures that any previously opened transactions are expired.
                Thread.sleep(6000);
            }
            // Now all transactions either committed or died.
        }
    }

    private void markIndexBuildTaskFailed(Throwable th) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuilderTaskState.setError(tr, subspace, taskId, th.getMessage());
            IndexBuilderTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.FAILED);
            tr.commit().join();
        }
    }

    private Versionstamp findOutCursorVersionstamp(Transaction tr, Index primaryIndex, byte[] begin, byte[] end) {
        List<KeyValue> entries = tr.getRange(begin, end, 1).asList().join();

        KeyValue entry = entries.getFirst();
        Tuple parsedKey = primaryIndex.subspace().unpack(entry.getKey());
        return (Versionstamp) parsedKey.get(1);
    }

    private Versionstamp findOutHighestVersionstamp(Transaction tr, Index primaryIndex, byte[] begin, byte[] end) {
        List<KeyValue> entries = tr.getRange(begin, end, 1, true).asList().join();

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
        IndexBuilderTaskState state = context.getFoundationDB().run(tr -> IndexBuilderTaskState.load(tr, subspace, taskId));
        if (state.cursorVersionstamp() != null && state.highestVersionstamp() != null) {
            return;
        }

        // Refresh bucket metadata, it's important to fetch the latest indexes before building the secondary index
        // We want that all threads have the latest view of the bucket metadata.
        refreshBucketMetadata();

        // Fetch the up-to-date version of BucketMetadata
        BucketMetadata metadata = context.getFoundationDB().run(
                tr -> BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket())
        );
        Index primaryIndex = metadata.indexes().getIndex(DefaultIndexDefinition.ID.selector(), IndexSelectionPolicy.ALL);
        byte[] begin = primaryIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        byte[] end = ByteArrayUtil.strinc(begin);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Versionstamp cursor = findOutCursorVersionstamp(tr, primaryIndex, begin, end);
            IndexBuilderTaskState.setCursorVersionstamp(tr, subspace, taskId, cursor);

            Versionstamp highest = findOutHighestVersionstamp(tr, primaryIndex, begin, end);
            IndexBuilderTaskState.setHighestVersionstamp(tr, subspace, taskId, highest);

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
            long processedEntries = 0;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
                Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.READWRITE);
                if (index == null) {
                    throw new IndexMaintenanceRoutineException("no index found with id " + task.getIndexId(), true);
                }

                if (index.definition().status() == IndexStatus.READY) {
                    throw new IndexMaintenanceRoutineException(String.format(
                            "index with selector=%s, id=%d is ready to query",
                            index.definition().selector(),
                            index.definition().id()
                    ), true);
                } else if (index.definition().status() == IndexStatus.DROPPED) {
                    throw new IndexMaintenanceRoutineException(String.format(
                            "index with selector=%s, id=%d is dropped",
                            index.definition().selector(),
                            index.definition().id()
                    ), true);
                }

                // Three possibilities for IndexStatus: WAITING, BUILDING, FAILED
                if (index.definition().status() != IndexStatus.BUILDING) {
                    IndexDefinition definition = index.definition().updateStatus(IndexStatus.BUILDING);
                    context.getFoundationDB().run(tx -> {
                        // This will retry in the case of conflict
                        IndexUtil.saveIndexDefinition(tx, definition, index.subspace());
                        return null;
                    });
                }

                IndexBuilderTaskState state = IndexBuilderTaskState.load(tr, subspace, taskId);
                if (state.status() == IndexTaskStatus.STOPPED) {
                    // The operator marked the task as STOPPED manually.
                    LOGGER.debug(
                            "Background index builder for namespace={}, bucket={}, index={} has been stopped by the operator",
                            task.getNamespace(),
                            task.getBucket(),
                            task.getIndexId()
                    );
                    break;
                }
                if (state.cursorVersionstamp().equals(state.highestVersionstamp())) {
                    LOGGER.debug(
                            "Background index builder for namespace={}, bucket={}, index={} has been completed",
                            task.getNamespace(),
                            task.getBucket(),
                            task.getIndexId()
                    );
                    // All entries are processed. End of the task.
                    IndexBuilderTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.COMPLETED);
                    break;
                }

                VersionstampedKeySelector begin = VersionstampedKeySelector.firstGreaterOrEqual(state.cursorVersionstamp());
                VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterThan(state.highestVersionstamp());
                VolumeSession session = new VolumeSession(tr, metadata.volumePrefix());

                Iterable<VolumeEntry> entries = shard.volume().getRange(session, begin, end, INDEX_SCAN_BATCH_SIZE);
                Versionstamp cursor = null;
                for (VolumeEntry pair : entries) {
                    processedEntries++;
                    Object indexValue = null;
                    BsonValue bsonValue = SelectorMatcher.match(index.definition().selector(), pair.entry());
                    if (bsonValue != null && !bsonValue.equals(BsonNull.VALUE)) {
                        indexValue = BSONUtil.toObject(bsonValue, index.definition().bsonType());
                        if (indexValue == null) {
                            // Type mismatch, continue
                            continue;
                        }
                    }
                    IndexBuilder.insertIndexEntry(tr, index.definition(), metadata, pair.key(), indexValue, pair.metadata());
                    cursor = pair.key();
                }
                if (cursor != null) {
                    IndexBuilderTaskState.setCursorVersionstamp(tr, subspace, taskId, cursor);
                }
                tr.commit().join();
            } finally {
                metrics.incrementProcessedEntries(processedEntries);
                metrics.setLatestExecution(System.currentTimeMillis());
            }
        }
    }

    public void start() {
        LOGGER.debug(
                "Starting to build namespace={}, bucket={}, index={} at the background",
                task.getNamespace(),
                task.getBucket(),
                task.getIndexId()
        );
        stopped = false; // also means a restart
        try {
            findOutBoundaries();
            Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
            retry.executeRunnable(this::scanPrimaryIndex);
        } catch (InterruptedException e) {
            // Do not mark the task as failed. Program has stopped and this task
            // can be retried.
            throw new RuntimeException(e);
        } catch (IndexMaintenanceRoutineException exp) {
            if (exp.isFailed()) {
                markIndexBuildTaskFailed(exp);
                return;
            }
            throw exp;
        }
    }

    public void stop() {
        stopped = true;
    }

    @Override
    public IndexMaintenanceRoutineMetrics getMetrics() {
        return metrics;
    }
}
