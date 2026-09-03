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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.*;
import com.kronotop.bucket.index.*;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.namespace.NamespaceBeingRemovedException;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.volume.VersionstampedKeySelector;
import com.kronotop.volume.VolumeEntry;
import com.kronotop.volume.VolumeSession;
import io.github.resilience4j.retry.Retry;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

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
    // Upper bound on consecutive in-place batch retries after a retriable FDB commit conflict.
    // Resets after any successful batch commit. When exceeded, the conflict is rethrown so the
    // worker-level retry and watchdog respawn act as the final backstop.
    private static final int MAX_CONSECUTIVE_COMMIT_CONFLICTS = 10;
    private static final long COMMIT_CONFLICT_BACKOFF_MILLIS = 100;
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
     * Reads the next batch of volume entries for this task, starting from the persisted cursor,
     * and extracts the ObjectId of each document.
     */
    DrainedBatch drainBatch(Transaction tr, BucketShard shard, BucketMetadata metadata, IndexBuildingTaskState state) {
        VersionstampedKeySelector begin = !state.bootstrapped() ?
                VersionstampedKeySelector.firstGreaterOrEqual(state.cursorVersionstamp()) :
                VersionstampedKeySelector.firstGreaterThan(state.cursorVersionstamp());

        VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterOrEqual(task.getUpper());
        VolumeSession session = new VolumeSession(tr, metadata.prefix());
        Iterable<VolumeEntry> entries = shard.volume().getRange(session, begin, end, INDEX_SCAN_BATCH_SIZE);

        List<BufferedEntry> buffer = new ArrayList<>();
        Versionstamp versionstamp = null;
        int total = 0;
        for (VolumeEntry pair : entries) {
            checkForShutdown();

            total++;
            versionstamp = pair.key();

            BsonValue idValue = SelectorMatcher.match(ReservedFieldName.ID.getValue(), pair.entry());
            if (idValue == null || !idValue.isObjectId()) {
                throw new IndexMaintenanceRoutineException("Document missing _id field or _id is not an ObjectId");
            }
            ObjectId objectId = idValue.asObjectId().getValue();
            buffer.add(new BufferedEntry(pair, objectId, objectId.toByteArray()));
        }

        return new DrainedBatch(buffer, versionstamp, total);
    }

    /**
     * Drains the next batch, enforces uniqueness if the index requires it, and probes FDB to find
     * which ObjectIds in the batch are already indexed by online writers. Entries already indexed
     * must be skipped by the caller so cardinality is not double-counted.
     */
    ScannedBatch scanBatch(Transaction tr, BucketShard shard, BucketMetadata metadata,
                           IndexBuildingTaskState state, IndexHolder<?> index) {
        DrainedBatch drained = drainBatch(tr, shard, metadata, state);
        List<BufferedEntry> buffer = drained.buffer();

        checkUniqueness(tr, metadata, index.definition(), buffer);

        Set<ObjectId> alreadyIndexed = IndexMaintainer.findIndexedObjectIds(
                tr, index.subspace(), buffer.stream().map(BufferedEntry::objectIdBytes).toList());

        return new ScannedBatch(buffer, alreadyIndexed, drained.lastVersionstamp(), drained.total());
    }

    void checkUniqueness(Transaction tr, BucketMetadata metadata, IndexDefinition indexDef, List<BufferedEntry> buffer) {
        if (indexDef.unique()) {
            UniquenessChecker checker = new UniquenessChecker(tr);
            UniquenessEnforcer enforcer = new UniquenessEnforcer(metadata, service.getCollatorCache(), strictTypes);
            for (BufferedEntry bufferedEntry : buffer) {
                VolumeEntry volumeEntry = bufferedEntry.entry();
                enforcer.enqueue(checker, bufferedEntry.objectIdBytes, volumeEntry.entry());
            }
            checker.await();
        }
    }

    /**
     * Returns whether the exception wraps a retriable FoundationDB transaction error, in which case
     * the failing batch can be retried in place rather than unwinding the whole build. The retriable
     * set matches {@link com.kronotop.bucket.RetryMethods}: 1007 (transaction_too_old),
     * 1020 (not_committed), 1021 (commit_unknown_result), 1031 (transaction_timed_out).
     */
    static boolean isRetriableConflict(KronotopException exp) {
        if (!(exp.getCause() instanceof FDBException fdb)) {
            return false;
        }
        int code = fdb.getCode();
        return code == 1007 || code == 1020 || code == 1021 || code == 1031;
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
        int consecutiveCommitConflicts = 0;
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
                consecutiveCommitConflicts = 0;
                if (processedEntries == 0) {
                    setIndexTaskStatus(IndexTaskStatus.COMPLETED);
                    break;
                } else {
                    metrics.incrementProcessedEntries(processedEntries);
                }
            } catch (DuplicateKeyException exp) {
                LOGGER.debug("TaskId: {} on Bucket shard: {} has failed due to duplicate key violation: '{}'",
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
                // Watchdog will detect the bucket is gone and drop the orphaned task.
                stopped = true;
                break;
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
            } catch (KronotopException exp) {
                // The findIndexedObjectIds probe registers read conflicts on purpose; a concurrent
                // online write to a probed ObjectId aborts this batch with a retriable FDB error.
                // Retry the batch in place (it resumes from the persisted cursor and the dedup probe
                // keeps it idempotent) instead of letting the whole build unwind and respawn.
                if (!isRetriableConflict(exp)) {
                    throw exp;
                }
                if (++consecutiveCommitConflicts > MAX_CONSECUTIVE_COMMIT_CONFLICTS) {
                    // Sustained conflicts: fall back to the worker-level retry and watchdog respawn.
                    throw exp;
                }
                metrics.incrementRetriedCommitConflicts();
                LOGGER.debug("Retrying index build batch for namespace={}, bucket={}, index={} on Bucket shard: {} after a retriable commit conflict (attempt {})",
                        task.getNamespace(),
                        task.getBucket(),
                        task.getIndexId(),
                        shardId,
                        consecutiveCommitConflicts
                );
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(COMMIT_CONFLICT_BACKOFF_MILLIS));
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

    record BufferedEntry(VolumeEntry entry, ObjectId objectId, byte[] objectIdBytes) {
    }

    /**
     * Result of one batch drain: the buffered entries, the versionstamp of the last scanned entry,
     * and the number of entries scanned.
     */
    record DrainedBatch(List<BufferedEntry> buffer, Versionstamp lastVersionstamp, int total) {
    }

    /**
     * Result of one batch scan: the buffered entries, the ObjectIds already indexed by online writers,
     * the versionstamp of the last scanned entry, and the number of entries scanned.
     */
    record ScannedBatch(List<BufferedEntry> buffer, Set<ObjectId> alreadyIndexed,
                        Versionstamp lastVersionstamp, int total) {
    }
}
