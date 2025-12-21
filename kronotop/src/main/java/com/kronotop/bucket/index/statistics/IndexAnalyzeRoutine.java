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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.RetryMethods;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.index.maintenance.AbstractIndexMaintenanceRoutine;
import com.kronotop.bucket.index.maintenance.IndexMaintenanceRoutineException;
import com.kronotop.bucket.index.maintenance.IndexTaskStatus;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.internal.task.TaskStorage;
import io.github.resilience4j.retry.Retry;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Analyzes index key distribution and generates histogram statistics for query optimization.
 *
 * <p>This routine collects samples from an index's key space and builds a histogram that represents
 * the distribution of values. The histogram is used by the query optimizer to estimate selectivity
 * and choose efficient query execution plans.
 *
 * <p><strong>Sampling Strategy:</strong>
 * <ul>
 *   <li>If stat hints exist: Uses hint-based pivots to sample across the key space with additional
 *       edge sampling for comprehensive coverage (up to 100 sample points)</li>
 *   <li>If no hints exist: Samples from both ends of the index (500 keys from start, 500 from end)</li>
 * </ul>
 *
 * <p><strong>Process Flow:</strong>
 * <ol>
 *   <li>Load bucket metadata and validate index status is READY</li>
 *   <li>Check for stat hints in the index's STAT_HINTS subspace</li>
 *   <li>Collect samples based on available hints</li>
 *   <li>Filter samples to match the index's declared BSON type</li>
 *   <li>Build and encode histogram using {@link HistogramUtils}</li>
 *   <li>Save histogram to FoundationDB and mark task as COMPLETED</li>
 * </ol>
 *
 * <p><strong>Failure Handling:</strong>
 * If the index is dropped during analysis or any error occurs, the task is marked as FAILED
 * with an error message stored in the task state.
 */
public class IndexAnalyzeRoutine extends AbstractIndexMaintenanceRoutine {
    protected static final Logger LOGGER = LoggerFactory.getLogger(IndexAnalyzeRoutine.class);
    private static final int HISTOGRAM_SAMPLE_SIZE = 1000;
    private static final int MAX_SAMPLE_POINTS = 100;
    private final IndexAnalyzeTask task;

    /**
     * Creates a new index analyze routine.
     *
     * @param context  the system context providing access to services
     * @param subspace the directory subspace for task state storage
     * @param taskId   the unique identifier for this analysis task
     * @param task     the task configuration containing namespace, bucket, and index information
     */
    public IndexAnalyzeRoutine(Context context,
                               DirectorySubspace subspace,
                               Versionstamp taskId,
                               IndexAnalyzeTask task) {
        super(context, subspace, taskId);
        this.task = task;
    }

    /**
     * Collects versionstamps from the STAT_HINTS subspace to use as sampling pivots.
     *
     * @param index the index to collect hints from
     * @return list of versionstamps representing hint positions in the index
     */
    private List<Versionstamp> collectStatHints(Index index) {
        byte[] beginKey = index.subspace().pack(Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue()));
        byte[] endKey = ByteArrayUtil.strinc(beginKey);

        List<Versionstamp> versionstamps = new ArrayList<>();
        KeySelector begin = KeySelector.firstGreaterThan(beginKey);
        KeySelector end = KeySelector.firstGreaterOrEqual(endKey);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            AsyncIterable<KeyValue> iterable = tr.snapshot().getRange(begin, end);
            for (KeyValue keyValue : iterable) {
                checkForShutdown();
                Tuple unpacked = index.subspace().unpack(keyValue.getKey());
                versionstamps.add((Versionstamp) unpacked.get(1));
            }
        }
        return versionstamps;
    }

    /**
     * Aggregates index keys from the ENTRIES subspace.
     *
     * @param index   the index to sample from
     * @param limit   maximum number of keys to collect
     * @param reverse whether to scan in reverse order
     * @return list of indexed values
     */
    private List<Object> aggregateKeysFromIndex(Index index, int limit, boolean reverse) {
        List<Object> indexedValues = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] prefix = index.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            KeySelector begin = KeySelector.firstGreaterThan(prefix);
            KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
            for (KeyValue keyValue : tr.snapshot().getRange(begin, end, limit, reverse)) {
                checkForShutdown();
                Tuple unpacked = index.subspace().unpack(keyValue.getKey());
                Object indexValue = unpacked.get(1);
                indexedValues.add(indexValue);
            }
        }
        return indexedValues;
    }

    /**
     * Aggregates index keys around a specific versionstamp pivot using the BACK_POINTER subspace.
     *
     * @param index   the index to sample from
     * @param pivot   the versionstamp to sample around
     * @param limit   maximum number of keys to collect
     * @param reverse whether to scan in reverse order
     * @return list of indexed values near the pivot point
     */
    private List<Object> aggregateKeysAroundPivot(Index index, Versionstamp pivot, int limit, boolean reverse) {
        // Operates on the back-pointer subspace
        byte[] beginPrefix = index.subspace().pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue(), pivot));
        byte[] endPrefix = index.subspace().pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
        List<Object> indexedValues = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            KeySelector begin = KeySelector.firstGreaterThan(beginPrefix);
            KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(endPrefix));
            for (KeyValue keyValue : tr.snapshot().getRange(begin, end, limit, reverse)) {
                checkForShutdown();
                Tuple unpacked = index.subspace().unpack(keyValue.getKey());
                Object indexValue = unpacked.get(2);
                indexedValues.add(indexValue);
            }
        }
        return indexedValues;
    }

    /**
     * Filters raw values to include only those matching the index's declared BSON type.
     *
     * @param index    the index definition containing the expected BSON type
     * @param values   raw values to filter
     * @param filtered the set to add matching BSON values to
     */
    private void filterBsonValuesByKind(Index index, List<Object> values, TreeSet<BsonValue> filtered) {
        for (Object value : values) {
            checkForShutdown();
            BsonValue bsonValue = BSONUtil.toBsonValue(value);
            if (BSONUtil.equals(bsonValue.getBsonType(), index.definition().bsonType())) {
                filtered.add(bsonValue);
            }
        }
    }

    /**
     * Builds histogram samples by collecting keys from both ends of the index.
     * Collects 500 keys from the start and 500 keys from the end.
     *
     * @param index the index to sample from
     * @return sorted set of BSON values for histogram construction
     */
    private TreeSet<BsonValue> buildHistogramFromEntries(Index index) {
        List<Object> left = aggregateKeysFromIndex(index, HISTOGRAM_SAMPLE_SIZE / 2, false);
        List<Object> right = aggregateKeysFromIndex(index, HISTOGRAM_SAMPLE_SIZE / 2, true);

        TreeSet<BsonValue> filtered = new TreeSet<>(BSONUtil::compareBsonValues);
        filterBsonValuesByKind(index, left, filtered);
        filterBsonValuesByKind(index, right, filtered);

        return filtered;
    }

    /**
     * Randomly selects a subset of versionstamps for sampling.
     *
     * @param versionstamps the full list of versionstamps
     * @param amount        the number of versionstamps to select
     * @return randomly selected versionstamps, or all if amount exceeds list size
     */
    private List<Versionstamp> pickRandomVersionstamps(List<Versionstamp> versionstamps, int amount) {
        if (amount >= versionstamps.size()) {
            return new ArrayList<>(versionstamps);
        }
        List<Versionstamp> shuffled = new ArrayList<>(versionstamps);
        Collections.shuffle(shuffled, new Random());
        return shuffled.subList(0, amount);
    }

    /**
     * Builds histogram samples using stat hints as pivots for distributed sampling.
     * Samples around each pivot and from the index edges to achieve comprehensive coverage.
     *
     * @param index         the index to sample from
     * @param versionstamps hint versionstamps to use as sampling pivots
     * @return sorted set of BSON values for histogram construction
     */
    private TreeSet<BsonValue> buildHistogramFromHints(Index index, List<Versionstamp> versionstamps) {
        int requiredSamplePoints = MAX_SAMPLE_POINTS - 2; // 2 = left and right edges of the index subspace.
        List<Versionstamp> pivots = versionstamps.size() > requiredSamplePoints
                ? pickRandomVersionstamps(versionstamps, requiredSamplePoints)
                : versionstamps;

        int samplePoints = pivots.size() + 2;
        int perPointLimit = HISTOGRAM_SAMPLE_SIZE / samplePoints;

        TreeSet<BsonValue> filtered = new TreeSet<>(BSONUtil::compareBsonValues);

        List<Object> leftEdge = aggregateKeysFromIndex(index, perPointLimit, false);
        List<Object> rightEdge = aggregateKeysFromIndex(index, perPointLimit, true);
        filterBsonValuesByKind(index, leftEdge, filtered);
        filterBsonValuesByKind(index, rightEdge, filtered);

        for (Versionstamp pivot : pivots) {
            checkForShutdown();
            List<Object> left = aggregateKeysAroundPivot(index, pivot, perPointLimit / 2, false);
            List<Object> right = aggregateKeysAroundPivot(index, pivot, perPointLimit / 2, true);
            filterBsonValuesByKind(index, left, filtered);
            filterBsonValuesByKind(index, right, filtered);
        }

        return filtered;
    }

    /**
     * Constructs a histogram from samples and saves it to FoundationDB.
     * Validates the index is still READY before persisting.
     *
     * @param metadata the bucket metadata containing the index
     * @param index    the index being analyzed
     * @param samples  the collected BSON value samples
     */
    private void buildAndSaveHistogram(BucketMetadata metadata, Index index, TreeSet<BsonValue> samples) {
        Histogram histogram = HistogramUtils.buildHistogram(samples);
        byte[] encodedHistogram = HistogramCodec.encode(histogram, context.now());

        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(() -> {
            checkForShutdown();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexDefinition definition = IndexUtil.loadIndexDefinition(tr, index.subspace());
                if (definition == null) {
                    // Index is dropped and flushed
                    return;
                }
                if (definition.status() != IndexStatus.READY) {
                    // Index is probably dropped
                    return;
                }
                byte[] histogramKey = IndexUtil.histogramKey(metadata.subspace(), index.definition().id());
                tr.set(histogramKey, encodedHistogram);
                IndexAnalyzeTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.COMPLETED);
                commit(tr);
            }
        });
    }

    /**
     * Marks the analysis task as failed with an error message.
     *
     * @param th the exception that caused the failure
     */
    private void markIndexAnalyzeTaskFailed(Throwable th) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexAnalyzeTaskState.setError(tr, subspace, taskId, th.getMessage());
            IndexAnalyzeTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.FAILED);
            commit(tr);
        }
    }

    /**
     * Loads and validates the target index from bucket metadata.
     * Ensures the index exists and is in READY status.
     *
     * @param metadata the bucket metadata to load the index from
     * @return the validated index
     * @throws IndexMaintenanceRoutineException if index not found or not ready
     */
    private Index loadIndexFromMetadata(BucketMetadata metadata) {
        Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
        if (index == null) {
            throw new IndexMaintenanceRoutineException("No such index");
        }
        index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.READ);
        if (index == null) {
            throw new IndexMaintenanceRoutineException("Index is not ready to analyze");
        }
        return index;
    }

    private void initialize() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] definition = TaskStorage.getDefinition(tr, subspace, taskId);
            if (definition == null) {
                IndexAnalyzeTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.STOPPED);
            } else {
                IndexAnalyzeTaskState state = IndexAnalyzeTaskState.load(tr, subspace, taskId);
                if (state.status() == IndexTaskStatus.STOPPED || state.status() == IndexTaskStatus.COMPLETED) {
                    stopped = true;
                    // Already completed or stopped
                    return;
                }
                IndexAnalyzeTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.RUNNING);
            }
            commit(tr);
        }
    }

    private void startInternal() {
        try {
            BucketMetadata metadata = TransactionUtils.execute(context,
                    (tr) -> BucketMetadataUtil.openUncached(context, tr, task.getNamespace(), task.getBucket())
            );
            Index index = loadIndexFromMetadata(metadata);
            List<Versionstamp> versionstamps = collectStatHints(index);

            TreeSet<BsonValue> samples;
            if (versionstamps.isEmpty()) {
                // No entry found in the STAT_HINTS subspace
                samples = buildHistogramFromEntries(index);
            } else {
                samples = buildHistogramFromHints(index, versionstamps);
            }
            buildAndSaveHistogram(metadata, index, samples);
        } catch (Exception exp) {
            LOGGER.error("Index analyze failed: {}/{}/{}",
                    task.getNamespace(),
                    task.getBucket(),
                    task.getIndexId(),
                    exp
            );
            markIndexAnalyzeTaskFailed(exp);
        }
    }

    /**
     * Executes the index analysis routine.
     * Loads metadata, collects samples based on available hints, and persists the histogram.
     * On failure, marks the task as FAILED.
     */
    @Override
    public void start() {
        long start = System.currentTimeMillis();
        LOGGER.debug(
                "Index analyze started: {}/{}/{}",
                task.getNamespace(),
                task.getBucket(),
                task.getIndexId()
        );
        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(this::initialize);
        if (!stopped || !Thread.currentThread().isInterrupted()) {
            retry.executeRunnable(this::startInternal);
        }
        long end = System.currentTimeMillis();
        LOGGER.debug(
                "Index analyze completed: {}/{}/{} ({})ms",
                task.getNamespace(),
                task.getBucket(),
                task.getIndexId(),
                end - start
        );
    }
}
