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
import com.kronotop.bucket.*;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.index.maintenance.AbstractIndexMaintenanceRoutine;
import com.kronotop.bucket.index.maintenance.IndexMaintenanceRoutineException;
import com.kronotop.bucket.index.maintenance.IndexTaskStatus;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.namespace.NamespaceBeingRemovedException;
import com.kronotop.transaction.TransactionUtil;
import io.github.resilience4j.retry.Retry;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Analyzes index key distribution and generates histogram statistics for query optimization.
 * Supports both single-field and compound indexes.
 *
 * <p>For compound indexes, the composite key (val1, val2, ..., valN) is packed into a byte array
 * and treated as BsonBinary in the histogram. This preserves the lexicographic ordering of the
 * FDB tuple layer, so the histogram correctly reflects the key-space distribution.
 */
public class IndexAnalyzeRoutine extends AbstractIndexMaintenanceRoutine {
    protected static final Logger LOGGER = LoggerFactory.getLogger(IndexAnalyzeRoutine.class);
    private static final int HISTOGRAM_SAMPLE_SIZE = 1000;
    private static final int MAX_SAMPLE_POINTS = 100;
    private final IndexAnalyzeTask task;

    private IndexHolder<?> index;
    private IndexAnalyzeStrategy strategy;

    public IndexAnalyzeRoutine(Context context,
                               DirectorySubspace subspace,
                               Versionstamp taskId,
                               IndexAnalyzeTask task) {
        super(context, subspace, taskId);
        this.task = task;
    }

    /**
     * Collects ObjectIds from the STAT_HINTS subspace to use as sampling pivots.
     */
    private List<byte[]> collectStatHints() {
        byte[] beginKey = index.subspace().pack(Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue()));
        byte[] endKey = ByteArrayUtil.strinc(beginKey);

        List<byte[]> objectIds = new ArrayList<>();
        KeySelector begin = KeySelector.firstGreaterThan(beginKey);
        KeySelector end = KeySelector.firstGreaterOrEqual(endKey);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            AsyncIterable<KeyValue> iterable = tr.snapshot().getRange(begin, end);
            for (KeyValue keyValue : iterable) {
                checkForShutdown();
                Tuple unpacked = index.subspace().unpack(keyValue.getKey());
                objectIds.add(unpacked.getBytes(1));
            }
        }
        return objectIds;
    }

    /**
     * Aggregates index keys from the ENTRIES subspace.
     */
    private List<Object> aggregateKeysFromIndex(IndexHolder<?> index, int limit, boolean reverse) {
        List<Object> indexedValues = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] prefix = index.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            KeySelector begin = KeySelector.firstGreaterThan(prefix);
            KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
            for (KeyValue keyValue : tr.snapshot().getRange(begin, end, limit, reverse)) {
                checkForShutdown();
                Tuple unpacked = index.subspace().unpack(keyValue.getKey());
                indexedValues.add(strategy.extractKey(unpacked, 1));
            }
        }
        return indexedValues;
    }

    /**
     * Aggregates index keys around a specific ObjectId pivot using the BACK_POINTER subspace.
     */
    private List<Object> aggregateKeysAroundPivot(IndexHolder<?> index, byte[] objectIdPivot, int limit, boolean reverse) {
        byte[] beginPrefix = index.subspace().pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue(), objectIdPivot));
        byte[] endPrefix = index.subspace().pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
        List<Object> indexedValues = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            KeySelector begin = KeySelector.firstGreaterThan(beginPrefix);
            KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(endPrefix));
            for (KeyValue keyValue : tr.snapshot().getRange(begin, end, limit, reverse)) {
                checkForShutdown();
                Tuple unpacked = index.subspace().unpack(keyValue.getKey());
                indexedValues.add(strategy.extractKey(unpacked, 2));
            }
        }
        return indexedValues;
    }

    /**
     * Aggregates keys around a pivot ObjectId for the primary index.
     * The primary index has no BACK_POINTER subspace, so the hint ObjectId is used as the ENTRIES key.
     */
    private List<Object> aggregateKeysAroundPivotForPrimaryIndex(IndexHolder<?> index, byte[] objectIdPivot, int limit, boolean reverse) {
        byte[] pivotKey = index.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), objectIdPivot));
        byte[] endPrefix = index.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        List<Object> indexedValues = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            KeySelector begin = KeySelector.firstGreaterThan(pivotKey);
            KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(endPrefix));
            for (KeyValue keyValue : tr.snapshot().getRange(begin, end, limit, reverse)) {
                checkForShutdown();
                Tuple unpacked = index.subspace().unpack(keyValue.getKey());
                indexedValues.add(unpacked.get(1));
            }
        }
        return indexedValues;
    }

    /**
     * Filters raw values and converts them to BsonValues via the active strategy.
     */
    private void filterBsonValues(IndexHolder<?> index, List<Object> values, TreeSet<BsonValue> filtered) {
        strategy.filterBsonValues(index, values, filtered);
    }

    /**
     * Builds histogram samples by collecting keys from both ends of the index.
     */
    private TreeSet<BsonValue> buildHistogramFromEntries() {
        List<Object> left = aggregateKeysFromIndex(index, HISTOGRAM_SAMPLE_SIZE / 2, false);
        List<Object> right = aggregateKeysFromIndex(index, HISTOGRAM_SAMPLE_SIZE / 2, true);

        TreeSet<BsonValue> filtered = new TreeSet<>(BSONUtil::compareBsonValues);
        filterBsonValues(index, left, filtered);
        filterBsonValues(index, right, filtered);

        return filtered;
    }

    private List<byte[]> pickRandomObjectIds(List<byte[]> objectIds, int amount) {
        if (amount >= objectIds.size()) {
            return new ArrayList<>(objectIds);
        }
        List<byte[]> shuffled = new ArrayList<>(objectIds);
        Collections.shuffle(shuffled, new Random());
        return shuffled.subList(0, amount);
    }

    /**
     * Builds histogram samples using stat hints as pivots for distributed sampling.
     */
    private TreeSet<BsonValue> buildHistogramFromHints(List<byte[]> objectIds) {
        int requiredSamplePoints = MAX_SAMPLE_POINTS - 2; // 2 = left and right edges of the index subspace.
        List<byte[]> pivots = objectIds.size() > requiredSamplePoints
                ? pickRandomObjectIds(objectIds, requiredSamplePoints)
                : objectIds;

        int samplePoints = pivots.size() + 2;
        int perPointLimit = HISTOGRAM_SAMPLE_SIZE / samplePoints;

        TreeSet<BsonValue> filtered = new TreeSet<>(BSONUtil::compareBsonValues);

        List<Object> leftEdge = aggregateKeysFromIndex(index, perPointLimit, false);
        List<Object> rightEdge = aggregateKeysFromIndex(index, perPointLimit, true);
        filterBsonValues(index, leftEdge, filtered);
        filterBsonValues(index, rightEdge, filtered);

        boolean isPrimary = strategy.isPrimary(index);
        for (byte[] pivot : pivots) {
            checkForShutdown();
            List<Object> left;
            List<Object> right;
            if (isPrimary) {
                left = aggregateKeysAroundPivotForPrimaryIndex(index, pivot, perPointLimit / 2, false);
                right = aggregateKeysAroundPivotForPrimaryIndex(index, pivot, perPointLimit / 2, true);
            } else {
                left = aggregateKeysAroundPivot(index, pivot, perPointLimit / 2, false);
                right = aggregateKeysAroundPivot(index, pivot, perPointLimit / 2, true);
            }
            filterBsonValues(index, left, filtered);
            filterBsonValues(index, right, filtered);
        }

        return filtered;
    }

    /**
     * Constructs a histogram from samples and saves it to FoundationDB.
     * Validates the index is still READY before persisting.
     */
    private void buildAndSaveHistogram(BucketMetadata metadata, TreeSet<BsonValue> samples) {
        Histogram histogram = HistogramUtil.buildHistogram(samples);
        byte[] encodedHistogram = HistogramCodec.encode(histogram, context.now());

        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(() -> {
            checkForShutdown();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexDefinition definition = strategy.loadDefinition(tr, index.subspace());
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
                BucketMetadataUtil.publishIndexStatisticsUpdatedEvent(tr, context, metadata);
                commit(tr);
            }
        });
    }

    private void markIndexAnalyzeTaskFailed(Throwable th) {
        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexAnalyzeTaskState.setError(tr, subspace, taskId, th.getMessage());
                IndexAnalyzeTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.FAILED);
                commit(tr);
            }
        });
    }

    /**
     * Loads and validates the target index from bucket metadata.
     * Tries each strategy in order until one claims the index.
     */
    private IndexHolder<?> loadIndexFromMetadata(BucketMetadata metadata) {
        for (IndexAnalyzeStrategy candidate : new IndexAnalyzeStrategy[]{
                new SingleFieldAnalyzeStrategy(),
                new CompoundAnalyzeStrategy()
        }) {
            IndexHolder<?> holder = candidate.loadIndex(metadata, task.getIndexId());
            if (holder != null) {
                this.strategy = candidate;
                return holder;
            }
        }
        throw new IndexMaintenanceRoutineException("No such index");
    }

    private void initialize() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] definition = TaskStorage.getDefinition(tr, subspace, taskId);
            if (definition == null) {
                stopped = true;
                return;
            }
            IndexAnalyzeTaskState state = IndexAnalyzeTaskState.load(tr, subspace, taskId);
            if (state.status() == IndexTaskStatus.STOPPED || state.status() == IndexTaskStatus.COMPLETED) {
                stopped = true;
                // Already completed or stopped
                return;
            }
            IndexAnalyzeTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.RUNNING);
            commit(tr);
        }
    }

    private void startInternal() {
        try {
            BucketMetadata metadata = TransactionUtil.execute(context,
                    (tr) -> BucketMetadataUtil.reload(context, tr, task.getNamespace(), task.getBucket())
            );

            this.index = loadIndexFromMetadata(metadata);
            List<byte[]> objectIds = collectStatHints();

            TreeSet<BsonValue> samples;
            if (objectIds.isEmpty()) {
                // No entry found in the STAT_HINTS subspace
                samples = buildHistogramFromEntries();
            } else {
                samples = buildHistogramFromHints(objectIds);
            }
            buildAndSaveHistogram(metadata, samples);
        } catch (NoSuchBucketException | BucketBeingRemovedException |
                 NamespaceBeingRemovedException exp) {
            LOGGER.debug("Index analyze failed due to bucket/namespace removal: {}/{}/{}",
                    task.getNamespace(),
                    task.getBucket(),
                    task.getIndexId(),
                    exp
            );
            // Watchdog will detect the bucket is gone and drop the orphaned task.
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

    @Override
    public void start() {
        long start = System.currentTimeMillis();
        LOGGER.debug(
                "Index analyze started: {}/{}/{}",
                task.getNamespace(),
                task.getBucket(),
                task.getIndexId()
        );
        initialize();
        if (!stopped && !Thread.currentThread().isInterrupted()) {
            startInternal();
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
