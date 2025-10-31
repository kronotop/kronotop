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
import com.kronotop.bucket.index.maintenance.IndexBuildingTaskState;
import com.kronotop.bucket.index.maintenance.IndexTaskStatus;
import io.github.resilience4j.retry.Retry;
import org.bson.BsonValue;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class IndexStatsRoutine extends AbstractIndexMaintenanceRoutine {
    private static final int FALLBACK_INSPECTION_LIMIT = 1000;
    private final IndexStatsTask task;

    public IndexStatsRoutine(Context context,
                             DirectorySubspace subspace,
                             Versionstamp taskId,
                             IndexStatsTask task) {
        super(context, subspace, taskId);
        this.task = task;
    }

    private List<Versionstamp> collectStatHints(Index index) {
        byte[] beginKey = index.subspace().pack(Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue()));
        byte[] endKey = index.subspace().pack(beginKey);

        List<Versionstamp> versionstamps = new ArrayList<>();
        KeySelector begin = KeySelector.firstGreaterThan(beginKey);
        KeySelector end = KeySelector.firstGreaterOrEqual(endKey);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            AsyncIterable<KeyValue> iterable = tr.getRange(begin, end);
            for (KeyValue keyValue : iterable) {
                Tuple unpacked = subspace.unpack(keyValue.getKey());
                versionstamps.add((Versionstamp) unpacked.get(1));
            }
        }
        return versionstamps;
    }

    private List<Object> aggregateKeysFromIndex(Index index, int limit, boolean reverse) {
        List<Object> indexedValues = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] prefix = index.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
            KeySelector begin = KeySelector.firstGreaterThan(prefix);
            KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
            for (KeyValue keyValue : tr.getRange(begin, end, limit, reverse)) {
                Tuple unpacked = index.subspace().unpack(keyValue.getKey());
                Object indexValue = unpacked.get(1);
                indexedValues.add(indexValue);
            }
        }
        return indexedValues;
    }

    private void filterBsonValuesByKind(Index index, List<Object> values, TreeSet<BsonValue> filtered) {
        for (Object value : values) {
            BsonValue bsonValue = BSONUtil.toBsonValue(value);
            if (BSONUtil.equals(bsonValue.getBsonType(), index.definition().bsonType())) {
                filtered.add(bsonValue);
            }
        }
    }

    private void constructHistogramFromEntries(BucketMetadata metadata, Index index) {
        List<Object> left = aggregateKeysFromIndex(index, FALLBACK_INSPECTION_LIMIT / 2, false);
        List<Object> right = aggregateKeysFromIndex(index, FALLBACK_INSPECTION_LIMIT / 2, true);

        TreeSet<BsonValue> filtered = new TreeSet<>(BSONUtil::compareBsonValues);
        filterBsonValuesByKind(index, left, filtered);
        filterBsonValuesByKind(index, right, filtered);

        buildAndSaveHistogram(metadata, index, filtered);
    }

    private void buildAndSaveHistogram(BucketMetadata metadata, Index index, TreeSet<BsonValue> filtered) {
        List<HistogramBucket> histogram = HistogramUtils.buildHistogram(filtered);
        byte[] encodedHistogram = HistogramCodec.encode(histogram, context.now());

        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(() -> {
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
                IndexStatsTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.COMPLETED);
                tr.commit().join();
            }
        });
    }

    private BucketMetadata openBucketMetadata() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
        }
    }

    private void markIndexStatsTaskFailed(Throwable th) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatsTaskState.setError(tr, subspace, taskId, th.getMessage());
            IndexStatsTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.FAILED);
            tr.commit().join();
        }
    }

    @Override
    public void start() {
        try {
            BucketMetadata metadata = openBucketMetadata();
            Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.READ);
            if (index == null) {
                return;
            }
            List<Versionstamp> versionstamps = collectStatHints(index);
            if (versionstamps.isEmpty()) {
                // No entry found in the STAT_HINTS subspace
                constructHistogramFromEntries(metadata, index);
            }
        } catch (Exception e) {
            markIndexStatsTaskFailed(e);
        }
    }
}
