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
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.maintenance.AbstractIndexMaintenanceRoutine;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.internal.BsonUtil;

import java.util.ArrayList;
import java.util.Arrays;
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

    private void fallback(Index index) {
        List<Object> left = aggregateKeysFromIndex(index, FALLBACK_INSPECTION_LIMIT / 2, false);
        List<Object> right = aggregateKeysFromIndex(index, FALLBACK_INSPECTION_LIMIT / 2, true);

        TreeSet<BsonValue> filtered = new TreeSet<>(BSONUtil::compareBsonValues);
        filterBsonValuesByKind(index, left, filtered);
        filterBsonValuesByKind(index, right, filtered);

        System.out.println(HistogramUtils.buildHistogram(filtered));
    }

    @Override
    public void start() {
        Index index = context.getFoundationDB().run(tr -> {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
            return metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.READ);
        });
        if (index == null) {
            return;
        }

        List<Versionstamp> versionstamps = collectStatHints(index);
        System.out.println(versionstamps);
        if (versionstamps.isEmpty()) {
            // No entry found in the STAT_HINTS subspace
            fallback(index);
        }
    }
}
