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
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.maintenance.AbstractIndexMaintenanceRoutine;

import java.util.ArrayList;
import java.util.List;

public class IndexStatsRoutine extends AbstractIndexMaintenanceRoutine {
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
        if (versionstamps.isEmpty()) {
            // No entry found in STAT_HINTS subspace
            // fallback
        }
    }
}
