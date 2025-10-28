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
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.maintenance.AbstractIndexMaintenanceRoutine;

public class IndexStatsRoutine extends AbstractIndexMaintenanceRoutine {
    protected IndexStatsRoutine(Context context, DirectorySubspace subspace, Versionstamp taskId) {
        super(context, subspace, taskId);
    }

    @Override
    public void start() {
        byte[] beginKey = subspace.pack(Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue()));
        byte[] endKey = subspace.pack(beginKey);

        KeySelector begin = KeySelector.firstGreaterThan(beginKey);
        KeySelector end = KeySelector.firstGreaterOrEqual(endKey);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            AsyncIterable<KeyValue> iterable = tr.getRange(begin, end);
            for (KeyValue keyValue : iterable) {
                
            }
        }

        // 1- scan STAT_HINTS subspace of subspace
        // 2- the whole subspace must be small. for 1B inserts, there might be ~10k hints
        // 3- hints are versionstamps from the backpointer subspace of the index
        // 4- as a heuristic, we'll pick 100 hints(versionstamps) and scan the left and the right sides of that points
        // 5- another heuristic: left size - 100 indexed values, right size 100 indexed values
        // 6- build a histogram from those points.

    }
}
