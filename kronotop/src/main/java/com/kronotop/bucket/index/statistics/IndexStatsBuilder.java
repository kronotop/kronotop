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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import org.bson.BsonValue;

import java.util.Arrays;

public class IndexStatsBuilder {
    private static final byte[] NULL_BYTES = new byte[]{};

    public static void setHintForStats(Transaction tr, int userVersion, Index index, BsonValue value) {
        if (!ProbabilisticSelector.match(value)) {
            return;
        }
        Tuple tuple = Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue(), Versionstamp.incomplete(userVersion));
        byte[] key = index.subspace().packWithVersionstamp(tuple);
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, key, NULL_BYTES);
    }

    public static void insertHintForStats(Transaction tr, Versionstamp versionstamp, Index index, BsonValue value) {
        if (!ProbabilisticSelector.match(value)) {
            return;
        }
        insertHintForStats(tr, versionstamp, index);
    }

    public static void insertHintForStats(Transaction tr, Versionstamp versionstamp, Index index) {
        Tuple tuple = Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue(), versionstamp);
        byte[] key = index.subspace().pack(tuple);
        tr.set(key, NULL_BYTES);
    }
}
