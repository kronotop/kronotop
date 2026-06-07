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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.bucket.index.IndexHolder;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import org.bson.BsonValue;

/**
 * Stores statistical hints for index analysis. Hints are ObjectIds sampled via probabilistic
 * selection during index operations. {@link IndexAnalyzeRoutine} uses these hints as pivots
 * when building histograms for query optimization.
 *
 * <p>Key structure: {@code (STAT_HINTS, ObjectId) → null}
 */
public class IndexStatsBuilder {
    private static final byte[] NULL_BYTES = new byte[]{};

    /**
     * Stores a hint if the value passes the probabilistic selector.
     *
     * @param tr            the transaction
     * @param index         the target index
     * @param objectIdBytes the document's ObjectId bytes to store as a hint
     * @param value         the indexed value used for probabilistic selection
     */
    public static void setHintForStats(Transaction tr, IndexHolder<?> index, byte[] objectIdBytes, BsonValue value) {
        if (!ProbabilisticSelector.match(value)) {
            return;
        }
        setHintForStats(tr, index, objectIdBytes);
    }

    /**
     * Stores a hint unconditionally, bypassing probabilistic selection.
     *
     * @param tr            the transaction
     * @param index         the target index
     * @param objectIdBytes the document's ObjectId bytes to store as a hint
     */
    public static void setHintForStats(Transaction tr, IndexHolder<?> index, byte[] objectIdBytes) {
        Tuple tuple = Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue(), objectIdBytes);
        byte[] key = index.subspace().pack(tuple);
        tr.set(key, NULL_BYTES);
    }

    /**
     * Stores a hint if the ObjectId bytes pass the probabilistic selector.
     * Designed for compound indexes where selection is based on ObjectId rather than field values.
     *
     * @param tr            the transaction
     * @param index         the target index
     * @param objectIdBytes the document's ObjectId bytes, used for both selection and storage
     */
    public static void setHintForStatsIfSelected(Transaction tr, IndexHolder<?> index, byte[] objectIdBytes) {
        if (!ProbabilisticSelector.match(objectIdBytes)) {
            return;
        }
        setHintForStats(tr, index, objectIdBytes);
    }
}
