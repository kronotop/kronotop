/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.bucket.index.IndexSubspaceMagic;

public class IndexUtil {

    /**
     * Constructs a {@link KeySelector} based on the specified bound and its value.
     * The method computes a key for index navigation and determines the appropriate selector
     * for the provided bound operator (GT, GTE, LT, LTE).
     *
     * @param indexSubspace the {@link DirectorySubspace} representing the index subspace
     * @param bound         a {@link Bound} object representing the cursor's boundary operator and value
     * @param boundValue    the value used to construct the boundary key for the index
     * @return a {@link KeySelector} configured for the specified boundary and operator
     * @throws IllegalStateException if the {@link Bound#operator()} is unsupported
     */
    static KeySelector getKeySelector(DirectorySubspace indexSubspace, Bound bound, Object boundValue) {
        Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), boundValue);
        byte[] boundKey = indexSubspace.pack(tuple);

        return getKeySelector(bound, boundKey);
    }

    private static KeySelector getKeySelector(Bound bound, byte[] boundKey) {
        return switch (bound.operator()) {
            case GT, LTE -> KeySelector.firstGreaterThan(boundKey);
            case GTE, LT -> KeySelector.firstGreaterOrEqual(boundKey);
            default -> throw new IllegalStateException("Unsupported cursor operator: " + bound.operator());
        };
    }

}