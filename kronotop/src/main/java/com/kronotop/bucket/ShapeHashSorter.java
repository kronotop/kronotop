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

package com.kronotop.bucket;

/**
 * Sorts indices by shape hash. ParameterExtractor and PhysicalPlanParameterBinder use it to
 * order AND/OR children in the same way.
 */
public final class ShapeHashSorter {

    private ShapeHashSorter() {
    }

    /**
     * Sorts hashes in ascending signed order and moves indices with them. Uses insertion sort,
     * which is stable: indices with equal hashes keep their input order.
     *
     * @param hashes  the hash values to sort by (modified in place)
     * @param indices the indices to reorder according to hash order (modified in place)
     */
    public static void sortIndicesByHash(long[] hashes, int[] indices) {
        int size = hashes.length;
        for (int i = 1; i < size; i++) {
            long keyHash = hashes[i];
            int keyIndex = indices[i];
            int j = i - 1;
            while (j >= 0 && hashes[j] > keyHash) {
                hashes[j + 1] = hashes[j];
                indices[j + 1] = indices[j];
                j--;
            }
            hashes[j + 1] = keyHash;
            indices[j + 1] = keyIndex;
        }
    }
}
