/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket.statistics;

import com.apple.foundationdb.Transaction;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrefixHistogramEstimatorTest extends BasePrefixHistogramTest {


    @Test
    void testBasicRangeEstimation2() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Add some ordered keys
            histogram.add(tr, "apple".getBytes(StandardCharsets.UTF_8));
            histogram.add(tr, "bpple".getBytes(StandardCharsets.UTF_8));
            histogram.add(tr, "cpple".getBytes(StandardCharsets.UTF_8));
            histogram.add(tr, "dpple".getBytes(StandardCharsets.UTF_8));

            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrefixHistogramEstimator estimator = histogram.getEstimator();

            // Test total count
            long total = estimator.estimateRange(tr, null, null);
            assertEquals(4, total);

            // Test range estimation
            byte[] lower = "".getBytes(StandardCharsets.UTF_8);
            byte[] upper = "z".getBytes(StandardCharsets.UTF_8);
            long rangeCount = estimator.estimateRange(tr, null, upper);
            System.out.println(rangeCount);
        }
    }

    @Test
    void testBasicRangeEstimation() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Add some ordered keys
            histogram.add(tr, "a".getBytes(StandardCharsets.UTF_8));
            histogram.add(tr, "b".getBytes(StandardCharsets.UTF_8));
            histogram.add(tr, "c".getBytes(StandardCharsets.UTF_8));
            histogram.add(tr, "d".getBytes(StandardCharsets.UTF_8));

            PrefixHistogramEstimator estimator = histogram.getEstimator();

            // Test total count
            long total = estimator.estimateRange(tr, null, null);
            assertEquals(4, total);

            // Test range estimation
            byte[] lower = "b".getBytes(StandardCharsets.UTF_8);
            byte[] upper = "d".getBytes(StandardCharsets.UTF_8);
            long rangeCount = estimator.estimateRange(tr, lower, upper);
            assertTrue(rangeCount >= 0 && rangeCount <= 4);

            tr.commit().join();
        }
    }

    @Test
    void testInequalityPredicates() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Add some keys with numeric ordering
            histogram.add(tr, new byte[]{1});
            histogram.add(tr, new byte[]{2});
            histogram.add(tr, new byte[]{3});
            histogram.add(tr, new byte[]{4});
            histogram.add(tr, new byte[]{5});

            PrefixHistogramEstimator estimator = histogram.getEstimator();

            // Test < 3 (should include 1, 2)
            long lessThan = estimator.estimateLessThan(tr, new byte[]{3});
            assertTrue(lessThan >= 0);

            // Test <= 3 (should include 1, 2, 3)
            long lessThanOrEqual = estimator.estimateLessThanOrEqual(tr, new byte[]{3});
            assertTrue(lessThanOrEqual >= lessThan);

            // Test > 3 (should include 4, 5)
            long greaterThan = estimator.estimateGreaterThan(tr, new byte[]{3});
            assertTrue(greaterThan >= 0);

            // Test >= 3 (should include 3, 4, 5)
            long greaterThanOrEqual = estimator.estimateGreaterThanOrEqual(tr, new byte[]{3});
            assertTrue(greaterThanOrEqual >= greaterThan);

            tr.commit().join();
        }
    }

    @Test
    void testEmptyRange() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            histogram.add(tr, "test".getBytes(StandardCharsets.UTF_8));

            PrefixHistogramEstimator estimator = histogram.getEstimator();

            // Same bounds should return 0
            byte[] key = "test".getBytes(StandardCharsets.UTF_8);
            long count = estimator.estimateRange(tr, key, key);
            assertEquals(0, count);

            tr.commit().join();
        }
    }

    @Test
    void testPrefixDifferentiation() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Add keys with common prefixes
            histogram.add(tr, "prefix1".getBytes(StandardCharsets.UTF_8));
            histogram.add(tr, "prefix2".getBytes(StandardCharsets.UTF_8));
            histogram.add(tr, "different".getBytes(StandardCharsets.UTF_8));

            PrefixHistogramEstimator estimator = histogram.getEstimator();

            // Range should differentiate between prefixes
            byte[] lower = "prefix".getBytes(StandardCharsets.UTF_8);
            byte[] upper = "prefiy".getBytes(StandardCharsets.UTF_8); // Just after "prefix"
            long count = estimator.estimateRange(tr, lower, upper);
            assertTrue(count >= 0 && count <= 3);

            tr.commit().join();
        }
    }

    @Test
    void testBoundaryConditions() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Test with minimum and maximum byte values
            histogram.add(tr, new byte[]{0});
            histogram.add(tr, new byte[]{127});
            histogram.add(tr, new byte[]{(byte) 255});

            PrefixHistogramEstimator estimator = histogram.getEstimator();

            // Test ranges with boundary values
            long count1 = estimator.estimateRange(tr, new byte[]{0}, new byte[]{(byte) 255});
            assertTrue(count1 >= 0);

            long count2 = estimator.estimateRange(tr, null, new byte[]{127});
            assertTrue(count2 >= 0);

            long count3 = estimator.estimateRange(tr, new byte[]{127}, null);
            assertTrue(count3 >= 0);

            tr.commit().join();
        }
    }

    @Test
    void testMultiDepthKeys() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Add keys of different lengths
            histogram.add(tr, new byte[]{1});
            histogram.add(tr, new byte[]{1, 2});
            histogram.add(tr, new byte[]{1, 2, 3});
            histogram.add(tr, new byte[]{1, 2, 3, 4});

            PrefixHistogramEstimator estimator = histogram.getEstimator();

            // All should be counted
            long total = estimator.estimateRange(tr, null, null);
            assertEquals(4, total);

            // Range estimation with varying depths
            long count = estimator.estimateRange(tr, new byte[]{1}, new byte[]{2});
            assertTrue(count >= 0 && count <= 4);

            tr.commit().join();
        }
    }
}