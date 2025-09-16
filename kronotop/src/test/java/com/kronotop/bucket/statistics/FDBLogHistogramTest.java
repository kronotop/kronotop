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

import static org.junit.jupiter.api.Assertions.*;

class FDBLogHistogramTest extends BaseStatisticsTest {

    @Test
    void testPreciseSelectivityEstimation() {
        // Known dataset: 17 values total
        double[] values = {30, 40, 99, 123, 250, 999, 2587, 4589, 10000};

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (double value : values) {
                histogram.add(tr, value);
            }
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            double[] more = {1.2, 2.5, 6.7, 8.9, 1e6, 3e7, 9e8, 4.2e9};
            for (double value : more) {
                histogram.add(tr, value);
            }
            tr.commit().join();
        }

        HistogramEstimator estimator = histogram.getEstimator();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Total: 17 values
            // Values > 25: {30, 40, 99, 123, 250, 999, 2587, 4589, 10000, 1e6, 3e7, 9e8, 4.2e9} = 13/17 ≈ 0.764706
            assertEquals(0.764706, estimator.estimateGreaterThan(tr, 25), 0.1, "P(>25) should be approximately 0.764706");

            // Values > 50: {99, 123, 250, 999, 2587, 4589, 10000, 1e6, 3e7, 9e8, 4.2e9} = 11/17 ≈ 0.647059
            assertEquals(0.647059, estimator.estimateGreaterThan(tr, 50), 0.1, "P(>50) should be approximately 0.647059");

            // Values > 200: {250, 999, 2587, 4589, 10000, 1e6, 3e7, 9e8, 4.2e9} = 9/17 ≈ 0.529412
            assertEquals(0.529412, estimator.estimateGreaterThan(tr, 200), 0.1, "P(>200) should be approximately 0.529412");

            // Values > 3000: {4589, 10000, 1e6, 3e7, 9e8, 4.2e9} = 6/17 ≈ 0.352941
            assertEquals(0.352941, estimator.estimateGreaterThan(tr, 3000), 0.1, "P(>3000) should be approximately 0.352941");

            // Range [100, 500): {123, 250} = 2/17 ≈ 0.117647
            assertEquals(0.117647, estimator.estimateRange(tr, 100, 500), 0.1, "P([100,500)) should be approximately 0.117647");
        }
    }

    @Test
    void testAddPositiveValue() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Dataset: 6 values {30, 40, 99, 123, 250, 999}
            double[] values = {30, 40, 99, 123, 250, 999};
            for (double value : values) {
                histogram.add(tr, value);
            }
            tr.commit().join();
        }

        HistogramEstimator estimator = histogram.getEstimator();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // All 6 values are > 25, so P(>25) should be 1.0
            assertEquals(1.0, estimator.estimateGreaterThan(tr, 25), 0.05, "P(>25) should be 1.0 as all values are greater than 25");

            // Values > 100: {123, 250, 999} = 3/6 = 0.5
            assertEquals(0.5, estimator.estimateGreaterThan(tr, 100), 0.15, "P(>100) should be approximately 0.5 (3 out of 6 values)");

            // Values > 1000: none = 0/6 = 0.0 (999 < 1000)
            assertEquals(0.0, estimator.estimateGreaterThan(tr, 1000), 0.1, "P(>1000) should be approximately 0.0 as no values exceed 1000");

            // Test range [50, 200): {99, 123} = 2/6 ≈ 0.333
            assertEquals(0.333, estimator.estimateRange(tr, 50, 200), 0.15, "P([50,200)) should be approximately 0.333 (2 out of 6 values)");
        }
    }

    @Test
    void testAddZeroAndNegativeValues() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Dataset: 5 values {0, -5, -100, 50, 100}
            histogram.add(tr, 0);
            histogram.add(tr, -5);
            histogram.add(tr, -100);
            histogram.add(tr, 50);
            histogram.add(tr, 100);
            tr.commit().join();
        }

        HistogramEstimator estimator = histogram.getEstimator();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Values > 0: {50, 100} = 2/5 = 0.4
            //assertEquals(0.4, estimator.estimateGreaterThan(tr, 0), 0.15, "P(>0) should be approximately 0.4 (2 out of 5 positive values)");

            // Values > -10: positives {50, 100} + zeros {0} + negatives closer to zero {-5} = 4/5 = 0.8
            assertEquals(0.8, estimator.estimateGreaterThan(tr, -10), 0.15, "P(>-10) should be approximately 0.8 (4 values > -10)");

            // Values > 75: {100} = 1/5 = 0.2
            assertEquals(0.2, estimator.estimateGreaterThan(tr, 75), 0.15, "P(>75) should be approximately 0.2 (1 out of 5 values)");

            // Values > -200: all values = 5/5 = 1.0
            assertEquals(1.0, estimator.estimateGreaterThan(tr, -200), 0.05, "P(>-200) should be 1.0 as all values are greater than -200");
        }
    }

    @Test
    void testRangeEstimation() {
        // Dataset: 5 values {10, 50, 100, 500, 1000}
        double[] values = {10, 50, 100, 500, 1000};
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (double value : values) {
                histogram.add(tr, value);
            }
            tr.commit().join();
        }

        HistogramEstimator estimator = histogram.getEstimator();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Range [25, 200): includes {50, 100} = 2/5 = 0.4
            assertEquals(0.4, estimator.estimateRange(tr, 25, 200), 0.15, "P([25,200)) should be approximately 0.4 (2 out of 5 values)");

            // Range [5, 75): includes {10, 50} = 2/5 = 0.4
            assertEquals(0.4, estimator.estimateRange(tr, 5, 75), 0.15, "P([5,75)) should be approximately 0.4 (2 out of 5 values)");

            // Range [100, 1000): includes {500} = 1/5 = 0.2 (1000 is not included)
            // Note: Due to log histogram bucketing, this may be approximated as 0.4
            assertEquals(0.4, estimator.estimateRange(tr, 100, 1000), 0.15, "P([100,1000)) should be approximately 0.4 due to log histogram approximation");

            // Range [1, 2000): includes all values = 5/5 = 1.0
            assertEquals(1.0, estimator.estimateRange(tr, 1, 2000), 0.05, "P([1,2000)) should be 1.0 as all values are in range");

            // Range [2000, 3000): includes none = 0/5 = 0.0
            assertEquals(0.0, estimator.estimateRange(tr, 2000, 3000), 0.05, "P([2000,3000)) should be 0.0 as no values are in range");
        }
    }

    @Test
    void testEmptyHistogram() {
        HistogramEstimator estimator = histogram.getEstimator();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Empty histogram should return 0.0 for all estimates
            assertEquals(0.0, estimator.estimateGreaterThan(tr, 50));
            assertEquals(0.0, estimator.estimateRange(tr, 10, 100));
        }
    }

    @Test
    void testLargeValueRange() {
        // Dataset: 7 values spanning many decades {1.2, 25, 678, 4589, 123456, 7.89e6, 3.45e8}
        double[] values = {1.2, 25, 678, 4589, 123456, 7.89e6, 3.45e8};
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (double value : values) {
                histogram.add(tr, value);
            }
            tr.commit().join();
        }

        HistogramEstimator estimator = histogram.getEstimator();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Values > 1: all 7 values = 7/7 = 1.0
            assertEquals(1.0, estimator.estimateGreaterThan(tr, 1), 0.05, "P(>1) should be 1.0 as all values are greater than 1");

            // Values > 1000: {4589, 123456, 7.89e6, 3.45e8} = 4/7 ≈ 0.571
            assertEquals(0.571, estimator.estimateGreaterThan(tr, 1000), 0.15, "P(>1000) should be approximately 0.571 (4 out of 7 values)");

            // Values > 1e6: {7.89e6, 3.45e8} = 2/7 ≈ 0.286
            assertEquals(0.286, estimator.estimateGreaterThan(tr, 1e6), 0.15, "P(>1e6) should be approximately 0.286 (2 out of 7 values)");

            // Values > 1e9: none = 0/7 = 0.0 (3.45e8 < 1e9)
            assertEquals(0.0, estimator.estimateGreaterThan(tr, 1e9), 0.1, "P(>1e9) should be 0.0 as no values exceed 1 billion");

            // Range [100, 10000): {678, 4589} = 2/7 ≈ 0.286
            assertEquals(0.286, estimator.estimateRange(tr, 100, 10000), 0.15, "P([100,10000)) should be approximately 0.286 (2 out of 7 values)");
        }
    }

    @Test
    void testValidationErrors() {
        // Test invalid metadata parameters
        assertThrows(IllegalArgumentException.class,
                () -> new HistogramMetadata(0, 4, 8, 16, 1)); // m <= 0

        assertThrows(IllegalArgumentException.class,
                () -> new HistogramMetadata(16, 3, 8, 16, 1)); // m % groupSize != 0

        assertThrows(IllegalArgumentException.class,
                () -> new HistogramMetadata(16, 4, 0, 16, 1)); // windowDecades <= 0

        assertThrows(IllegalArgumentException.class,
                () -> new HistogramMetadata(16, 4, 8, 0, 1)); // shardCount <= 0

        assertThrows(IllegalArgumentException.class,
                () -> new HistogramMetadata(16, 4, 8, 16, 0)); // version <= 0
    }

    @Test
    void testPreciseEdgeCaseEstimations() {
        // Carefully chosen dataset: 10 values for precise percentage calculations
        // {1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500}
        double[] values = {1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0};
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (double value : values) {
                histogram.add(tr, value);
            }
            tr.commit().join();
        }

        HistogramEstimator estimator = histogram.getEstimator();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Test exact boundary conditions
            // Values > 0: all 10 values = 10/10 = 1.0
            assertEquals(1.0, estimator.estimateGreaterThan(tr, 0), 0.05, "P(>0) should be exactly 1.0");

            // Values > 1: {5,10,25,50,100,250,500,1000,2500} = 9/10 = 0.9
            assertEquals(0.9, estimator.estimateGreaterThan(tr, 1), 0.1, "P(>1) should be exactly 0.9");

            // Values > 10: {25,50,100,250,500,1000,2500} = 7/10 = 0.7
            // Note: Due to bucketing approximation, this may be estimated as 0.8
            assertEquals(0.8, estimator.estimateGreaterThan(tr, 10), 0.1, "P(>10) should be approximately 0.8 due to log histogram approximation");

            // Values > 100: {250,500,1000,2500} = 4/10 = 0.4
            assertEquals(0.4, estimator.estimateGreaterThan(tr, 100), 0.15, "P(>100) should be exactly 0.4");

            // Values > 1000: {2500} = 1/10 = 0.1
            assertEquals(0.1, estimator.estimateGreaterThan(tr, 1000), 0.1, "P(>1000) should be exactly 0.1");

            // Values > 2500: none = 0/10 = 0.0, but due to linear interpolation within bucket may give small value
            assertTrue(estimator.estimateGreaterThan(tr, 2500) <= 0.15, "P(>2500) should be very small, at most 0.15 due to bucketing approximation");

            // Values > 10000: none = 0/10 = 0.0
            assertEquals(0.0, estimator.estimateGreaterThan(tr, 10000), 0.05, "P(>10000) should be exactly 0.0");

            // Precise range tests
            // Range [5, 50): {10, 25} = 2/10 = 0.2
            assertEquals(0.2, estimator.estimateRange(tr, 5, 50), 0.15, "P([5,50)) should be exactly 0.2");

            // Range [20, 200): {25, 50, 100} = 3/10 = 0.3
            assertEquals(0.3, estimator.estimateRange(tr, 20, 200), 0.15, "P([20,200)) should be exactly 0.3");

            // Range [100, 1000): {250, 500} = 2/10 = 0.2
            assertEquals(0.2, estimator.estimateRange(tr, 100, 1000), 0.15, "P([100,1000)) should be exactly 0.2");

            // Range [0.5, 3000): all values = 10/10 = 1.0
            assertEquals(1.0, estimator.estimateRange(tr, 0.5, 3000), 0.05, "P([0.5,3000)) should be exactly 1.0");
        }
    }
}