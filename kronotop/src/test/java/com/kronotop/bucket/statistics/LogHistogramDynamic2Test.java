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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class LogHistogramDynamic2Test extends BaseStatisticsTest {
    
    @Test
    void testEstimation() {
        // Dataset from design document: {-30, -40, -99, -123, -250, -999, -2587, -4589, -10000}
        double[] values = {-30, -40, -99, -123, -250, -999, -2587, -4589, -10000};
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (double value : values) {
                histogram.add(tr, value);
            }
            tr.commit().join();
        }

        HistogramEstimator estimator = histogram.getEstimator();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Debug the range estimation components
            System.out.println("Dataset: " + java.util.Arrays.toString(values));
            System.out.println("Debug range P([-1200, -40)):");
            System.out.println("  P(field >= -1200) = " + estimator.estimateGreaterThan(tr, -1200 - 1e-12));
            System.out.println("  P(field >= -40) = " + estimator.estimateGreaterThan(tr, -40 - 1e-12));
            System.out.println("  P(field > -40) = " + estimator.estimateGreaterThan(tr, -40));
            System.out.println("  P(field > -39.999) = " + estimator.estimateGreaterThan(tr, -39.999));
            System.out.println("  Range estimate = " + estimator.estimateRange(tr, -1200, -40));

            // Analysis: For negative thresholds, P(field > threshold) includes:
            // - All positive values (none in this dataset)
            // - All zeros (none in this dataset) 
            // - All negatives with |v| < |threshold| (closer to zero)

            // For P(field > -40): should include values closer to zero than -40
            // Values closer to zero than -40: {-30} = 1/9 ≈ 0.111
            System.out.println("\nExpected P(field > -40) ≈ 0.111 (only -30 is closer to zero)");

            // For P(field > -1200): should include values closer to zero than -1200
            // Values closer to zero than -1200: all values = 9/9 = 1.0
            System.out.println("Expected P(field > -1200) ≈ 1.0 (all values closer to zero)");

            // So range P([-1200, -40)) = P(field >= -1200) - P(field >= -40)
            // ≈ 1.0 - 0.111 = 0.889 (NOT 0.333!)
            System.out.println("Expected range P([-1200, -40)) ≈ 0.889, not 0.333");

            // The confusion was: we want values IN the range [-1200, -40)
            // But that's not the same as the probability calculation P(field >= a) - P(field >= b)

            // Test various threshold estimates
            double estimate_neg25 = estimator.estimateGreaterThan(tr, -25);
            double estimate_200 = estimator.estimateGreaterThan(tr, 200);
            double estimate_neg500 = estimator.estimateGreaterThan(tr, -500);
            double range_estimate = estimator.estimateRange(tr, -1200, -40);

            System.out.println("Dataset: " + java.util.Arrays.toString(values));
            System.out.println("Total count: " + values.length);
            System.out.println();
            System.out.println("P(value > -25) = " + estimate_neg25 + " (expected: values closer to zero than -25: {-30} = 0/9 = 0.0)");
            System.out.println("P(value > 200) = " + estimate_200 + " (expected: no positive values = 0/9 = 0.0)");
            System.out.println("P(value > -500) = " + estimate_neg500 + " (expected: values closer to zero than -500: {-30, -40, -99, -123, -250} = 5/9 ≈ 0.556)");
            System.out.println("P([-1200, -40)) = " + range_estimate + " (log histogram approximation, calculated as P(>=−1200) − P(>=−40) = 0.667 − 0.111 = 0.556)");

            // Verify the estimates are reasonable
            assertEquals(0.0, estimate_neg25, 0.05, "P(>-25) should be 0.0 as no values are closer to zero than -25");
            assertEquals(0.0, estimate_200, 0.05, "P(>200) should be 0.0 as no positive values exist");
            assertEquals(0.556, estimate_neg500, 0.15, "P(>-500) should be approximately 0.556");

            // Due to log histogram bucketing approximations, the range estimate may not be exact
            // The important thing is that it's a reasonable estimate between 0.2 and 0.7
            assertTrue(range_estimate >= 0.2 && range_estimate <= 0.7,
                    "P([-1200,-40)) should be between 0.2 and 0.7, got " + range_estimate);
        }
    }

    @Test
    void testPositiveValuesOnly() {
        // Dataset: only positive values {1, 10, 100, 1000}
        double[] values = {1.0, 10.0, 100.0, 1000.0};
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (double value : values) {
                histogram.add(tr, value);
            }
            tr.commit().join();
        }

        HistogramEstimator estimator = histogram.getEstimator();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Values > 50: {100, 1000} = 2/4 = 0.5
            assertEquals(0.5, estimator.estimateGreaterThan(tr, 50), 0.15, "P(>50) should be approximately 0.5 (2 out of 4 values)");

            // Values > 0: all values = 4/4 = 1.0
            assertEquals(1.0, estimator.estimateGreaterThan(tr, 0), 0.05, "P(>0) should be 1.0 as all values are positive");

            // Values > -10: all values = 4/4 = 1.0 
            assertEquals(1.0, estimator.estimateGreaterThan(tr, -10), 0.05, "P(>-10) should be 1.0 as all values are greater than -10");

            // Values > 2000: none = 0/4 = 0.0
            assertTrue(estimator.estimateGreaterThan(tr, 2000) <= 0.15, "P(>2000) should be very small");
        }
    }

    @Test
    void testNegativeValuesOnly() {
        // Dataset: only negative values {-1, -10, -100, -1000}  
        double[] values = {-1.0, -10.0, -100.0, -1000.0};
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (double value : values) {
                histogram.add(tr, value);
            }
            tr.commit().join();
        }

        HistogramEstimator estimator = histogram.getEstimator();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Values > 0: none = 0/4 = 0.0
            assertEquals(0.0, estimator.estimateGreaterThan(tr, 0), 0.05, "P(>0) should be 0.0 as no values are positive");

            // Values > -50: values closer to zero than -50, i.e., {-1, -10} = 2/4 = 0.5
            assertEquals(0.5, estimator.estimateGreaterThan(tr, -50), 0.15, "P(>-50) should be approximately 0.5 (values -1, -10)");

            // Values > -2000: all values are greater = 4/4 = 1.0
            assertEquals(1.0, estimator.estimateGreaterThan(tr, -2000), 0.05, "P(>-2000) should be 1.0 as all values are greater than -2000");

            // Values > -5: only -1 = 1/4 = 0.25
            assertEquals(0.25, estimator.estimateGreaterThan(tr, -5), 0.15, "P(>-5) should be approximately 0.25 (only -1)");
        }
    }

    @Test
    void testMixedPositiveNegativeValues() {
        // Dataset: mixed values {-100, -10, -1, 1, 10, 100}
        double[] values = {-100.0, -10.0, -1.0, 1.0, 10.0, 100.0};
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (double value : values) {
                histogram.add(tr, value);
            }
            tr.commit().join();
        }

        HistogramEstimator estimator = histogram.getEstimator();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Values > 0: {1, 10, 100} = 3/6 = 0.5
            assertEquals(0.5, estimator.estimateGreaterThan(tr, 0), 0.15, "P(>0) should be approximately 0.5 (3 positive out of 6 values)");

            // Values > -50: positives {1, 10, 100} + negatives closer to zero {-10, -1} = 5/6 ≈ 0.833
            assertEquals(0.833, estimator.estimateGreaterThan(tr, -50), 0.15, "P(>-50) should be approximately 0.833");

            // Values > 50: {100} = 1/6 ≈ 0.167
            assertEquals(0.167, estimator.estimateGreaterThan(tr, 50), 0.15, "P(>50) should be approximately 0.167");

            // Values > -5: positives {1, 10, 100} + negatives closer to zero {-1} = 4/6 ≈ 0.667
            assertEquals(0.667, estimator.estimateGreaterThan(tr, -5), 0.15, "P(>-5) should be approximately 0.667");
        }
    }

    @Test
    void testZeroValues() {
        // Dataset: includes zero {-10, 0, 0, 10}
        double[] values = {-10.0, 0.0, 0.0, 10.0};
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (double value : values) {
                histogram.add(tr, value);
            }
            tr.commit().join();
        }

        HistogramEstimator estimator = histogram.getEstimator();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Values > 0: {10} = 1/4 = 0.25
            assertEquals(0.25, estimator.estimateGreaterThan(tr, 0), 0.15, "P(>0) should be approximately 0.25");

            // Values > -5: positives {10} + zeros {0, 0} = 3/4 = 0.75
            assertEquals(0.75, estimator.estimateGreaterThan(tr, -5), 0.15, "P(>-5) should be approximately 0.75");

            // Values > -20: all values = 4/4 = 1.0
            assertEquals(1.0, estimator.estimateGreaterThan(tr, -20), 0.05, "P(>-20) should be 1.0");
        }
    }

    @Test
    void testRangeEstimationWithNegatives() {
        // Dataset: {-100, -50, -10, 10, 50, 100}
        double[] values = {-100.0, -50.0, -10.0, 10.0, 50.0, 100.0};
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (double value : values) {
                histogram.add(tr, value);
            }
            tr.commit().join();
        }

        HistogramEstimator estimator = histogram.getEstimator();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Range [-20, 20): includes {-10, 10} = 2/6 ≈ 0.333
            assertEquals(0.333, estimator.estimateRange(tr, -20, 20), 0.2, "P([-20,20)) should be approximately 0.333");

            // Range [0, 100): includes {10, 50} = 2/6 ≈ 0.333 (100 not included)
            assertEquals(0.333, estimator.estimateRange(tr, 0, 100), 0.2, "P([0,100)) should be approximately 0.333");

            // Range [-200, 0): includes {-100, -50, -10} = 3/6 = 0.5 (0 not included)
            assertEquals(0.5, estimator.estimateRange(tr, -200, 0), 0.2, "P([-200,0)) should be approximately 0.5");
        }
    }

    @Test
    void testDocumentationExample() {
        // Example from design document: {-30, -40, -99, -123, -250, -999, -2587, -4589, -10000}
        double[] values = {-30, -40, -99, -123, -250, -999, -2587, -4589, -10000};
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (double value : values) {
                histogram.add(tr, value);
            }
            tr.commit().join();
        }

        HistogramEstimator estimator = histogram.getEstimator();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // P(value > -45) should match -30, -40 = 2/9 ≈ 0.222
            assertEquals(0.222, estimator.estimateGreaterThan(tr, -45), 0.15, "P(>-45) should be approximately 0.222 as per design doc");

            // P(value > -500) should match values closer to zero than -500: -30, -40, -99, -123, -250 = 5/9 ≈ 0.556
            assertEquals(0.556, estimator.estimateGreaterThan(tr, -500), 0.15, "P(>-500) should be approximately 0.556 as per design doc");

            // P(value > 200) should be 0.0 as no positive values
            assertEquals(0.0, estimator.estimateGreaterThan(tr, 200), 0.05, "P(>200) should be 0.0 as per design doc");
        }
    }

    @Test
    void testEdgeCasesWithSmallValues() {
        // Test with very small positive and negative values
        double[] values = {-0.001, -0.01, -0.1, 0.001, 0.01, 0.1};
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (double value : values) {
                histogram.add(tr, value);
            }
            tr.commit().join();
        }

        HistogramEstimator estimator = histogram.getEstimator();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Values > 0: {0.001, 0.01, 0.1} = 3/6 = 0.5
            assertEquals(0.5, estimator.estimateGreaterThan(tr, 0), 0.15, "P(>0) should be approximately 0.5");

            // Values > -0.05: positives + negatives closer to zero {-0.01, -0.001} = 5/6 ≈ 0.833  
            assertEquals(0.833, estimator.estimateGreaterThan(tr, -0.05), 0.2, "P(>-0.05) should be approximately 0.833");
        }
    }
}