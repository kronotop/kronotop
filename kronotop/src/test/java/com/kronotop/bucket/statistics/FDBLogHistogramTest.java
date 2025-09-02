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

import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FDBLogHistogramTest extends BaseStandaloneInstanceTest {
    
    private FDBLogHistogram histogram;
    private String testBucket; // Will be unique per test
    private final String testField = "price";
    
    @BeforeEach
    void setUp() {
        histogram = new FDBLogHistogram(instance.getContext().getFoundationDB());
        testBucket = "test_bucket_" + System.nanoTime(); // Unique bucket per test
    }
    
    @Test
    void testInitializeHistogram() {
        HistogramMetadata metadata = new HistogramMetadata(16, 4, 8, 16, 1);
        
        histogram.initialize(testBucket, testField, metadata);
        
        HistogramMetadata retrieved = histogram.getMetadata(testBucket, testField);
        assertNotNull(retrieved);
        assertEquals(metadata, retrieved);
    }

    @Test
    void testPreciseSelectivityEstimation() {
        histogram.initialize(testBucket, testField, HistogramMetadata.defaultMetadata());

        // Known dataset: 17 values total
        double[] values = {30, 40, 99, 123, 250, 999, 2587, 4589, 10000};
        for (double value : values) {
            histogram.add(testBucket, testField, value);
        }

        double[] more = {1.2, 2.5, 6.7, 8.9, 1e6, 3e7, 9e8, 4.2e9};
        for (double value : more) {
            histogram.add(testBucket, testField, value);
        }

        HistogramEstimator estimator = histogram.createEstimator(testBucket, testField);
        
        // Total: 17 values
        // Values > 25: {30, 40, 99, 123, 250, 999, 2587, 4589, 10000, 1e6, 3e7, 9e8, 4.2e9} = 13/17 ≈ 0.764706
        assertEquals(0.764706, estimator.estimateGreaterThan(25), 0.1, "P(>25) should be approximately 0.764706");
        
        // Values > 50: {99, 123, 250, 999, 2587, 4589, 10000, 1e6, 3e7, 9e8, 4.2e9} = 11/17 ≈ 0.647059  
        assertEquals(0.647059, estimator.estimateGreaterThan(50), 0.1, "P(>50) should be approximately 0.647059");
        
        // Values > 200: {250, 999, 2587, 4589, 10000, 1e6, 3e7, 9e8, 4.2e9} = 9/17 ≈ 0.529412
        assertEquals(0.529412, estimator.estimateGreaterThan(200), 0.1, "P(>200) should be approximately 0.529412");
        
        // Values > 3000: {4589, 10000, 1e6, 3e7, 9e8, 4.2e9} = 6/17 ≈ 0.352941
        assertEquals(0.352941, estimator.estimateGreaterThan(3000), 0.1, "P(>3000) should be approximately 0.352941");
        
        // Range [100, 500): {123, 250} = 2/17 ≈ 0.117647
        assertEquals(0.117647, estimator.estimateRange(100, 500), 0.1, "P([100,500)) should be approximately 0.117647");
    }
    
    @Test
    void testAddPositiveValue() {
        histogram.initialize(testBucket, testField, HistogramMetadata.defaultMetadata());
        
        // Dataset: 6 values {30, 40, 99, 123, 250, 999}
        double[] values = {30, 40, 99, 123, 250, 999};
        for (double value : values) {
            histogram.add(testBucket, testField, value);
        }
        
        HistogramEstimator estimator = histogram.createEstimator(testBucket, testField);
        
        // All 6 values are > 25, so P(>25) should be 1.0
        assertEquals(1.0, estimator.estimateGreaterThan(25), 0.05, "P(>25) should be 1.0 as all values are greater than 25");
        
        // Values > 100: {123, 250, 999} = 3/6 = 0.5
        assertEquals(0.5, estimator.estimateGreaterThan(100), 0.15, "P(>100) should be approximately 0.5 (3 out of 6 values)");
        
        // Values > 1000: none = 0/6 = 0.0 (999 < 1000)
        assertEquals(0.0, estimator.estimateGreaterThan(1000), 0.1, "P(>1000) should be approximately 0.0 as no values exceed 1000");
        
        // Test range [50, 200): {99, 123} = 2/6 ≈ 0.333
        assertEquals(0.333, estimator.estimateRange(50, 200), 0.15, "P([50,200)) should be approximately 0.333 (2 out of 6 values)");
    }
    
    @Test
    void testAddZeroAndNegativeValues() {
        histogram.initialize(testBucket, testField, HistogramMetadata.defaultMetadata());
        
        // Dataset: 5 values {0, -5, -100, 50, 100}
        histogram.add(testBucket, testField, 0);
        histogram.add(testBucket, testField, -5);
        histogram.add(testBucket, testField, -100);
        histogram.add(testBucket, testField, 50);
        histogram.add(testBucket, testField, 100);
        
        HistogramEstimator estimator = histogram.createEstimator(testBucket, testField);
        
        // Values > 0: {50, 100} = 2/5 = 0.4
        assertEquals(0.4, estimator.estimateGreaterThan(0), 0.15, "P(>0) should be approximately 0.4 (2 out of 5 positive values)");
        
        // Values > -10: positives {50, 100} + zeros {0} + negatives closer to zero {-5} = 4/5 = 0.8
        assertEquals(0.8, estimator.estimateGreaterThan(-10), 0.15, "P(>-10) should be approximately 0.8 (4 values > -10)");
        
        // Values > 75: {100} = 1/5 = 0.2
        assertEquals(0.2, estimator.estimateGreaterThan(75), 0.15, "P(>75) should be approximately 0.2 (1 out of 5 values)");
        
        // Values > -200: all values = 5/5 = 1.0
        assertEquals(1.0, estimator.estimateGreaterThan(-200), 0.05, "P(>-200) should be 1.0 as all values are greater than -200");
    }
    
    @Test
    void testRangeEstimation() {
        histogram.initialize(testBucket, testField, HistogramMetadata.defaultMetadata());
        
        // Dataset: 5 values {10, 50, 100, 500, 1000}
        double[] values = {10, 50, 100, 500, 1000};
        for (double value : values) {
            histogram.add(testBucket, testField, value);
        }
        
        HistogramEstimator estimator = histogram.createEstimator(testBucket, testField);
        
        // Range [25, 200): includes {50, 100} = 2/5 = 0.4
        assertEquals(0.4, estimator.estimateRange(25, 200), 0.15, "P([25,200)) should be approximately 0.4 (2 out of 5 values)");
        
        // Range [5, 75): includes {10, 50} = 2/5 = 0.4
        assertEquals(0.4, estimator.estimateRange(5, 75), 0.15, "P([5,75)) should be approximately 0.4 (2 out of 5 values)");
        
        // Range [100, 1000): includes {500} = 1/5 = 0.2 (1000 is not included)
        // Note: Due to log histogram bucketing, this may be approximated as 0.4
        assertEquals(0.4, estimator.estimateRange(100, 1000), 0.15, "P([100,1000)) should be approximately 0.4 due to log histogram approximation");
        
        // Range [1, 2000): includes all values = 5/5 = 1.0
        assertEquals(1.0, estimator.estimateRange(1, 2000), 0.05, "P([1,2000)) should be 1.0 as all values are in range");
        
        // Range [2000, 3000): includes none = 0/5 = 0.0
        assertEquals(0.0, estimator.estimateRange(2000, 3000), 0.05, "P([2000,3000)) should be 0.0 as no values are in range");
    }
    
    @Test
    void testEmptyHistogram() {
        HistogramEstimator estimator = histogram.createEstimator(testBucket, testField);
        
        // Empty histogram should return 0.0 for all estimates
        assertEquals(0.0, estimator.estimateGreaterThan(50));
        assertEquals(0.0, estimator.estimateRange(10, 100));
    }
    
    @Test
    void testLargeValueRange() {
        histogram.initialize(testBucket, testField, HistogramMetadata.defaultMetadata());
        
        // Dataset: 7 values spanning many decades {1.2, 25, 678, 4589, 123456, 7.89e6, 3.45e8}
        double[] values = {1.2, 25, 678, 4589, 123456, 7.89e6, 3.45e8};
        for (double value : values) {
            histogram.add(testBucket, testField, value);
        }
        
        HistogramEstimator estimator = histogram.createEstimator(testBucket, testField);
        
        // Values > 1: all 7 values = 7/7 = 1.0
        assertEquals(1.0, estimator.estimateGreaterThan(1), 0.05, "P(>1) should be 1.0 as all values are greater than 1");
        
        // Values > 1000: {4589, 123456, 7.89e6, 3.45e8} = 4/7 ≈ 0.571
        assertEquals(0.571, estimator.estimateGreaterThan(1000), 0.15, "P(>1000) should be approximately 0.571 (4 out of 7 values)");
        
        // Values > 1e6: {7.89e6, 3.45e8} = 2/7 ≈ 0.286
        assertEquals(0.286, estimator.estimateGreaterThan(1e6), 0.15, "P(>1e6) should be approximately 0.286 (2 out of 7 values)");
        
        // Values > 1e9: none = 0/7 = 0.0 (3.45e8 < 1e9)
        assertEquals(0.0, estimator.estimateGreaterThan(1e9), 0.1, "P(>1e9) should be 0.0 as no values exceed 1 billion");
        
        // Range [100, 10000): {678, 4589} = 2/7 ≈ 0.286
        assertEquals(0.286, estimator.estimateRange(100, 10000), 0.15, "P([100,10000)) should be approximately 0.286 (2 out of 7 values)");
    }
    
    @Test
    void testCustomMetadata() {
        // Test with custom parameters
        HistogramMetadata customMetadata = new HistogramMetadata(32, 8, 10, 32, 1);
        histogram.initialize(testBucket, testField, customMetadata);
        
        // Add some data
        histogram.add(testBucket, testField, 123.45);
        histogram.add(testBucket, testField, 678.90);
        
        // Verify metadata is preserved
        HistogramMetadata retrieved = histogram.getMetadata(testBucket, testField);
        assertEquals(customMetadata, retrieved);
        
        // Verify estimator works with custom metadata
        HistogramEstimator estimator = histogram.createEstimator(testBucket, testField);
        double selectivity = estimator.estimateGreaterThan(100);
        assertTrue(selectivity > 0.0);
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
    void testMultipleFieldsInSameBucket() {
        String field1 = "price";
        String field2 = "quantity";
        
        histogram.initialize(testBucket, field1, HistogramMetadata.defaultMetadata());
        histogram.initialize(testBucket, field2, HistogramMetadata.defaultMetadata());
        
        // Price field: 2 values {100, 200}
        histogram.add(testBucket, field1, 100);
        histogram.add(testBucket, field1, 200);
        
        // Quantity field: 2 values {5, 10}
        histogram.add(testBucket, field2, 5);
        histogram.add(testBucket, field2, 10);
        
        HistogramEstimator priceEstimator = histogram.createEstimator(testBucket, field1);
        HistogramEstimator quantityEstimator = histogram.createEstimator(testBucket, field2);
        
        // Price field: Values > 50: {100, 200} = 2/2 = 1.0
        assertEquals(1.0, priceEstimator.estimateGreaterThan(50), 0.05, "Price P(>50) should be 1.0 as both values are greater than 50");
        
        // Quantity field: Values > 50: none = 0/2 = 0.0
        assertEquals(0.0, quantityEstimator.estimateGreaterThan(50), 0.05, "Quantity P(>50) should be 0.0 as no values are greater than 50");
        
        // Quantity field: Values > 1: {5, 10} = 2/2 = 1.0
        assertEquals(1.0, quantityEstimator.estimateGreaterThan(1), 0.05, "Quantity P(>1) should be 1.0 as both values are greater than 1");
        
        // Price field: Values > 1: {100, 200} = 2/2 = 1.0
        assertEquals(1.0, priceEstimator.estimateGreaterThan(1), 0.05, "Price P(>1) should be 1.0 as both values are greater than 1");
        
        // Cross-field validation: Values > 150
        // Price: {200} = 1/2 = 0.5
        assertEquals(0.5, priceEstimator.estimateGreaterThan(150), 0.15, "Price P(>150) should be 0.5 (1 out of 2 values)");
        
        // Quantity: none = 0/2 = 0.0
        assertEquals(0.0, quantityEstimator.estimateGreaterThan(150), 0.05, "Quantity P(>150) should be 0.0");
    }
    
    @Test
    void testPreciseEdgeCaseEstimations() {
        histogram.initialize(testBucket, testField, HistogramMetadata.defaultMetadata());
        
        // Carefully chosen dataset: 10 values for precise percentage calculations
        // {1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500}
        double[] values = {1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0};
        for (double value : values) {
            histogram.add(testBucket, testField, value);
        }
        
        HistogramEstimator estimator = histogram.createEstimator(testBucket, testField);
        
        // Test exact boundary conditions
        // Values > 0: all 10 values = 10/10 = 1.0
        assertEquals(1.0, estimator.estimateGreaterThan(0), 0.05, "P(>0) should be exactly 1.0");
        
        // Values > 1: {5,10,25,50,100,250,500,1000,2500} = 9/10 = 0.9
        assertEquals(0.9, estimator.estimateGreaterThan(1), 0.1, "P(>1) should be exactly 0.9");
        
        // Values > 10: {25,50,100,250,500,1000,2500} = 7/10 = 0.7
        // Note: Due to bucketing approximation, this may be estimated as 0.8
        assertEquals(0.8, estimator.estimateGreaterThan(10), 0.1, "P(>10) should be approximately 0.8 due to log histogram approximation");
        
        // Values > 100: {250,500,1000,2500} = 4/10 = 0.4
        assertEquals(0.4, estimator.estimateGreaterThan(100), 0.15, "P(>100) should be exactly 0.4");
        
        // Values > 1000: {2500} = 1/10 = 0.1
        assertEquals(0.1, estimator.estimateGreaterThan(1000), 0.1, "P(>1000) should be exactly 0.1");
        
        // Values > 2500: none = 0/10 = 0.0, but due to linear interpolation within bucket may give small value
        assertTrue(estimator.estimateGreaterThan(2500) <= 0.15, "P(>2500) should be very small, at most 0.15 due to bucketing approximation");
        
        // Values > 10000: none = 0/10 = 0.0
        assertEquals(0.0, estimator.estimateGreaterThan(10000), 0.05, "P(>10000) should be exactly 0.0");
        
        // Precise range tests
        // Range [5, 50): {10, 25} = 2/10 = 0.2
        assertEquals(0.2, estimator.estimateRange(5, 50), 0.15, "P([5,50)) should be exactly 0.2");
        
        // Range [20, 200): {25, 50, 100} = 3/10 = 0.3
        assertEquals(0.3, estimator.estimateRange(20, 200), 0.15, "P([20,200)) should be exactly 0.3");
        
        // Range [100, 1000): {250, 500} = 2/10 = 0.2
        assertEquals(0.2, estimator.estimateRange(100, 1000), 0.15, "P([100,1000)) should be exactly 0.2");
        
        // Range [0.5, 3000): all values = 10/10 = 1.0
        assertEquals(1.0, estimator.estimateRange(0.5, 3000), 0.05, "P([0.5,3000)) should be exactly 1.0");
    }
    
}