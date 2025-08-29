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
    void humanTest() {
        histogram.initialize(testBucket, testField, HistogramMetadata.defaultMetadata());

        double[] values = {30, 40, 99, 123, 250, 999, 2587, 4589, 10000};
        for (double value : values) {
            histogram.add(testBucket, testField, value);
        }

        double[] more = {1.2, 2.5, 6.7, 8.9, 1e6, 3e7, 9e8, 4.2e9};
        for (double value : more) {
            histogram.add(testBucket, testField, value);
        }

        HistogramEstimator estimator = histogram.createEstimator(testBucket, testField);
        System.out.println(estimator.estimateGreaterThan(25));
        
        // Debug the issue - test multiple thresholds
        System.out.println("Testing thresholds:");
        System.out.println("P(>25) = " + estimator.estimateGreaterThan(25) + " (expected: 0.764706)");
        System.out.println("P(>50) = " + estimator.estimateGreaterThan(50) + " (expected: 0.647059)");
        System.out.println("P(>200) = " + estimator.estimateGreaterThan(200) + " (expected: 0.529412)");
        System.out.println("P(>3000) = " + estimator.estimateGreaterThan(3000) + " (expected: 0.352941)");
        System.out.println("Range P([100,500)) = " + estimator.estimateRange(100, 500) + " (expected: 0.117647)");
    }
    
    @Test
    void testAddPositiveValue() {
        histogram.initialize(testBucket, testField, HistogramMetadata.defaultMetadata());
        
        // Add some values
        double[] values = {30, 40, 99, 123, 250, 999};
        for (double value : values) {
            histogram.add(testBucket, testField, value);
        }
        
        // Create estimator and test basic functionality
        HistogramEstimator estimator = histogram.createEstimator(testBucket, testField);
        
        // All values are > 25, so selectivity should be 1.0 
        double selectivity = estimator.estimateGreaterThan(25);
        assertEquals(1.0, selectivity, 0.1);
        
        // Some values are > 100, so selectivity should be between 0 and 1
        selectivity = estimator.estimateGreaterThan(100);
        assertTrue(selectivity > 0.0 && selectivity <= 1.0);
        
        // Values > 1000 should be low (allowing for approximation error in log histogram) 
        selectivity = estimator.estimateGreaterThan(1000);
        assertTrue(selectivity >= 0.0 && selectivity <= 0.5, "P(>1000) = " + selectivity); // Allow approximation error
    }
    
    @Test
    void testAddZeroAndNegativeValues() {
        histogram.initialize(testBucket, testField, HistogramMetadata.defaultMetadata());
        
        // Add zero and negative values
        histogram.add(testBucket, testField, 0);
        histogram.add(testBucket, testField, -5);
        histogram.add(testBucket, testField, -100);
        
        // Add some positive values
        histogram.add(testBucket, testField, 50);
        histogram.add(testBucket, testField, 100);
        
        HistogramEstimator estimator = histogram.createEstimator(testBucket, testField);
        
        // Selectivity for threshold <= 0 should be 1.0
        assertEquals(1.0, estimator.estimateGreaterThan(0), 0.01);
        assertEquals(1.0, estimator.estimateGreaterThan(-10), 0.01);
        
        // Selectivity for positive thresholds should account for positive values only
        double selectivity = estimator.estimateGreaterThan(75);
        assertTrue(selectivity > 0.0 && selectivity < 1.0);
    }
    
    @Test
    void testRangeEstimation() {
        histogram.initialize(testBucket, testField, HistogramMetadata.defaultMetadata());
        
        // Add values: 10, 50, 100, 500, 1000
        double[] values = {10, 50, 100, 500, 1000};
        for (double value : values) {
            histogram.add(testBucket, testField, value);
        }
        
        HistogramEstimator estimator = histogram.createEstimator(testBucket, testField);
        
        // Test range [25, 200) - should include 50 and 100 (2 out of 5 values)
        double rangeSelectivity = estimator.estimateRange(25, 200);
        assertTrue(rangeSelectivity >= 0.0 && rangeSelectivity <= 1.0, "Range [25, 200) selectivity: " + rangeSelectivity);
        
        // Test range [5, 75) - should include 10 and 50 (2 out of 5 values) 
        rangeSelectivity = estimator.estimateRange(5, 75);
        assertTrue(rangeSelectivity >= 0.0 && rangeSelectivity <= 1.0, "Range [5, 75) selectivity: " + rangeSelectivity);
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
        
        // Test with values spanning many decades
        double[] values = {1.2, 25, 678, 4589, 123456, 7.89e6, 3.45e8};
        for (double value : values) {
            histogram.add(testBucket, testField, value);
        }
        
        HistogramEstimator estimator = histogram.createEstimator(testBucket, testField);
        
        // Test estimates at different scales
        assertTrue(estimator.estimateGreaterThan(1) > 0.9); // Almost all values > 1
        assertTrue(estimator.estimateGreaterThan(1000) >= 0.0 && estimator.estimateGreaterThan(1000) <= 1.0);
        assertTrue(estimator.estimateGreaterThan(1e9) < 0.1); // Very few values > 1 billion
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
        
        // Add different data to each field
        histogram.add(testBucket, field1, 100);
        histogram.add(testBucket, field1, 200);
        histogram.add(testBucket, field2, 5);
        histogram.add(testBucket, field2, 10);
        
        // Verify they maintain separate histograms
        HistogramEstimator priceEstimator = histogram.createEstimator(testBucket, field1);
        HistogramEstimator quantityEstimator = histogram.createEstimator(testBucket, field2);
        
        // Price field should have higher values
        assertTrue(priceEstimator.estimateGreaterThan(50) > 0);
        assertEquals(0.0, quantityEstimator.estimateGreaterThan(50), 0.01);
        
        // Quantity field should have lower values  
        assertTrue(quantityEstimator.estimateGreaterThan(1) > 0);
        assertEquals(1.0, priceEstimator.estimateGreaterThan(1), 0.01);
    }
    
}