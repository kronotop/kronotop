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

class HistogramWindowManagerTest extends BaseStandaloneInstanceTest {
    
    private FDBLogHistogram histogram;
    private HistogramWindowManager windowManager;
    private String testBucket; // Will be unique per test
    private final String testField = "price";
    
    @BeforeEach
    void setUp() {
        histogram = new FDBLogHistogram(instance.getContext().getFoundationDB());
        windowManager = histogram.createWindowManager();
        testBucket = "test_bucket_" + System.nanoTime(); // Unique bucket per test
    }
    
    @Test
    void testWindowMaintenanceWithSmallWindow() {
        // Use small window to force evictions
        HistogramMetadata metadata = new HistogramMetadata(16, 4, 3, 16, 1);
        histogram.initialize(testBucket, testField, metadata);
        
        // Add values that span many decades to trigger window overflow
        double[] values = {
            1.0,     // decade 0  
            15.0,    // decade 1
            150.0,   // decade 2  
            1500.0,  // decade 3
            15000.0, // decade 4
            150000.0 // decade 5
        };
        
        for (double value : values) {
            histogram.add(testBucket, testField, value);
        }
        
        // Get initial stats
        HistogramWindowManager.WindowStats initialStats = windowManager.getWindowStats(testBucket, testField, metadata);
        assertTrue(initialStats.activeDecadeCount() > metadata.windowDecades());
        
        // Perform window maintenance
        windowManager.maintainWindow(testBucket, testField, metadata);
        
        // Check that window was reduced
        HistogramWindowManager.WindowStats finalStats = windowManager.getWindowStats(testBucket, testField, metadata);
        assertTrue(finalStats.activeDecadeCount() <= metadata.windowDecades());
        
        // Check that underflow summary was updated
        assertTrue(finalStats.underflowSum() > 0);
    }
    
    @Test
    void testWindowMaintenanceWithinLimits() {
        HistogramMetadata metadata = new HistogramMetadata(16, 4, 10, 16, 1);
        histogram.initialize(testBucket, testField, metadata);
        
        // Add values that don't exceed window size
        double[] values = {10.0, 20.0, 30.0, 40.0, 50.0};
        for (double value : values) {
            histogram.add(testBucket, testField, value);
        }
        
        HistogramWindowManager.WindowStats initialStats = windowManager.getWindowStats(testBucket, testField, metadata);
        
        // Perform maintenance
        windowManager.maintainWindow(testBucket, testField, metadata);
        
        // Window should remain unchanged
        HistogramWindowManager.WindowStats finalStats = windowManager.getWindowStats(testBucket, testField, metadata);
        assertEquals(initialStats.activeDecadeCount(), finalStats.activeDecadeCount());
        assertEquals(initialStats.underflowSum(), finalStats.underflowSum());
        assertEquals(initialStats.overflowSum(), finalStats.overflowSum());
    }
    
    @Test
    void testEmptyHistogramStats() {
        HistogramMetadata metadata = HistogramMetadata.defaultMetadata();
        histogram.initialize(testBucket, testField, metadata);
        
        HistogramWindowManager.WindowStats stats = windowManager.getWindowStats(testBucket, testField, metadata);
        
        assertEquals(0, stats.activeDecadeCount());
        assertNull(stats.minActiveDecade());
        assertNull(stats.maxActiveDecade());
        assertEquals(0, stats.underflowSum());
        assertEquals(0, stats.overflowSum());
    }
    
    @Test
    void testWindowStatsAfterDataAdded() {
        HistogramMetadata metadata = HistogramMetadata.defaultMetadata();
        histogram.initialize(testBucket, testField, metadata);
        
        // Add values in different decades
        histogram.add(testBucket, testField, 1.5);      // decade 0
        histogram.add(testBucket, testField, 25.0);     // decade 1  
        histogram.add(testBucket, testField, 350.0);    // decade 2
        histogram.add(testBucket, testField, 4700.0);   // decade 3
        
        HistogramWindowManager.WindowStats stats = windowManager.getWindowStats(testBucket, testField, metadata);
        
        assertEquals(4, stats.activeDecadeCount());
        assertEquals(Integer.valueOf(0), stats.minActiveDecade());
        assertEquals(Integer.valueOf(3), stats.maxActiveDecade());
        assertEquals(0, stats.underflowSum()); // No evictions yet
        assertEquals(0, stats.overflowSum());  // No evictions yet
    }
    
    @Test
    void testWindowMaintenancePreservesEstimation() {
        // Create histogram with small window to force eviction
        HistogramMetadata metadata = new HistogramMetadata(16, 4, 2, 16, 1);
        histogram.initialize(testBucket, testField, metadata);
        
        // Add values across multiple decades
        double[] values = {1.0, 10.0, 100.0, 1000.0, 10000.0};
        for (double value : values) {
            histogram.add(testBucket, testField, value);
        }
        
        HistogramEstimator estimator = histogram.createEstimator(testBucket, testField);
        
        // Get estimate before maintenance
        double beforeMaintenance = estimator.estimateGreaterThan(0.5);
        assertTrue(beforeMaintenance > 0.9); // Should be close to 1.0
        
        // Perform maintenance
        windowManager.maintainWindow(testBucket, testField, metadata);
        
        // Get estimate after maintenance
        double afterMaintenance = estimator.estimateGreaterThan(0.5);
        
        // Estimate should still be reasonable (data preserved in summaries)
        assertTrue(afterMaintenance > 0.8);
    }
    
    @Test
    void testMultipleBucketMaintenance() {
        String bucket1 = "bucket1";
        String bucket2 = "bucket2";
        String field = "test_field";
        
        HistogramMetadata metadata = new HistogramMetadata(16, 4, 3, 16, 1);
        
        // Initialize histograms for both buckets
        histogram.initialize(bucket1, field, metadata);
        histogram.initialize(bucket2, field, metadata);
        
        // Add data that will trigger evictions
        double[] values = {1.0, 50.0, 2500.0, 125000.0, 6250000.0};
        for (double value : values) {
            histogram.add(bucket1, field, value);
            histogram.add(bucket2, field, value * 2); // Different data for bucket2
        }
        
        // Perform maintenance on both
        java.util.List<String> fields = java.util.List.of(field);
        windowManager.maintainBucketWindows(bucket1, fields, metadata);
        windowManager.maintainBucketWindows(bucket2, fields, metadata);
        
        // Both should have window constraints enforced
        HistogramWindowManager.WindowStats stats1 = windowManager.getWindowStats(bucket1, field, metadata);
        HistogramWindowManager.WindowStats stats2 = windowManager.getWindowStats(bucket2, field, metadata);
        
        assertTrue(stats1.activeDecadeCount() <= metadata.windowDecades());
        assertTrue(stats2.activeDecadeCount() <= metadata.windowDecades());
        
        // Both should have some evicted data
        assertTrue(stats1.underflowSum() > 0);
        assertTrue(stats2.underflowSum() > 0);
    }
}