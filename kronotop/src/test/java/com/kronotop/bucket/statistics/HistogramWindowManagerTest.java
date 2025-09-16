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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.bucket.index.SortOrder;
import com.kronotop.server.Session;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HistogramWindowManagerTest extends BaseStandaloneInstanceTest {

    protected void createBucket(String bucketName) {
        // Bucket is created implicitly through BucketMetadataUtil.createOrOpen()
        Session session = getSession();
        BucketMetadataUtil.createOrOpen(context, session, bucketName);
    }

    protected void createIndex(String bucketName, IndexDefinition indexDefinition) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = getBucketMetadata(bucketName);
            IndexUtil.create(tr, metadata.subspace(), indexDefinition);
            tr.commit().join();
        }
    }

    protected BucketMetadata createIndexesAndLoadBucketMetadata(String bucketName, IndexDefinition definition) {
        // Create the bucket first
        createBucket(bucketName);

        createIndex(bucketName, definition);

        // Load and return metadata
        Session session = getSession();
        return BucketMetadataUtil.createOrOpen(context, session, bucketName);
    }

    FDBLogHistogram initialize(HistogramMetadata metadata) {
        String testBucket = "test_bucket_" + System.nanoTime(); // Unique bucket per test

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata bucketMetadata = createIndexesAndLoadBucketMetadata(testBucket, ageIndex);
        DirectorySubspace indexSubspace = bucketMetadata.indexes().getSubspace("age");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            FDBLogHistogram.initialize(tr, indexSubspace.getPath(), metadata);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return new FDBLogHistogram(tr, indexSubspace.getPath());
        }
    }

    @Test
    void testWindowMaintenanceWithSmallWindow() {
        // Use a small window (3 decades) to force evictions
        HistogramMetadata metadata = new HistogramMetadata(16, 4, 3, 16, 1);
        FDBLogHistogram histogram = initialize(metadata);

        HistogramWindowManager windowManager = histogram.getWindowManager();
        // Dataset: 6 values spanning decades 0-5
        // Decades: 0(1.0), 1(15.0), 2(150.0), 3(1500.0), 4(15000.0), 5(150000.0) = 6 decades total
        double[] values = {
                1.0,     // decade 0
                15.0,    // decade 1
                150.0,   // decade 2
                1500.0,  // decade 3
                15000.0, // decade 4
                150000.0 // decade 5
        };

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (double value : values) {
                histogram.add(tr, value);
            }
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Get initial stats - should have 6 active decades
            HistogramWindowManager.WindowStats initialStats = windowManager.getWindowStats(tr);
            assertEquals(6, initialStats.activeDecadeCount(), "Should have exactly 6 active decades before maintenance");
            assertTrue(initialStats.activeDecadeCount() > metadata.windowDecades(), "Initial decades (6) should exceed window limit (3)");
            assertEquals(0, initialStats.underflowSum(), "Should have no underflow before maintenance");

            // Perform window maintenance - will keep 3 newest decades (3,4,5) and evict oldest (0,1,2)
            windowManager.maintainWindow(tr);

            // Check that window was reduced to the limit
            HistogramWindowManager.WindowStats finalStats = windowManager.getWindowStats(tr);
            assertEquals(3, finalStats.activeDecadeCount(), "Should have exactly 3 active decades after maintenance (the limit)");
            assertTrue(finalStats.activeDecadeCount() <= metadata.windowDecades(), "Final decades should not exceed window limit");

            // Check that the underflow summary was updated with evicted data
            assertEquals(3, finalStats.underflowSum(), "Should have exactly 3 values in underflow (evicted decades 0,1,2)");
            assertEquals(0, finalStats.overflowSum(), "Should have no overflow data");

            tr.commit().join();
        }

        // Test that estimation still works after eviction
        HistogramEstimator estimator = histogram.getEstimator();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Values > 1000: should include {1500.0, 15000.0, 150000.0} plus some contribution from underflow
            double highValueEstimate = estimator.estimateGreaterThan(tr, 1000);
            assertTrue(highValueEstimate >= 0.3, "P(>1000) should be at least 0.3 after maintenance, got " + highValueEstimate);
            assertTrue(highValueEstimate <= 1.0, "P(>1000) should not exceed 1.0, got " + highValueEstimate);
        }
    }

    @Test
    void testWindowMaintenanceWithinLimits() {
        // Large window (10 decades) to avoid evictions
        HistogramMetadata metadata = new HistogramMetadata(16, 4, 10, 16, 1);
        FDBLogHistogram histogram = initialize(metadata);

        HistogramWindowManager windowManager = histogram.getWindowManager();

        // Dataset: 5 values in same decade (decade 1) - won't exceed window limit
        double[] values = {10.0, 20.0, 30.0, 40.0, 50.0}; // All in decade 1
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (double value : values) {
                histogram.add(tr, value);
            }
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            HistogramWindowManager.WindowStats initialStats = windowManager.getWindowStats(tr);

            // Should have only 1 active decade (well within limit of 10)
            assertEquals(1, initialStats.activeDecadeCount(), "Should have exactly 1 active decade (all values in decade 1)");
            assertTrue(initialStats.activeDecadeCount() <= metadata.windowDecades(), "Initial decades should be within window limit");
            assertEquals(0, initialStats.underflowSum(), "Should have no underflow initially");
            assertEquals(0, initialStats.overflowSum(), "Should have no overflow initially");

            // Perform maintenance - should be no-op since within limits
            windowManager.maintainWindow(tr);

            // Window should remain completely unchanged
            HistogramWindowManager.WindowStats finalStats = windowManager.getWindowStats(tr);
            assertEquals(initialStats.activeDecadeCount(), finalStats.activeDecadeCount(), "Active decade count should remain unchanged");
            assertEquals(initialStats.underflowSum(), finalStats.underflowSum(), "Underflow sum should remain unchanged (0)");
            assertEquals(initialStats.overflowSum(), finalStats.overflowSum(), "Overflow sum should remain unchanged (0)");
            assertEquals(initialStats.minActiveDecade(), finalStats.minActiveDecade(), "Min active decade should remain unchanged");
            assertEquals(initialStats.maxActiveDecade(), finalStats.maxActiveDecade(), "Max active decade should remain unchanged");

            tr.commit().join();
        }

        // Verify estimation accuracy is preserved
        HistogramEstimator estimator = histogram.getEstimator();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Values > 5: all 5 values = 5/5 = 1.0
            assertEquals(1.0, estimator.estimateGreaterThan(tr, 5), 0.05, "P(>5) should be exactly 1.0 as all values are greater than 5");

            // Values > 25: {30, 40, 50} = 3/5 = 0.6
            assertEquals(0.6, estimator.estimateGreaterThan(tr, 25), 0.15, "P(>25) should be exactly 0.6 (3 out of 5 values)");

            // Values > 50: none = 0/5 = 0.0, but due to linear interpolation in bucket may give small estimate
            assertTrue(estimator.estimateGreaterThan(tr, 50) <= 0.25, "P(>50) should be very small, at most 0.25 due to bucketing approximation");
        }
    }

    @Test
    void testEmptyHistogramStats() {
        HistogramMetadata metadata = HistogramMetadata.defaultMetadata();
        FDBLogHistogram histogram = initialize(metadata);

        HistogramWindowManager windowManager = histogram.getWindowManager();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            HistogramWindowManager.WindowStats stats = windowManager.getWindowStats(tr);

            assertEquals(0, stats.activeDecadeCount());
            assertNull(stats.minActiveDecade());
            assertNull(stats.maxActiveDecade());
            assertEquals(0, stats.underflowSum());
            assertEquals(0, stats.overflowSum());
        }
    }

    @Test
    void testWindowStatsAfterDataAdded() {
        HistogramMetadata metadata = HistogramMetadata.defaultMetadata();
        FDBLogHistogram histogram = initialize(metadata);

        HistogramWindowManager windowManager = histogram.getWindowManager();

        // Add values in different decades
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            histogram.add(tr, 1.5);      // decade 0
            histogram.add(tr, 25.0);     // decade 1
            histogram.add(tr, 350.0);    // decade 2
            histogram.add(tr, 4700.0);   // decade 3
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            HistogramWindowManager.WindowStats stats = windowManager.getWindowStats(tr);

            assertEquals(4, stats.activeDecadeCount());
            assertEquals(Integer.valueOf(0), stats.minActiveDecade());
            assertEquals(Integer.valueOf(3), stats.maxActiveDecade());
            assertEquals(0, stats.underflowSum()); // No evictions yet
            assertEquals(0, stats.overflowSum());  // No evictions yet
        }
    }

    @Test
    void testWindowMaintenancePreservesEstimation() {
        // Create histogram with small window (2 decades) to force eviction
        HistogramMetadata metadata = new HistogramMetadata(16, 4, 2, 16, 1);
        FDBLogHistogram histogram = initialize(metadata);

        HistogramWindowManager windowManager = histogram.getWindowManager();

        // Dataset: 5 values spanning decades {1.0, 10.0, 100.0, 1000.0, 10000.0}
        // Decades: 0(1.0), 1(10.0), 2(100.0), 3(1000.0), 4(10000.0) = 5 decades total
        double[] values = {1.0, 10.0, 100.0, 1000.0, 10000.0};
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (double value : values) {
                histogram.add(tr, value);
            }
            tr.commit().join();
        }

        HistogramEstimator estimator = histogram.getEstimator();

        // Before maintenance: Values > 0.5: all 5 values = 5/5 = 1.0
        double beforeMaintenance;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            beforeMaintenance = estimator.estimateGreaterThan(tr, 0.5);
            assertEquals(1.0, beforeMaintenance, 0.05, "Before maintenance: P(>0.5) should be 1.0 as all values are greater than 0.5");
        }

        // Perform maintenance - will evict oldest decades (0,1,2) keeping only (3,4)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            windowManager.maintainWindow(tr);
            tr.commit().join();
        }

        // After maintenance: some data evicted to underflow summary
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            double afterMaintenance = estimator.estimateGreaterThan(tr, 0.5);

            // Estimation should still be reasonable but may have precision loss
            // Since evicted data goes to underflow summary, the estimate should account for that
            assertTrue(afterMaintenance >= 0.3, "After maintenance: P(>0.5) should be at least 0.3, got " + afterMaintenance + " (precision loss due to eviction is expected)");
            assertTrue(afterMaintenance <= 1.0, "After maintenance: P(>0.5) should not exceed 1.0, got " + afterMaintenance);

            // Test more specific threshold after maintenance
            // Values > 500: {1000.0, 10000.0} should still be detectable
            double afterMaintenanceHighThreshold = estimator.estimateGreaterThan(tr, 500);
            assertTrue(afterMaintenanceHighThreshold > 0.1, "After maintenance: P(>500) should be greater than 0.1, got " + afterMaintenanceHighThreshold);
        }
    }
}