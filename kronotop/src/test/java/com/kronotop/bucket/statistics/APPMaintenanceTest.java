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
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class APPMaintenanceTest extends BaseStandaloneInstanceTest {

    private FDBAdaptivePrefixHistogram histogram;
    private final String bucketName = "maintenance_test_bucket";
    private final String fieldName = "maintenance_test_field";

    @BeforeEach
    public void setUp() {
        histogram = new FDBAdaptivePrefixHistogram(instance.getContext().getFoundationDB());
    }

    @Test
    public void testSplitThreshold() {
        // Create metadata with low split threshold for testing
        APPHistogramMetadata metadata = new APPHistogramMetadata(
                3,     // maxDepth
                4,     // fanout
                5,     // splitThreshold (low for easy testing)
                2,     // mergeThreshold
                1      // shardCount
        );
        
        histogram.initialize(bucketName, fieldName, metadata);
        
        // Add enough items to trigger split threshold
        for (int i = 0; i < 10; i++) {
            String value = "item" + String.format("%03d", i);
            histogram.add(bucketName, fieldName, value.getBytes(StandardCharsets.UTF_8));
        }
        
        APPEstimator estimator = histogram.createEstimator(bucketName, fieldName);
        assertEquals(10, estimator.getTotalCount());
        
        // Perform maintenance - this should trigger splits
        histogram.performMaintenance(bucketName, fieldName);
        
        // After maintenance, total count should be preserved
        assertEquals(10, estimator.getTotalCount());
        
        // Check if histogram structure changed (more leaves at deeper levels)
        APPEstimator.APPHistogramStats stats = estimator.getHistogramStats();
        assertNotNull(stats);
        assertTrue(stats.leafCount >= 1); // Should have at least one leaf
    }

    @Test
    public void testMergeThreshold() {
        // Create metadata with merge-friendly thresholds
        APPHistogramMetadata metadata = new APPHistogramMetadata(
                3,     // maxDepth  
                4,     // fanout
                100,   // splitThreshold (high to prevent splits)
                10,    // mergeThreshold (high to enable merges)
                1      // shardCount
        );
        
        histogram.initialize(bucketName, fieldName, metadata);
        
        // Add a few items
        for (int i = 0; i < 5; i++) {
            String value = "item" + i;
            histogram.add(bucketName, fieldName, value.getBytes(StandardCharsets.UTF_8));
        }
        
        APPEstimator estimator = histogram.createEstimator(bucketName, fieldName);
        long initialCount = estimator.getTotalCount();
        assertEquals(5, initialCount);
        
        // Remove most items to trigger merge conditions
        try (var tr = histogram.getDatabase().createTransaction()) {
            for (int i = 0; i < 3; i++) {
                String value = "item" + i;
                histogram.deleteValue(tr, bucketName, fieldName, value.getBytes(StandardCharsets.UTF_8), metadata);
            }
            tr.commit().join();
        }
        
        // Perform maintenance
        histogram.performMaintenance(bucketName, fieldName);
        
        // Count should be reduced
        long finalCount = estimator.getTotalCount();
        assertEquals(2, finalCount);
    }

    @Test
    public void testMaintenanceOperationsClass() {
        APPMaintenanceOperations maintenance = new APPMaintenanceOperations();
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        
        // Test split attempt on non-existent data should not crash
        try (var tr = histogram.getDatabase().createTransaction()) {
            var subspace = histogram.getHistogramSubspace(tr, bucketName, fieldName);
            APPLeaf dummyLeaf = APPLeaf.create(new byte[]{1, 2, 3}, 2, 3);
            
            // Should return false for non-existent leaf
            boolean result = maintenance.attemptSplit(tr, subspace, dummyLeaf, metadata);
            assertFalse(result);
        }
    }

    @Test
    public void testLeafGeometryOperations() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        byte[] rootLowerBound = new byte[]{(byte) 0, (byte) 0, (byte) 0};
        
        APPLeaf rootLeaf = APPLeaf.create(rootLowerBound, 1, metadata.maxDepth());
        
        // Test child generation
        APPLeaf[] children = rootLeaf.getChildren(metadata);
        assertEquals(metadata.fanout(), children.length);
        
        // All children should be at depth 2
        for (APPLeaf child : children) {
            assertEquals(2, child.depth());
        }
        
        // Children should partition the parent's space
        // First child should start at parent's lower bound
        assertArrayEquals(rootLeaf.lowerBound(), children[0].lowerBound());
        
        // Children should be in ascending order
        for (int i = 1; i < children.length; i++) {
            assertTrue(java.util.Arrays.compareUnsigned(
                children[i-1].lowerBound(), 
                children[i].lowerBound()
            ) < 0);
        }
    }

    @Test
    public void testSiblingOperations() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        byte[] rootLowerBound = new byte[]{(byte) 0, (byte) 0, (byte) 0};
        
        APPLeaf rootLeaf = APPLeaf.create(rootLowerBound, 1, metadata.maxDepth());
        APPLeaf[] children = rootLeaf.getChildren(metadata);
        
        // Test parent-child relationships
        for (APPLeaf child : children) {
            APPLeaf parent = child.getParent(metadata);
            assertEquals(rootLeaf.depth(), parent.depth());
            assertArrayEquals(rootLeaf.lowerBound(), parent.lowerBound());
        }
        
        // Test sibling relationships
        APPLeaf[] siblings = children[0].getSiblings(metadata);
        assertEquals(metadata.fanout(), siblings.length);
        
        // All siblings should be at the same depth
        for (APPLeaf sibling : siblings) {
            assertEquals(children[0].depth(), sibling.depth());
        }
    }

    @Test 
    public void testMaintenanceFlags() {
        // Test flag encoding/decoding
        int flags = APPKeySchema.FLAG_NEEDS_SPLIT | APPKeySchema.FLAG_NEEDS_MERGE;
        byte[] encoded = APPKeySchema.encodeFlags(flags);
        int decoded = APPKeySchema.decodeFlags(encoded);
        
        assertEquals(flags, decoded);
        assertTrue((decoded & APPKeySchema.FLAG_NEEDS_SPLIT) != 0);
        assertTrue((decoded & APPKeySchema.FLAG_NEEDS_MERGE) != 0);
        
        // Test individual flags
        int splitOnly = APPKeySchema.FLAG_NEEDS_SPLIT;
        encoded = APPKeySchema.encodeFlags(splitOnly);
        decoded = APPKeySchema.decodeFlags(encoded);
        
        assertEquals(splitOnly, decoded);
        assertTrue((decoded & APPKeySchema.FLAG_NEEDS_SPLIT) != 0);
        assertFalse((decoded & APPKeySchema.FLAG_NEEDS_MERGE) != 0);
    }

    @Test
    public void testCoverageRatioCalculation() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        
        // Create a leaf covering range [1,0,0] to [2,0,0)
        byte[] lowerBound = new byte[]{1, (byte) 0, (byte) 0};
        APPLeaf leaf = APPLeaf.create(lowerBound, 1, metadata.maxDepth());
        
        // Test full coverage
        double fullCoverage = leaf.computeCoverageRatio(
            new byte[]{1, (byte) 0, (byte) 0},  // Start of leaf
            new byte[]{2, (byte) 0, (byte) 0}   // End of leaf
        );
        assertEquals(1.0, fullCoverage, 0.001);
        
        // Test no coverage (range completely outside)
        double noCoverage = leaf.computeCoverageRatio(
            new byte[]{(byte) 0, (byte) 0, (byte) 0},  // Before leaf
            new byte[]{(byte) 0, (byte) 255, (byte) 255}  // Still before leaf
        );
        assertEquals(0.0, noCoverage, 0.001);
        
        // Test partial coverage should be between 0 and 1
        double partialCoverage = leaf.computeCoverageRatio(
            new byte[]{1, (byte) 128, (byte) 0},  // Middle of leaf
            new byte[]{1, (byte) 192, (byte) 0}   // Still within leaf
        );
        assertTrue(partialCoverage > 0.0 && partialCoverage < 1.0);
    }
}