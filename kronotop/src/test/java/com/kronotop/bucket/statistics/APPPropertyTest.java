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

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property-based tests for APP histogram covering edge cases and metamorphic properties.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class APPPropertyTest extends BaseStandaloneInstanceTest {

    private FDBAdaptivePrefixHistogram histogram;
    private final String bucketName = "property_test";
    private final String fieldName = "test_field";

    @BeforeEach
    public void setUp() {
        histogram = new FDBAdaptivePrefixHistogram(instance.getContext().getFoundationDB());
    }

    @Test
    public void testEmptyHistogram() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        histogram.initialize(bucketName, fieldName, metadata);
        
        APPEstimator estimator = histogram.createEstimator(bucketName, fieldName);
        
        // Empty histogram should return 0 for all queries
        assertEquals(0, estimator.getTotalCount());
        assertEquals(0.0, estimator.estimateRange(new byte[]{(byte) 0}, new byte[]{(byte) 255}));
        assertEquals(0.0, estimator.estimateEquality(new byte[]{42}));
        assertEquals(0.0, estimator.estimateGreaterThan(new byte[]{(byte) 0}));
        assertEquals(0.0, estimator.estimateLessThan(new byte[]{(byte) 255}));
    }

    @Test
    public void testSingleKey() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        histogram.initialize(bucketName, fieldName, metadata);
        
        byte[] singleKey = new byte[]{42, 100, (byte) 200};
        histogram.add(bucketName, fieldName, singleKey);
        
        APPEstimator estimator = histogram.createEstimator(bucketName, fieldName);
        
        assertEquals(1, estimator.getTotalCount());
        
        // Range covering the key should return 1
        double estimate = estimator.estimateRange(new byte[]{0, 0, 0}, new byte[]{(byte) 255, (byte) 255, (byte) 255});
        assertTrue(estimate >= 0.8 && estimate <= 1.2, "Estimate should be close to 1, got: " + estimate);
        
        // Range not covering the key should return 0 or close to 0
        double estimateExcluding = estimator.estimateRange(new byte[]{0, 0, 0}, new byte[]{10, 10, 10});
        assertTrue(estimateExcluding <= 0.2, "Estimate excluding key should be close to 0, got: " + estimateExcluding);
    }

    @Test
    public void testHeavyDuplicates() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        histogram.initialize(bucketName, fieldName, metadata);
        
        byte[] duplicateKey = new byte[]{100, 100, 100};
        int duplicateCount = 100;
        
        // Add same key multiple times
        for (int i = 0; i < duplicateCount; i++) {
            histogram.add(bucketName, fieldName, duplicateKey);
        }
        
        APPEstimator estimator = histogram.createEstimator(bucketName, fieldName);
        
        assertEquals(duplicateCount, estimator.getTotalCount());
        
        // Range covering duplicates should return close to duplicateCount
        double estimate = estimator.estimateRange(new byte[]{50, 50, 50}, new byte[]{(byte) 150, (byte) 150, (byte) 150});
        assertTrue(estimate >= duplicateCount * 0.8 && estimate <= duplicateCount * 1.2,
                "Estimate should be close to " + duplicateCount + ", got: " + estimate);
    }

    @Test
    public void testExtremeKeys() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        histogram.initialize(bucketName, fieldName, metadata);
        
        // Add extreme keys
        byte[] minKey = new byte[]{(byte) 0, (byte) 0, (byte) 0};
        byte[] maxKey = new byte[]{(byte) 255, (byte) 255, (byte) 255};
        byte[] midKey = new byte[]{127, 127, 127};
        
        histogram.add(bucketName, fieldName, minKey);
        histogram.add(bucketName, fieldName, maxKey);  
        histogram.add(bucketName, fieldName, midKey);
        
        APPEstimator estimator = histogram.createEstimator(bucketName, fieldName);
        assertEquals(3, estimator.getTotalCount());
        
        // Each key should be findable
        assertNotNull(histogram.findLeaf(bucketName, fieldName, minKey));
        assertNotNull(histogram.findLeaf(bucketName, fieldName, maxKey));
        assertNotNull(histogram.findLeaf(bucketName, fieldName, midKey));
        
        // Range covering all should return 3
        double fullRangeEstimate = estimator.estimateRange(minKey, 
                APPKeySchema.addOne(maxKey)); // Need exclusive upper bound
        assertTrue(fullRangeEstimate >= 2.0 && fullRangeEstimate <= 4.0,
                "Full range estimate should be close to 3, got: " + fullRangeEstimate);
    }

    @Test
    public void testPaddingInvariance() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        histogram.initialize(bucketName, fieldName, metadata);
        
        // Add keys of different lengths
        byte[] shortKey = new byte[]{100};
        byte[] mediumKey = new byte[]{100, 50};
        byte[] longKey = new byte[]{100, 50, 25};
        
        histogram.add(bucketName, fieldName, shortKey);
        histogram.add(bucketName, fieldName, mediumKey);
        histogram.add(bucketName, fieldName, longKey);
        
        APPEstimator estimator = histogram.createEstimator(bucketName, fieldName);
        assertEquals(3, estimator.getTotalCount());
        
        // All keys should be findable regardless of original length
        assertNotNull(histogram.findLeaf(bucketName, fieldName, shortKey));
        assertNotNull(histogram.findLeaf(bucketName, fieldName, mediumKey));
        assertNotNull(histogram.findLeaf(bucketName, fieldName, longKey));
        
        // Padded versions should behave consistently
        byte[] shortKeyPadded = APPKeySchema.canonicalizeLowerBound(shortKey, metadata.maxDepth());
        byte[] mediumKeyPadded = APPKeySchema.canonicalizeLowerBound(mediumKey, metadata.maxDepth());
        
        APPLeaf leafShort = histogram.findLeaf(bucketName, fieldName, shortKey);
        APPLeaf leafShortPadded = histogram.findLeaf(bucketName, fieldName, shortKeyPadded);
        
        assertEquals(leafShort, leafShortPadded, "Padded and unpadded keys should find same leaf");
    }

    @Test
    public void testAdditivityProperty() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        histogram.initialize(bucketName, fieldName, metadata);
        
        // Add data across a range
        Random rng = new Random(777);
        for (int i = 0; i < 100; i++) {
            byte[] key = new byte[3];
            key[0] = (byte) rng.nextInt(128); // First byte 0-127
            key[1] = (byte) rng.nextInt(256);
            key[2] = (byte) rng.nextInt(256);
            histogram.add(bucketName, fieldName, key);
        }
        
        APPEstimator estimator = histogram.createEstimator(bucketName, fieldName);
        
        // Test additivity: est(A,B) ≈ est(A,M) + est(M,B) for M in (A,B)
        byte[] rangeA = new byte[]{0, 0, 0};
        byte[] rangeM = new byte[]{64, 0, 0};
        byte[] rangeB = new byte[]{127, (byte) 255, (byte) 255};
        
        double estAB = estimator.estimateRange(rangeA, rangeB);
        double estAM = estimator.estimateRange(rangeA, rangeM);
        double estMB = estimator.estimateRange(rangeM, rangeB);
        
        double sumParts = estAM + estMB;
        double relativeDiff = Math.abs(estAB - sumParts) / Math.max(1.0, estAB);
        
        assertTrue(relativeDiff <= 0.3, 
                String.format("Additivity violated: est(A,B)=%.2f vs est(A,M)+est(M,B)=%.2f, diff=%.2f", 
                        estAB, sumParts, relativeDiff));
    }

    @Test
    public void testLinearityProperty() {
        APPHistogramMetadata metadata1 = APPHistogramMetadata.defaultMetadata();
        histogram.initialize(bucketName, fieldName, metadata1);
        
        // Create another histogram with same structure  
        String bucketName2 = bucketName + "_2";
        APPHistogramMetadata metadata2 = APPHistogramMetadata.defaultMetadata();
        histogram.initialize(bucketName2, fieldName, metadata2);
        
        // Add same data to both histograms (simulating duplication)
        Random rng = new Random(888);
        List<byte[]> keys = new ArrayList<>();
        
        for (int i = 0; i < 50; i++) {
            byte[] key = new byte[2];
            rng.nextBytes(key);
            // Make sure bytes are non-negative for simplicity
            key[0] = (byte) (key[0] & 0x7F);
            key[1] = (byte) (key[1] & 0x7F);
            
            keys.add(key);
            histogram.add(bucketName, fieldName, key);
            histogram.add(bucketName2, fieldName, key); // Same data in second histogram
        }
        
        APPEstimator estimator1 = histogram.createEstimator(bucketName, fieldName);
        APPEstimator estimator2 = histogram.createEstimator(bucketName2, fieldName);
        
        // Both histograms should have same total count
        assertEquals(estimator1.getTotalCount(), estimator2.getTotalCount());
        
        // Range estimates should be similar (within tolerance due to different leaf structures)
        byte[] queryLower = new byte[]{10, 10};
        byte[] queryUpper = new byte[]{50, 50};
        
        double est1 = estimator1.estimateRange(queryLower, queryUpper);
        double est2 = estimator2.estimateRange(queryLower, queryUpper);
        
        double relativeDiff = Math.abs(est1 - est2) / Math.max(1.0, Math.max(est1, est2));
        assertTrue(relativeDiff <= 0.5, 
                "Linear histograms should give similar estimates: " + est1 + " vs " + est2);
    }

    @Test
    public void testUpdateSymmetry() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        histogram.initialize(bucketName, fieldName, metadata);
        
        byte[] key1 = new byte[]{10, 20, 30};
        byte[] key2 = new byte[]{40, 50, 60};
        
        // Add key1
        histogram.add(bucketName, fieldName, key1);
        APPEstimator estimator = histogram.createEstimator(bucketName, fieldName);
        long countAfterAdd = estimator.getTotalCount();
        assertEquals(1, countAfterAdd);
        
        // Update key1 -> key2 (should be same as delete key1 + add key2)
        try (var tr = histogram.getDatabase().createTransaction()) {
            histogram.updateValue(tr, bucketName, fieldName, key1, key2, metadata);
            tr.commit().join();
        }
        
        long countAfterUpdate = estimator.getTotalCount();
        assertEquals(1, countAfterUpdate, "Update should preserve total count");
        
        // key1 should no longer be strongly represented, key2 should be
        assertNotNull(histogram.findLeaf(bucketName, fieldName, key2));
        
        // Update back: key2 -> key1  
        try (var tr = histogram.getDatabase().createTransaction()) {
            histogram.updateValue(tr, bucketName, fieldName, key2, key1, metadata);
            tr.commit().join();
        }
        
        long countAfterReverseUpdate = estimator.getTotalCount();
        assertEquals(1, countAfterReverseUpdate, "Reverse update should preserve total count");
    }

    @Test
    public void testStressWithMaintenance() {
        APPHistogramMetadata metadata = new APPHistogramMetadata(3, 4, 10, 5, 1); // Low thresholds
        histogram.initialize(bucketName, fieldName, metadata);
        
        Random rng = new Random(999);
        Set<String> addedKeys = new HashSet<>();
        
        // Add many keys to trigger multiple splits/merges
        for (int i = 0; i < 200; i++) {
            byte[] key = new byte[2];
            rng.nextBytes(key);
            key[0] = (byte) (key[0] & 0x7F); // Keep non-negative
            key[1] = (byte) (key[1] & 0x7F);
            
            String keyStr = Arrays.toString(key);
            if (!addedKeys.contains(keyStr)) {
                histogram.add(bucketName, fieldName, key);
                addedKeys.add(keyStr);
                
                // Periodically trigger maintenance
                if (i % 20 == 0) {
                    histogram.performMaintenance(bucketName, fieldName);
                }
            }
        }
        
        // Final maintenance pass
        histogram.performMaintenance(bucketName, fieldName);
        
        APPEstimator estimator = histogram.createEstimator(bucketName, fieldName);
        long finalCount = estimator.getTotalCount();
        
        assertEquals(addedKeys.size(), finalCount, "Mass conservation failed under stress");
        
        // All keys should still be findable
        for (String keyStr : addedKeys) {
            byte[] key = parseKeyFromString(keyStr);
            assertNotNull(histogram.findLeaf(bucketName, fieldName, key), 
                    "Key not found after stress test: " + keyStr);
        }
        
        // Estimates should be reasonable
        double totalEstimate = estimator.estimateRange(new byte[]{0, 0}, new byte[]{127, 127});
        double relativeError = Math.abs(totalEstimate - finalCount) / Math.max(1.0, finalCount);
        assertTrue(relativeError <= 0.5, 
                "Total range estimate too far off: " + totalEstimate + " vs " + finalCount);
    }

    // Helper method to parse byte array from string representation
    private byte[] parseKeyFromString(String keyStr) {
        // Parse "[1, 2]" format
        keyStr = keyStr.substring(1, keyStr.length() - 1); // Remove brackets
        String[] parts = keyStr.split(", ");
        byte[] key = new byte[parts.length];
        for (int i = 0; i < parts.length; i++) {
            key[i] = (byte) Integer.parseInt(parts[i]);
        }
        return key;
    }
}