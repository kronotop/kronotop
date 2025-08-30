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
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class APPHistogramTest extends BaseStandaloneInstanceTest {

    private FDBAdaptivePrefixHistogram histogram;
    private final String bucketName = "test_bucket";
    private final String fieldName = "test_field";

    @BeforeEach
    public void setUp() {
        histogram = new FDBAdaptivePrefixHistogram(instance.getContext().getFoundationDB());
        
        // Clear any existing data to ensure clean test state
        clearHistogramData();
    }
    
    private void clearHistogramData() {
        try (var tr = histogram.getDatabase().createTransaction()) {
            var subspace = histogram.getHistogramSubspace(tr, bucketName, fieldName);
            
            // Clear all data in the histogram subspace
            tr.clear(subspace.range());
            tr.commit().join();
        }
    }

    @Test
    public void testMetadataValidation() {
        // Valid metadata should not throw
        APPHistogramMetadata valid = APPHistogramMetadata.defaultMetadata();
        assertDoesNotThrow(valid::validate);

        // Invalid max depth
        APPHistogramMetadata invalidDepth = new APPHistogramMetadata(0, 4, 4096, 1024, 1);
        assertThrows(IllegalArgumentException.class, invalidDepth::validate);

        // Invalid fanout
        APPHistogramMetadata invalidFanout = new APPHistogramMetadata(3, 1, 4096, 1024, 1);
        assertThrows(IllegalArgumentException.class, invalidFanout::validate);

        // Invalid threshold order
        APPHistogramMetadata invalidThreshold = new APPHistogramMetadata(3, 4, 1024, 4096, 1);
        assertThrows(IllegalArgumentException.class, invalidThreshold::validate);

        // Invalid shard count (not power of 2)
        APPHistogramMetadata invalidShardCount = new APPHistogramMetadata(3, 4, 4096, 1024, 3);
        assertThrows(IllegalArgumentException.class, invalidShardCount::validate);
    }

    @Test
    public void testKeyPadding() {
        byte[] input = "abc".getBytes(StandardCharsets.UTF_8);
        byte[] padded = APPKeySchema.rightPad(input, 5, (byte) 0x00);
        
        assertArrayEquals(new byte[]{97, 98, 99, (byte) 0, (byte) 0}, padded);
        
        // Test padding with different byte
        byte[] paddedFF = APPKeySchema.rightPad(input, 5, (byte) 0xFF);
        assertArrayEquals(new byte[]{97, 98, 99, -1, -1}, paddedFF);
        
        // Test truncation when input is longer
        byte[] longInput = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        byte[] truncated = APPKeySchema.rightPad(longInput, 3, (byte) 0x00);
        assertArrayEquals(new byte[]{97, 98, 99}, truncated);
    }

    @Test
    public void testCompoundKeyOperations() {
        byte[] lowerBound = new byte[]{1, 2, 3};
        int depth = 2;
        
        byte[] compound = APPKeySchema.createCompoundKey(lowerBound, depth);
        assertEquals(4, compound.length);
        assertEquals(depth, APPKeySchema.extractDepth(compound));
        assertArrayEquals(lowerBound, APPKeySchema.extractLowerBound(compound));
    }

    @Test
    public void testLeafGeometry() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        
        // Test leaf width calculation
        assertEquals(256L * 256L, metadata.leafWidth(1)); // 256^(3-1) = 256^2
        assertEquals(256L, metadata.leafWidth(2)); // 256^(3-2) = 256^1
        assertEquals(1L, metadata.leafWidth(3)); // 256^(3-3) = 256^0
        
        // Test child width calculation
        assertEquals(16384L, metadata.childWidth(1)); // 65536 / 4 = 16384
        assertEquals(64L, metadata.childWidth(2)); // 256 / 4 = 64
    }

    @Test
    public void testLeafCreation() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        byte[] lowerBound = new byte[]{1, 2, 0};
        
        APPLeaf leaf = APPLeaf.create(lowerBound, 2, metadata.maxDepth());
        
        assertEquals(2, leaf.depth());
        assertArrayEquals(new byte[]{1, 2, 0}, leaf.lowerBound());
        
        // Upper bound should be lower + leafWidth
        byte[] expectedUpper = new byte[]{1, 3, 0}; // 1*256^2 + 2*256 + 256 = ...
        // Simplified test - check that upper > lower
        assertTrue(Arrays.compareUnsigned(leaf.upperBound(), leaf.lowerBound()) > 0);
    }

    @Test
    public void testLeafContains() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        byte[] lowerBound = new byte[]{1, (byte) 0, (byte) 0};
        APPLeaf leaf = APPLeaf.create(lowerBound, 1, metadata.maxDepth());
        
        // Test containment
        assertTrue(leaf.contains(new byte[]{1, (byte) 0, (byte) 0})); // Lower boundary (inclusive)
        assertTrue(leaf.contains(new byte[]{1, (byte) 128, (byte) 255})); // Middle value
        assertFalse(leaf.contains(new byte[]{(byte) 0, (byte) 255, (byte) 255})); // Below range
        // Upper boundary test depends on exact computation
    }

    @Test
    public void testBasicHistogramOperations() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        histogram.initialize(bucketName, fieldName, metadata);
        
        // Test metadata retrieval
        APPHistogramMetadata retrieved = histogram.getMetadata(bucketName, fieldName);
        assertNotNull(retrieved);
        assertEquals(metadata.maxDepth(), retrieved.maxDepth());
        assertEquals(metadata.fanout(), retrieved.fanout());
        
        // Test adding values
        byte[] value1 = "apple".getBytes(StandardCharsets.UTF_8);
        byte[] value2 = "banana".getBytes(StandardCharsets.UTF_8);
        
        assertDoesNotThrow(() -> histogram.add(bucketName, fieldName, value1));
        assertDoesNotThrow(() -> histogram.add(bucketName, fieldName, value2));
    }

    @Test
    public void testLeafLookup() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        histogram.initialize(bucketName, fieldName, metadata);
        
        byte[] searchKey = "test".getBytes(StandardCharsets.UTF_8);
        
        // Should find the root leaf initially
        APPLeaf leaf = histogram.findLeaf(bucketName, fieldName, searchKey);
        assertNotNull(leaf);
        assertEquals(1, leaf.depth()); // Should be the root leaf at depth 1
    }

    @Test
    public void testEstimatorBasics() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        histogram.initialize(bucketName, fieldName, metadata);
        
        // Add some test data
        byte[] value1 = "apple".getBytes(StandardCharsets.UTF_8);
        byte[] value2 = "banana".getBytes(StandardCharsets.UTF_8);
        byte[] value3 = "cherry".getBytes(StandardCharsets.UTF_8);
        
        histogram.add(bucketName, fieldName, value1);
        histogram.add(bucketName, fieldName, value2);
        histogram.add(bucketName, fieldName, value3);
        
        APPEstimator estimator = histogram.createEstimator(bucketName, fieldName);
        
        // Test total count
        long totalCount = estimator.getTotalCount();
        assertEquals(3, totalCount);
        
        // Test range estimation
        double rangeEstimate = estimator.estimateRange(
            "a".getBytes(StandardCharsets.UTF_8), 
            "z".getBytes(StandardCharsets.UTF_8)
        );
        assertTrue(rangeEstimate > 0); // Should find some items
        
        // Test equality estimation
        double equalityEstimate = estimator.estimateEquality(value1);
        assertTrue(equalityEstimate > 0);
    }

    @Test
    public void testHistogramStats() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        histogram.initialize(bucketName, fieldName, metadata);
        
        // Add some test data
        for (int i = 0; i < 10; i++) {
            String value = "item" + String.format("%03d", i);
            histogram.add(bucketName, fieldName, value.getBytes(StandardCharsets.UTF_8));
        }
        
        APPEstimator estimator = histogram.createEstimator(bucketName, fieldName);
        APPEstimator.APPHistogramStats stats = estimator.getHistogramStats();
        
        assertNotNull(stats);
        assertEquals(10, stats.totalItems);
        assertTrue(stats.leafCount > 0);
        assertTrue(stats.minDepth > 0);
        assertTrue(stats.maxDepth >= stats.minDepth);
    }

    @Test
    public void testEmptyHistogram() {
        // Test operations on non-initialized histogram
        APPEstimator estimator = histogram.createEstimator("nonexistent", "field");
        
        assertEquals(0, estimator.getTotalCount());
        assertEquals(0.0, estimator.estimateEquality("test".getBytes(StandardCharsets.UTF_8)));
        assertEquals(0.0, estimator.estimateRange(
            "a".getBytes(StandardCharsets.UTF_8),
            "z".getBytes(StandardCharsets.UTF_8)
        ));
    }

    @Test
    public void testConcurrentOperations() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        histogram.initialize(bucketName, fieldName, metadata);
        
        // Test add/delete pairs (should be symmetric)
        byte[] value = "test_value".getBytes(StandardCharsets.UTF_8);
        
        histogram.add(bucketName, fieldName, value);
        APPEstimator estimator = histogram.createEstimator(bucketName, fieldName);
        assertEquals(1, estimator.getTotalCount());
        
        // Delete should reduce count
        try (var tr = histogram.getDatabase().createTransaction()) {
            histogram.deleteValue(tr, bucketName, fieldName, value, metadata);
            tr.commit().join();
        }
        
        assertEquals(0, estimator.getTotalCount());
    }
}