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

import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class HistogramKeySchemaTest extends BaseStandaloneInstanceTest {
    
    private DirectorySubspace testSubspace;
    
    @BeforeEach
    void setUp() {
        try {
            testSubspace = DirectoryLayer.getDefault()
                    .createOrOpen(instance.getContext().getFoundationDB(), Arrays.asList("test", "histogram"))
                    .join();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test subspace", e);
        }
    }
    
    @Test
    void testCounterValueEncoding() {
        // Test encoding and decoding of counter values
        long originalValue = 42L;
        byte[] encoded = HistogramKeySchema.encodeCounterValue(originalValue);
        long decoded = HistogramKeySchema.decodeCounterValue(encoded);
        
        assertEquals(originalValue, decoded);
        
        // Test with large values
        originalValue = Long.MAX_VALUE;
        encoded = HistogramKeySchema.encodeCounterValue(originalValue);
        decoded = HistogramKeySchema.decodeCounterValue(encoded);
        assertEquals(originalValue, decoded);
        
        // Test with zero
        originalValue = 0L;
        encoded = HistogramKeySchema.encodeCounterValue(originalValue);
        decoded = HistogramKeySchema.decodeCounterValue(encoded);
        assertEquals(originalValue, decoded);
        
        // Test with negative values
        originalValue = -123L;
        encoded = HistogramKeySchema.encodeCounterValue(originalValue);
        decoded = HistogramKeySchema.decodeCounterValue(encoded);
        assertEquals(originalValue, decoded);
    }
    
    @Test
    void testCounterValueDecodingNullAndInvalidData() {
        // Test null data
        assertEquals(0L, HistogramKeySchema.decodeCounterValue(null));
        
        // Test invalid length data
        assertEquals(0L, HistogramKeySchema.decodeCounterValue(new byte[4]));
        assertEquals(0L, HistogramKeySchema.decodeCounterValue(new byte[0]));
    }
    
    @Test
    void testMetadataEncoding() {
        HistogramMetadata originalMetadata = new HistogramMetadata(16, 4, 8, 16, 1);
        
        byte[] encoded = HistogramKeySchema.encodeMetadata(originalMetadata);
        HistogramMetadata decoded = HistogramKeySchema.decodeMetadata(encoded);
        
        assertEquals(originalMetadata, decoded);
    }
    
    @Test
    void testMetadataDecodingNull() {
        assertNull(HistogramKeySchema.decodeMetadata(null));
    }
    
    @Test
    void testBucketCountKey() {
        byte[] key = HistogramKeySchema.bucketCountKey(testSubspace, 2, 5);
        assertNotNull(key);
        
        // Different parameters should produce different keys
        byte[] differentKey = HistogramKeySchema.bucketCountKey(testSubspace, 2, 6);
        assertFalse(Arrays.equals(key, differentKey));
        
        byte[] differentDecadeKey = HistogramKeySchema.bucketCountKey(testSubspace, 3, 5);
        assertFalse(Arrays.equals(key, differentDecadeKey));
    }
    
    @Test
    void testDecadeSumKey() {
        byte[] key = HistogramKeySchema.decadeSumKey(testSubspace, 2);
        assertNotNull(key);
        
        // Different decade should produce different key
        byte[] differentKey = HistogramKeySchema.decadeSumKey(testSubspace, 3);
        assertFalse(Arrays.equals(key, differentKey));
    }
    
    @Test
    void testGroupSumKey() {
        byte[] key = HistogramKeySchema.groupSumKey(testSubspace, 2, 1);
        assertNotNull(key);
        
        // Different parameters should produce different keys
        byte[] differentGroupKey = HistogramKeySchema.groupSumKey(testSubspace, 2, 2);
        assertFalse(Arrays.equals(key, differentGroupKey));
        
        byte[] differentDecadeKey = HistogramKeySchema.groupSumKey(testSubspace, 3, 1);
        assertFalse(Arrays.equals(key, differentDecadeKey));
    }
    
    @Test
    void testTotalShardKey() {
        byte[] key = HistogramKeySchema.totalShardKey(testSubspace, 5);
        assertNotNull(key);
        
        // Different shard should produce different key
        byte[] differentKey = HistogramKeySchema.totalShardKey(testSubspace, 6);
        assertFalse(Arrays.equals(key, differentKey));
    }
    
    @Test
    void testSummaryKeys() {
        byte[] underflowKey = HistogramKeySchema.underflowSumKey(testSubspace);
        byte[] overflowKey = HistogramKeySchema.overflowSumKey(testSubspace);
        byte[] zeroOrNegKey = HistogramKeySchema.zeroOrNegKey(testSubspace);
        byte[] metadataKey = HistogramKeySchema.metadataKey(testSubspace);
        
        // All keys should be different
        assertFalse(Arrays.equals(underflowKey, overflowKey));
        assertFalse(Arrays.equals(underflowKey, zeroOrNegKey));
        assertFalse(Arrays.equals(underflowKey, metadataKey));
        assertFalse(Arrays.equals(overflowKey, zeroOrNegKey));
        assertFalse(Arrays.equals(overflowKey, metadataKey));
        assertFalse(Arrays.equals(zeroOrNegKey, metadataKey));
    }
    
    @Test
    void testRangeKeys() {
        // Test decade sum range
        byte[] beginKey = HistogramKeySchema.decadeSumRangeBegin(testSubspace, 2);
        byte[] endKey = HistogramKeySchema.decadeSumRangeEnd(testSubspace, 5);
        
        // Begin should be less than end (lexicographic order)
        assertTrue(compareByteArrays(beginKey, endKey) < 0);
        
        // Test group sum range
        byte[] groupBeginKey = HistogramKeySchema.groupSumRangeBegin(testSubspace, 2, 1);
        byte[] groupEndKey = HistogramKeySchema.groupSumRangeEnd(testSubspace, 2, 3);
        
        assertTrue(compareByteArrays(groupBeginKey, groupEndKey) < 0);
        
        // Test decade range
        byte[] decadeBeginKey = HistogramKeySchema.decadeRangeBegin(testSubspace, 2);
        byte[] decadeEndKey = HistogramKeySchema.decadeRangeEnd(testSubspace, 2);
        
        assertTrue(compareByteArrays(decadeBeginKey, decadeEndKey) < 0);
    }
    
    @Test
    void testONE_LE_Constant() {
        // Test that the constant is correctly encoded
        long decoded = HistogramKeySchema.decodeCounterValue(HistogramKeySchema.ONE_LE);
        assertEquals(1L, decoded);
    }
    
    /**
     * Utility method to compare byte arrays lexicographically
     */
    private int compareByteArrays(byte[] a, byte[] b) {
        int minLength = Math.min(a.length, b.length);
        for (int i = 0; i < minLength; i++) {
            int result = Byte.compareUnsigned(a[i], b[i]);
            if (result != 0) {
                return result;
            }
        }
        return Integer.compare(a.length, b.length);
    }
}