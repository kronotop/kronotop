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

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;


class HistogramKeySchemaTest extends BaseStandaloneInstanceTest {

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
        DirectorySubspace testSubspace = createOrOpenSubspaceUnderCluster(UUID.randomUUID().toString());

        String histType = HistogramKeySchema.POS_HIST_PREFIX;
        byte[] key = HistogramKeySchema.bucketCountKey(testSubspace, histType, 2, 5);
        assertNotNull(key);

        // Different parameters should produce different keys
        byte[] differentKey = HistogramKeySchema.bucketCountKey(testSubspace, histType, 2, 6);
        assertFalse(Arrays.equals(key, differentKey));

        byte[] differentDecadeKey = HistogramKeySchema.bucketCountKey(testSubspace, histType, 3, 5);
        assertFalse(Arrays.equals(key, differentDecadeKey));

        // Different histogram type should produce different key
        byte[] differentHistTypeKey = HistogramKeySchema.bucketCountKey(testSubspace, HistogramKeySchema.NEG_HIST_PREFIX, 2, 5);
        assertFalse(Arrays.equals(key, differentHistTypeKey));
    }

    @Test
    void testDecadeSumKey() {
        DirectorySubspace testSubspace = createOrOpenSubspaceUnderCluster(UUID.randomUUID().toString());

        String histType = HistogramKeySchema.POS_HIST_PREFIX;
        byte[] key = HistogramKeySchema.decadeSumKey(testSubspace, histType, 2);
        assertNotNull(key);

        // Different decade should produce different key
        byte[] differentKey = HistogramKeySchema.decadeSumKey(testSubspace, histType, 3);
        assertFalse(Arrays.equals(key, differentKey));

        // Different histogram type should produce different key
        byte[] differentHistTypeKey = HistogramKeySchema.decadeSumKey(testSubspace, HistogramKeySchema.NEG_HIST_PREFIX, 2);
        assertFalse(Arrays.equals(key, differentHistTypeKey));
    }

    @Test
    void testGroupSumKey() {
        DirectorySubspace testSubspace = createOrOpenSubspaceUnderCluster(UUID.randomUUID().toString());

        String histType = HistogramKeySchema.POS_HIST_PREFIX;
        byte[] key = HistogramKeySchema.groupSumKey(testSubspace, histType, 2, 1);
        assertNotNull(key);

        // Different parameters should produce different keys
        byte[] differentGroupKey = HistogramKeySchema.groupSumKey(testSubspace, histType, 2, 2);
        assertFalse(Arrays.equals(key, differentGroupKey));

        byte[] differentDecadeKey = HistogramKeySchema.groupSumKey(testSubspace, histType, 3, 1);
        assertFalse(Arrays.equals(key, differentDecadeKey));

        // Different histogram type should produce different key
        byte[] differentHistTypeKey = HistogramKeySchema.groupSumKey(testSubspace, HistogramKeySchema.NEG_HIST_PREFIX, 2, 1);
        assertFalse(Arrays.equals(key, differentHistTypeKey));
    }

    @Test
    void testTotalShardKey() {
        DirectorySubspace testSubspace = createOrOpenSubspaceUnderCluster(UUID.randomUUID().toString());

        byte[] key = HistogramKeySchema.totalShardKey(testSubspace, "pos", 5);
        assertNotNull(key);

        // Different shard should produce different key
        byte[] differentKey = HistogramKeySchema.totalShardKey(testSubspace, "pos", 6);
        assertFalse(Arrays.equals(key, differentKey));

        // Different histogram type should produce different key
        byte[] differentHistTypeKey = HistogramKeySchema.totalShardKey(testSubspace, "neg", 5);
        assertFalse(Arrays.equals(key, differentHistTypeKey));
    }

    @Test
    void testSummaryKeys() {
        DirectorySubspace testSubspace = createOrOpenSubspaceUnderCluster(UUID.randomUUID().toString());

        String posHistType = HistogramKeySchema.POS_HIST_PREFIX;
        String negHistType = HistogramKeySchema.NEG_HIST_PREFIX;

        byte[] posUnderflowKey = HistogramKeySchema.underflowSumKey(testSubspace, posHistType);
        byte[] posOverflowKey = HistogramKeySchema.overflowSumKey(testSubspace, posHistType);
        byte[] negUnderflowKey = HistogramKeySchema.underflowSumKey(testSubspace, negHistType);
        byte[] negOverflowKey = HistogramKeySchema.overflowSumKey(testSubspace, negHistType);
        byte[] zeroCountKey = HistogramKeySchema.zeroCountKey(testSubspace);
        byte[] metadataKey = HistogramKeySchema.metadataKey(testSubspace);

        // All keys should be different
        assertFalse(Arrays.equals(posUnderflowKey, posOverflowKey));
        assertFalse(Arrays.equals(posUnderflowKey, negUnderflowKey));
        assertFalse(Arrays.equals(posUnderflowKey, zeroCountKey));
        assertFalse(Arrays.equals(posUnderflowKey, metadataKey));
        assertFalse(Arrays.equals(posOverflowKey, negOverflowKey));
        assertFalse(Arrays.equals(posOverflowKey, zeroCountKey));
        assertFalse(Arrays.equals(posOverflowKey, metadataKey));
        assertFalse(Arrays.equals(zeroCountKey, metadataKey));
    }

    @Test
    void testRangeKeys() {
        DirectorySubspace testSubspace = createOrOpenSubspaceUnderCluster(UUID.randomUUID().toString());

        String histType = HistogramKeySchema.POS_HIST_PREFIX;

        // Test histogram type range
        byte[] histTypeBeginKey = HistogramKeySchema.histogramTypeRangeBegin(testSubspace, histType);
        byte[] histTypeEndKey = HistogramKeySchema.histogramTypeRangeEnd(testSubspace, histType);

        // Begin should be less than end (lexicographic order)
        assertTrue(compareByteArrays(histTypeBeginKey, histTypeEndKey) < 0);

        // Test decade range
        byte[] decadeBeginKey = HistogramKeySchema.decadeRangeBegin(testSubspace, histType, 2);
        byte[] decadeEndKey = HistogramKeySchema.decadeRangeEnd(testSubspace, histType, 2);

        assertTrue(compareByteArrays(decadeBeginKey, decadeEndKey) < 0);

        // Test different histogram types produce different ranges
        byte[] negDecadeBeginKey = HistogramKeySchema.decadeRangeBegin(testSubspace, HistogramKeySchema.NEG_HIST_PREFIX, 2);
        assertFalse(Arrays.equals(decadeBeginKey, negDecadeBeginKey));
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