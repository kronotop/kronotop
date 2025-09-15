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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("APPHistogramArithmetic Tests")
class APPHistogramArithmeticTest {

    private final APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();

    @Test
    @DisplayName("Byte array addition should work correctly")
    void testByteArrayAddition() {
        // Test simple addition 
        byte[] input = {0x00, 0x00, 0x00};
        byte[] result = APPHistogramArithmetic.addToByteArray(input, BigInteger.valueOf(1));
        assertArrayEquals(new byte[]{0x00, 0x00, 0x01}, result);

        // Test larger addition
        input = new byte[]{0x00, 0x00, (byte) 0xFF};
        result = APPHistogramArithmetic.addToByteArray(input, BigInteger.valueOf(1));
        assertArrayEquals(new byte[]{0x00, 0x01, 0x00}, result);

        // Test overflow wrapping
        input = new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        result = APPHistogramArithmetic.addToByteArray(input, BigInteger.valueOf(1));
        assertArrayEquals(new byte[]{0x00, 0x00, 0x00}, result);

        // Test addition of larger value
        input = new byte[]{0x00, 0x00, 0x10};
        result = APPHistogramArithmetic.addToByteArray(input, BigInteger.valueOf(256));
        assertArrayEquals(new byte[]{0x00, 0x01, 0x10}, result);
    }

    @Test
    @DisplayName("Byte array subtraction should work correctly")
    void testByteArraySubtraction() {
        // Test simple subtraction
        byte[] input = {0x00, 0x00, 0x05};
        byte[] result = APPHistogramArithmetic.subtractFromByteArray(input, BigInteger.valueOf(3));
        assertArrayEquals(new byte[]{0x00, 0x00, 0x02}, result);

        // Test subtraction with borrow
        input = new byte[]{0x00, 0x01, 0x00};
        result = APPHistogramArithmetic.subtractFromByteArray(input, BigInteger.valueOf(1));
        assertArrayEquals(new byte[]{0x00, 0x00, (byte) 0xFF}, result);

        // Test underflow wrapping
        input = new byte[]{0x00, 0x00, 0x00};
        result = APPHistogramArithmetic.subtractFromByteArray(input, BigInteger.valueOf(1));
        assertArrayEquals(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}, result);
    }

    @Test
    @DisplayName("Upper bound calculation should follow APP specification")
    void testCalculateUpperBound() {
        // For depth 1: S(1) = 256^2 = 65536
        byte[] lowerBound = {0x00, 0x00, 0x00};
        byte[] upperBound = APPHistogramArithmetic.calculateUpperBound(lowerBound, 1, metadata);
        assertArrayEquals(new byte[]{0x01, 0x00, 0x00}, upperBound);

        // For depth 2: S(2) = 256^1 = 256
        lowerBound = new byte[]{0x12, 0x34, 0x00};
        upperBound = APPHistogramArithmetic.calculateUpperBound(lowerBound, 2, metadata);
        assertArrayEquals(new byte[]{0x12, 0x35, 0x00}, upperBound);

        // For depth 3: S(3) = 256^0 = 1
        lowerBound = new byte[]{0x12, 0x34, 0x56};
        upperBound = APPHistogramArithmetic.calculateUpperBound(lowerBound, 3, metadata);
        assertArrayEquals(new byte[]{0x12, 0x34, 0x57}, upperBound);
    }

    @Test
    @DisplayName("Parent lower bound computation should be correct")
    void testComputeParentLowerBound() {
        // Test with aligned child
        byte[] childLower = {0x12, 0x00, 0x00}; // depth 2, S(2) = 256
        byte[] parentLower = APPHistogramArithmetic.computeParentLowerBound(childLower, 2, metadata);
        // Parent should be at fanout * S(2) = 4 * 256 = 1024 boundary
        // 0x1200 = 4608, floor(4608/1024) * 1024 = 4 * 1024 = 4096 = 0x1000
        assertArrayEquals(new byte[]{0x10, 0x00, 0x00}, parentLower);

        // Test with non-aligned child
        childLower = new byte[]{0x12, 0x34, 0x56}; // depth 3, S(3) = 1
        parentLower = APPHistogramArithmetic.computeParentLowerBound(childLower, 3, metadata);
        // Parent should be at fanout * S(3) = 4 * 1 = 4 boundary
        // 0x123456 = 1193046, floor(1193046/4) * 4 = 298261 * 4 = 1193044 = 0x123454
        assertArrayEquals(new byte[]{0x12, 0x34, 0x54}, parentLower);
    }

    @Test
    @DisplayName("Sibling lower bounds computation should create 4 equal-width siblings")
    void testComputeSiblingLowerBounds() {
        byte[] parentLower = {0x10, 0x00, 0x00}; // Parent at depth 1
        int childDepth = 2; // S(2) = 256

        byte[][] siblings = APPHistogramArithmetic.computeSiblingLowerBounds(parentLower, childDepth, metadata);

        assertEquals(4, siblings.length);
        assertArrayEquals(new byte[]{0x10, 0x00, 0x00}, siblings[0]); // parent + 0 * 256
        assertArrayEquals(new byte[]{0x10, 0x01, 0x00}, siblings[1]); // parent + 1 * 256
        assertArrayEquals(new byte[]{0x10, 0x02, 0x00}, siblings[2]); // parent + 2 * 256
        assertArrayEquals(new byte[]{0x10, 0x03, 0x00}, siblings[3]); // parent + 3 * 256
    }

    @Test
    @DisplayName("Child lower bounds computation should create 4 equal-width children")
    void testComputeChildLowerBounds() {
        byte[] leafLower = {0x12, 0x00, 0x00}; // Leaf at depth 2, S(2) = 256
        int leafDepth = 2;

        byte[][] children = APPHistogramArithmetic.computeChildLowerBounds(leafLower, leafDepth, metadata);

        assertEquals(4, children.length);
        long childWidth = 256 / 4; // = 64
        assertArrayEquals(new byte[]{0x12, 0x00, 0x00}, children[0]); // leaf + 0 * 64
        assertArrayEquals(new byte[]{0x12, 0x00, 0x40}, children[1]); // leaf + 1 * 64 = leaf + 64
        assertArrayEquals(new byte[]{0x12, 0x00, (byte) 0x80}, children[2]); // leaf + 2 * 64 = leaf + 128
        assertArrayEquals(new byte[]{0x12, 0x00, (byte) 0xC0}, children[3]); // leaf + 3 * 64 = leaf + 192
    }

    @Test
    @DisplayName("Child index computation should correctly bucket keys")
    void testComputeChildIndex() {
        byte[] leafLower = {0x12, 0x00, 0x00}; // Leaf at depth 2
        int leafDepth = 2;

        // Test keys in each child bucket
        byte[] key0 = {0x12, 0x00, 0x20}; // In first quarter (0-63)
        assertEquals(0, APPHistogramArithmetic.computeChildIndex(key0, leafLower, leafDepth, metadata));

        byte[] key1 = {0x12, 0x00, 0x50}; // In second quarter (64-127)
        assertEquals(1, APPHistogramArithmetic.computeChildIndex(key1, leafLower, leafDepth, metadata));

        byte[] key2 = {0x12, 0x00, (byte) 0x90}; // In third quarter (128-191)
        assertEquals(2, APPHistogramArithmetic.computeChildIndex(key2, leafLower, leafDepth, metadata));

        byte[] key3 = {0x12, 0x00, (byte) 0xD0}; // In fourth quarter (192-255)
        assertEquals(3, APPHistogramArithmetic.computeChildIndex(key3, leafLower, leafDepth, metadata));

        // Test boundary cases
        byte[] boundary1 = {0x12, 0x00, 0x40}; // Exactly at second child start
        assertEquals(1, APPHistogramArithmetic.computeChildIndex(boundary1, leafLower, leafDepth, metadata));

        byte[] boundary2 = {0x12, 0x00, (byte) 0x80}; // Exactly at third child start
        assertEquals(2, APPHistogramArithmetic.computeChildIndex(boundary2, leafLower, leafDepth, metadata));
    }

    @Test
    @DisplayName("Overlap calculation should handle various range scenarios")
    void testCalculateOverlap() {
        // Complete overlap: query contains leaf
        byte[] queryStart = {0x10, 0x00, 0x00};
        byte[] queryEnd = {0x20, 0x00, 0x00};
        byte[] leafStart = {0x12, 0x00, 0x00};
        byte[] leafEnd = {0x13, 0x00, 0x00};
        long overlap = APPHistogramArithmetic.calculateOverlap(queryStart, queryEnd, leafStart, leafEnd);
        assertEquals(0x10000L, overlap); // Full leaf width

        // Partial overlap: query starts inside leaf
        queryStart = new byte[]{0x12, (byte) 0x80, 0x00};
        queryEnd = new byte[]{0x20, 0x00, 0x00};
        leafStart = new byte[]{0x12, 0x00, 0x00};
        leafEnd = new byte[]{0x13, 0x00, 0x00};
        overlap = APPHistogramArithmetic.calculateOverlap(queryStart, queryEnd, leafStart, leafEnd);
        assertEquals(0x8000L, overlap); // Half leaf width

        // Partial overlap: query ends inside leaf
        queryStart = new byte[]{0x10, 0x00, 0x00};
        queryEnd = new byte[]{0x12, (byte) 0x80, 0x00};
        leafStart = new byte[]{0x12, 0x00, 0x00};
        leafEnd = new byte[]{0x13, 0x00, 0x00};
        overlap = APPHistogramArithmetic.calculateOverlap(queryStart, queryEnd, leafStart, leafEnd);
        assertEquals(0x8000L, overlap); // Half leaf width

        // No overlap: query completely before leaf
        queryStart = new byte[]{0x10, 0x00, 0x00};
        queryEnd = new byte[]{0x11, 0x00, 0x00};
        leafStart = new byte[]{0x12, 0x00, 0x00};
        leafEnd = new byte[]{0x13, 0x00, 0x00};
        overlap = APPHistogramArithmetic.calculateOverlap(queryStart, queryEnd, leafStart, leafEnd);
        assertEquals(0L, overlap);

        // No overlap: query completely after leaf
        queryStart = new byte[]{0x14, 0x00, 0x00};
        queryEnd = new byte[]{0x15, 0x00, 0x00};
        leafStart = new byte[]{0x12, 0x00, 0x00};
        leafEnd = new byte[]{0x13, 0x00, 0x00};
        overlap = APPHistogramArithmetic.calculateOverlap(queryStart, queryEnd, leafStart, leafEnd);
        assertEquals(0L, overlap);
    }

    @Test
    @DisplayName("Unsigned byte array comparison should work correctly")
    void testCompareUnsigned() {
        // Equal arrays
        byte[] a = {0x12, 0x34, 0x56};
        byte[] b = {0x12, 0x34, 0x56};
        assertEquals(0, APPHistogramArithmetic.compareUnsigned(a, b));

        // First array smaller
        a = new byte[]{0x12, 0x34, 0x55};
        b = new byte[]{0x12, 0x34, 0x56};
        assertTrue(APPHistogramArithmetic.compareUnsigned(a, b) < 0);

        // First array larger
        a = new byte[]{0x12, 0x34, 0x57};
        b = new byte[]{0x12, 0x34, 0x56};
        assertTrue(APPHistogramArithmetic.compareUnsigned(a, b) > 0);

        // Test with negative bytes (should be treated as unsigned)
        a = new byte[]{(byte) 0xFF, 0x00, 0x00};
        b = new byte[]{0x7F, 0x00, 0x00};
        assertTrue(APPHistogramArithmetic.compareUnsigned(a, b) > 0); // 0xFF > 0x7F when unsigned

        // Null handling
        assertEquals(0, APPHistogramArithmetic.compareUnsigned(null, null));
        assertTrue(APPHistogramArithmetic.compareUnsigned(null, b) < 0);
        assertTrue(APPHistogramArithmetic.compareUnsigned(a, null) > 0);
    }

    @Test
    @DisplayName("Global bounds creation should work correctly")
    void testGlobalBounds() {
        byte[] globalLow = APPHistogramArithmetic.createGlobalLow(3);
        assertArrayEquals(new byte[]{0x00, 0x00, 0x00}, globalLow);

        byte[] globalHigh = APPHistogramArithmetic.createGlobalHigh(3);
        assertArrayEquals(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}, globalHigh);

        // Verify they compare correctly
        assertTrue(APPHistogramArithmetic.compareUnsigned(globalLow, globalHigh) < 0);
    }

    @Test
    @DisplayName("Edge case handling should be robust")
    void testEdgeCases() {
        // Empty byte arrays
        byte[] empty = new byte[0];
        byte[] result = APPHistogramArithmetic.addToByteArray(empty, BigInteger.ONE);
        assertArrayEquals(new byte[0], result);

        // Single byte operations
        byte[] single = {0x7F};
        result = APPHistogramArithmetic.addToByteArray(single, BigInteger.ONE);
        assertArrayEquals(new byte[]{(byte) 0x80}, result);

        // Large additions that cause multiple carries
        byte[] input = {0x00, (byte) 0xFF, (byte) 0xFF};
        result = APPHistogramArithmetic.addToByteArray(input, BigInteger.valueOf(257));
        assertArrayEquals(new byte[]{0x01, 0x01, 0x00}, result);
    }
}