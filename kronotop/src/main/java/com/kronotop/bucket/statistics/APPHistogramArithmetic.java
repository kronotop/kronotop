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

import java.math.BigInteger;
import java.util.Arrays;

/**
 * Arithmetic operations for APP histogram geometry calculations.
 * Handles byte array arithmetic for leaf bounds, parent/sibling computation,
 * and address space navigation.
 *
 * All calculations follow the APP specification:
 * - S(d) = 256^(D_max - d) (leaf width at depth d)
 * - Equal-width geometry with quartile splits
 * - Deterministic parent/sibling relationships
 */
public class APPHistogramArithmetic {

    /**
     * Adds a BigInteger value to a byte array representing an unsigned integer.
     * The byte array is treated as big-endian.
     *
     * @param bytes    byte array to add to (big-endian)
     * @param addValue value to add
     * @return new byte array with the sum, same length as input
     */
    public static byte[] addToByteArray(byte[] bytes, BigInteger addValue) {
        if (bytes == null || bytes.length == 0) {
            return new byte[0];
        }

        // Convert byte array to BigInteger (unsigned)
        BigInteger current = new BigInteger(1, bytes);

        // Add the value
        BigInteger result = current.add(addValue);

        // Convert back to byte array, ensuring same length
        byte[] resultBytes = result.toByteArray();

        // Handle sign byte if result is too large
        if (resultBytes.length > bytes.length) {
            // Overflow - wrap around by taking modulo 2^(8*bytes.length)
            BigInteger maxValue = BigInteger.ONE.shiftLeft(bytes.length * 8);
            result = result.remainder(maxValue);
            resultBytes = result.toByteArray();
        }

        // Pad to correct length if needed
        if (resultBytes.length < bytes.length) {
            byte[] padded = new byte[bytes.length];
            System.arraycopy(resultBytes, 0, padded, bytes.length - resultBytes.length, resultBytes.length);
            return padded;
        } else if (resultBytes.length == bytes.length) {
            return resultBytes;
        } else {
            // Take only the rightmost bytes
            return Arrays.copyOfRange(resultBytes, resultBytes.length - bytes.length, resultBytes.length);
        }
    }

    /**
     * Subtracts a BigInteger value from a byte array representing an unsigned integer.
     *
     * @param bytes        byte array to subtract from (big-endian)
     * @param subtractValue value to subtract
     * @return new byte array with the difference, same length as input
     */
    public static byte[] subtractFromByteArray(byte[] bytes, BigInteger subtractValue) {
        return addToByteArray(bytes, subtractValue.negate());
    }

    /**
     * Calculates the upper bound of a leaf given its lower bound and depth.
     * U_pad = L_pad + S(d)
     *
     * @param lowerPad the lower bound (padded to maxDepth bytes)
     * @param depth    the depth of the leaf
     * @param metadata histogram metadata containing maxDepth
     * @return upper bound byte array
     */
    public static byte[] calculateUpperBound(byte[] lowerPad, int depth, APPHistogramMetadata metadata) {
        if (lowerPad == null) {
            throw new IllegalArgumentException("lowerPad cannot be null");
        }

        // Special case for depth 0 (root leaf): spans entire address space
        if (depth == 0) {
            return createGlobalHigh(lowerPad.length);
        }

        long leafWidth = metadata.leafWidth(depth);
        BigInteger width = BigInteger.valueOf(leafWidth);

        return addToByteArray(lowerPad, width);
    }

    /**
     * Computes the parent's lower bound for a given leaf.
     * parentL = floor(L_pad / S_parent) * S_parent
     * where S_parent = fanout * S(d)
     *
     * @param lowerPad child's lower bound
     * @param depth    child's depth
     * @param metadata histogram metadata
     * @return parent's lower bound
     */
    public static byte[] computeParentLowerBound(byte[] lowerPad, int depth, APPHistogramMetadata metadata) {
        if (depth <= 1) {
            throw new IllegalArgumentException("Cannot compute parent for depth <= 1");
        }

        // Convert to BigInteger for arithmetic
        BigInteger currentL = new BigInteger(1, lowerPad);

        // Calculate parent width: S_parent = fanout * S(d)
        long parentWidth = metadata.parentWidth(depth);
        BigInteger parentWidthBI = BigInteger.valueOf(parentWidth);

        // parentL = floor(L_pad / S_parent) * S_parent
        BigInteger parentL = currentL.divide(parentWidthBI).multiply(parentWidthBI);

        // Convert back to byte array
        byte[] result = parentL.toByteArray();

        // Ensure same length as input
        if (result.length < lowerPad.length) {
            byte[] padded = new byte[lowerPad.length];
            System.arraycopy(result, 0, padded, lowerPad.length - result.length, result.length);
            return padded;
        } else if (result.length == lowerPad.length) {
            return result;
        } else {
            // Take rightmost bytes (should not happen with proper bounds)
            return Arrays.copyOfRange(result, result.length - lowerPad.length, result.length);
        }
    }

    /**
     * Computes the lower bounds of all four siblings given a parent.
     * sibL_i = parentL + i * S(d) for i = 0, 1, 2, 3
     *
     * @param parentLowerBound parent's lower bound
     * @param childDepth       depth of the children
     * @param metadata         histogram metadata
     * @return array of 4 sibling lower bounds
     */
    public static byte[][] computeSiblingLowerBounds(byte[] parentLowerBound, int childDepth, APPHistogramMetadata metadata) {
        if (metadata.isMaxDepth(childDepth)) {
            throw new IllegalArgumentException("Cannot create siblings at max depth");
        }

        long childWidth = metadata.leafWidth(childDepth);
        BigInteger childWidthBI = BigInteger.valueOf(childWidth);

        byte[][] siblings = new byte[metadata.fanout()][];

        for (int i = 0; i < metadata.fanout(); i++) {
            BigInteger offset = childWidthBI.multiply(BigInteger.valueOf(i));
            siblings[i] = addToByteArray(parentLowerBound, offset);
        }

        return siblings;
    }

    /**
     * Computes the lower bounds of all four children when splitting a leaf.
     * childL_i = L_pad + i * (S(d) / fanout) for i = 0, 1, 2, 3
     *
     * @param leafLowerBound parent leaf's lower bound
     * @param leafDepth      parent leaf's depth
     * @param metadata       histogram metadata
     * @return array of 4 child lower bounds
     */
    public static byte[][] computeChildLowerBounds(byte[] leafLowerBound, int leafDepth, APPHistogramMetadata metadata) {
        if (metadata.isMaxDepth(leafDepth)) {
            throw new IllegalArgumentException("Cannot split leaf at max depth");
        }

        long leafWidth = metadata.leafWidth(leafDepth);
        long childWidth = leafWidth / metadata.fanout();
        BigInteger childWidthBI = BigInteger.valueOf(childWidth);

        byte[][] children = new byte[metadata.fanout()][];

        for (int i = 0; i < metadata.fanout(); i++) {
            BigInteger offset = childWidthBI.multiply(BigInteger.valueOf(i));
            children[i] = addToByteArray(leafLowerBound, offset);
        }

        return children;
    }

    /**
     * Determines which child a given key belongs to when splitting a leaf.
     *
     * @param keyPad         the key (padded to maxDepth)
     * @param leafLowerBound parent leaf's lower bound
     * @param leafDepth      parent leaf's depth
     * @param metadata       histogram metadata
     * @return child index (0, 1, 2, or 3)
     */
    public static int computeChildIndex(byte[] keyPad, byte[] leafLowerBound, int leafDepth, APPHistogramMetadata metadata) {
        // Calculate relative position within the leaf
        BigInteger keyBI = new BigInteger(1, keyPad);
        BigInteger leafBI = new BigInteger(1, leafLowerBound);
        BigInteger relativePos = keyBI.subtract(leafBI);

        // Calculate child width
        long leafWidth = metadata.leafWidth(leafDepth);
        long childWidth = leafWidth / metadata.fanout();
        BigInteger childWidthBI = BigInteger.valueOf(childWidth);

        // Determine which child bucket
        BigInteger childIndex = relativePos.divide(childWidthBI);

        // Clamp to valid range [0, fanout-1]
        int index = childIndex.intValue();
        if (index < 0) index = 0;
        if (index >= metadata.fanout()) index = metadata.fanout() - 1;

        return index;
    }

    /**
     * Calculates the overlap between a query range [queryStart, queryEnd) and a leaf [leafStart, leafEnd).
     * Returns the number of "minimal cells" that overlap.
     *
     * @param queryStart query range start (inclusive)
     * @param queryEnd   query range end (exclusive)
     * @param leafStart  leaf range start (inclusive)
     * @param leafEnd    leaf range end (exclusive)
     * @return overlap size in minimal cells
     */
    public static long calculateOverlap(byte[] queryStart, byte[] queryEnd, byte[] leafStart, byte[] leafEnd) {
        // Convert to BigInteger for comparison
        BigInteger qStart = new BigInteger(1, queryStart);
        BigInteger qEnd = new BigInteger(1, queryEnd);
        BigInteger lStart = new BigInteger(1, leafStart);
        BigInteger lEnd = new BigInteger(1, leafEnd);

        // Calculate effective overlap bounds
        BigInteger overlapStart = qStart.max(lStart);
        BigInteger overlapEnd = qEnd.min(lEnd);

        // If no overlap, return 0
        if (overlapStart.compareTo(overlapEnd) >= 0) {
            return 0;
        }

        // Return overlap size
        BigInteger overlap = overlapEnd.subtract(overlapStart);

        // Convert to long, clamping to Long.MAX_VALUE if too large
        if (overlap.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
            return Long.MAX_VALUE;
        }

        return overlap.longValue();
    }

    /**
     * Compares two byte arrays lexicographically as unsigned integers.
     *
     * @param a first byte array
     * @param b second byte array
     * @return negative if a < b, zero if a == b, positive if a > b
     */
    public static int compareUnsigned(byte[] a, byte[] b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;

        BigInteger aBI = new BigInteger(1, a);
        BigInteger bBI = new BigInteger(1, b);

        return aBI.compareTo(bBI);
    }

    /**
     * Creates a global minimum bound (all zeros) for the given length.
     */
    public static byte[] createGlobalLow(int length) {
        return new byte[length];
    }

    /**
     * Creates a global maximum bound (all 0xFF) for the given length.
     */
    public static byte[] createGlobalHigh(int length) {
        byte[] high = new byte[length];
        Arrays.fill(high, (byte) 0xFF);
        return high;
    }
}