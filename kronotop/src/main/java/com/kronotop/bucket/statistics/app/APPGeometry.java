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

package com.kronotop.bucket.statistics.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Geometric helper functions for Adaptive Prefix Partitioning (APP) histogram.
 * 
 * Implements the core geometric operations from the APP specification:
 * - Right padding for canonicalization
 * - Leaf width calculations: S(d) = 256^(D_max - d)
 * - Upper bound computation
 * - Parent/sibling/children arithmetic
 * 
 * These functions are the foundation of the APP algorithm's deterministic geometry.
 */
public final class APPGeometry {
    
    private APPGeometry() {
        // Utility class
    }
    
    /**
     * Right-pads a byte array to the specified length with the given pad byte.
     * This is the canonicalization function from the APP spec.
     * 
     * @param input The input byte array
     * @param targetLength The desired length
     * @param padByte The byte to pad with (typically 0x00 or 0xFF)
     * @return Right-padded byte array
     */
    public static byte[] rightPad(byte[] input, int targetLength, byte padByte) {
        if (input == null) {
            throw new IllegalArgumentException("input cannot be null");
        }
        if (targetLength < 0) {
            throw new IllegalArgumentException("targetLength must be >= 0");
        }
        
        if (input.length >= targetLength) {
            return Arrays.copyOf(input, targetLength);
        }
        
        byte[] result = new byte[targetLength];
        System.arraycopy(input, 0, result, 0, input.length);
        Arrays.fill(result, input.length, targetLength, padByte);
        return result;
    }
    
    /**
     * Calculates leaf width using the APP formula: S(d) = 256^(D_max - d).
     * This determines how many "minimal cells" a leaf at depth d covers.
     * 
     * @param depth The depth of the leaf (1 to maxDepth)
     * @param maxDepth The maximum depth of the tree
     * @return The leaf width in minimal cells
     */
    public static long leafWidth(int depth, int maxDepth) {
        if (depth < 1 || depth > maxDepth) {
            throw new IllegalArgumentException("depth must be between 1 and " + maxDepth + ", got: " + depth);
        }
        
        int exponent = maxDepth - depth;
        if (exponent == 0) {
            return 1L;
        }
        
        long result = 1L;
        for (int i = 0; i < exponent; i++) {
            result *= 256L;
            if (result < 0) {
                throw new ArithmeticException("Leaf width overflow for depth " + depth + ", maxDepth " + maxDepth);
            }
        }
        return result;
    }
    
    /**
     * Computes the upper bound of a leaf's range.
     * Formula: U_pad = L_pad + S(depth)
     * 
     * @param lowerPad The lower bound (padded to maxDepth)
     * @param depth The depth of the leaf
     * @param maxDepth The maximum depth
     * @return The upper bound (exclusive)
     */
    public static byte[] computeUpperBound(byte[] lowerPad, int depth, int maxDepth) {
        if (lowerPad == null) {
            throw new IllegalArgumentException("lowerPad cannot be null");
        }
        if (lowerPad.length != maxDepth) {
            throw new IllegalArgumentException("lowerPad must be padded to maxDepth");
        }
        
        long width = leafWidth(depth, maxDepth);
        return addToByteArray(lowerPad, width);
    }
    
    /**
     * Computes the parent leaf ID for a given child leaf.
     * Uses arithmetic geometry: parentL = floor(L_pad / S_parent) * S_parent
     * where S_parent = 4 * S(depth)
     * 
     * @param childLeaf The child leaf
     * @return The parent leaf ID
     */
    public static APPLeafId computeParent(APPLeafId childLeaf) {
        if (childLeaf.depth() <= 1) {
            throw new IllegalArgumentException("Cannot compute parent for root leaf");
        }
        
        int maxDepth = childLeaf.getMaxDepth();
        int childDepth = childLeaf.depth();
        int parentDepth = childDepth - 1;
        
        long childWidth = leafWidth(childDepth, maxDepth);
        long parentWidth = 4 * childWidth; // FANOUT = 4
        
        byte[] childLowerPad = childLeaf.lowerPad();
        long childOffset = byteArrayToLong(childLowerPad, maxDepth - childDepth);
        
        long parentOffset = (childOffset / parentWidth) * parentWidth;
        byte[] parentLowerPad = longToByteArray(parentOffset, maxDepth, maxDepth - parentDepth);
        
        return new APPLeafId(parentLowerPad, parentDepth);
    }
    
    /**
     * Computes all four sibling leaf IDs for a given leaf.
     * Siblings share the same parent and are at the same depth.
     * 
     * @param leaf The leaf to find siblings for
     * @return List of four sibling leaf IDs (including the input leaf)
     */
    public static List<APPLeafId> computeSiblings(APPLeafId leaf) {
        if (leaf.depth() <= 1) {
            // Root level - return just this leaf
            return List.of(leaf);
        }
        
        APPLeafId parent = computeParent(leaf);
        return computeChildren(parent);
    }
    
    /**
     * Computes the four child leaf IDs for a given parent leaf.
     * Uses quartile split geometry: childL_i = L_pad + i * childWidth
     * 
     * @param parentLeaf The parent leaf
     * @return List of four child leaf IDs
     */
    public static List<APPLeafId> computeChildren(APPLeafId parentLeaf) {
        int maxDepth = parentLeaf.getMaxDepth();
        int parentDepth = parentLeaf.depth();
        
        if (parentDepth >= maxDepth) {
            throw new IllegalArgumentException("Cannot compute children for leaf at maximum depth");
        }
        
        int childDepth = parentDepth + 1;
        long parentWidth = leafWidth(parentDepth, maxDepth);
        long childWidth = parentWidth / APPConfiguration.FANOUT; // Should be parentWidth / 4
        
        byte[] parentLowerPad = parentLeaf.lowerPad();
        List<APPLeafId> children = new ArrayList<>();
        
        for (int i = 0; i < APPConfiguration.FANOUT; i++) {
            long childOffset = byteArrayToLong(parentLowerPad, maxDepth - parentDepth) + i * childWidth;
            byte[] childLowerPad = longToByteArray(childOffset, maxDepth, maxDepth - childDepth);
            children.add(new APPLeafId(childLowerPad, childDepth));
        }
        
        return children;
    }
    
    /**
     * Checks if two byte arrays are lexicographically ordered.
     * 
     * @param a First array
     * @param b Second array
     * @return true if a <= b lexicographically
     */
    public static boolean isLexicographicallyOrdered(byte[] a, byte[] b) {
        return compareBytes(a, b) <= 0;
    }
    
    /**
     * Adds a long value to a byte array, treating it as a big-endian unsigned integer.
     * Used for computing upper bounds.
     */
    private static byte[] addToByteArray(byte[] input, long value) {
        byte[] result = input.clone();
        long carry = value;
        
        for (int i = result.length - 1; i >= 0 && carry > 0; i--) {
            long sum = (result[i] & 0xFFL) + carry;
            result[i] = (byte) (sum & 0xFF);
            carry = sum >>> 8;
        }
        
        if (carry > 0) {
            throw new ArithmeticException("Addition overflow in byte array");
        }
        
        return result;
    }
    
    /**
     * Converts relevant bytes of a byte array to a long value.
     * Only considers the bytes that vary at the given depth.
     */
    private static long byteArrayToLong(byte[] array, int significantBytes) {
        long result = 0;
        int startIndex = array.length - significantBytes;
        
        for (int i = startIndex; i < array.length; i++) {
            result = (result << 8) | (array[i] & 0xFFL);
        }
        
        return result;
    }
    
    /**
     * Converts a long value back to a byte array at the appropriate position.
     */
    private static byte[] longToByteArray(long value, int totalLength, int significantBytes) {
        byte[] result = new byte[totalLength];
        
        int startIndex = totalLength - significantBytes;
        for (int i = startIndex; i < totalLength; i++) {
            int shift = (totalLength - 1 - i) * 8;
            result[i] = (byte) ((value >>> shift) & 0xFF);
        }
        
        return result;
    }
    
    /**
     * Compares two byte arrays lexicographically.
     */
    private static int compareBytes(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int result = Byte.compareUnsigned(a[i], b[i]);
            if (result != 0) {
                return result;
            }
        }
        return Integer.compare(a.length, b.length);
    }
}