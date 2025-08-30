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

import java.util.Arrays;

/**
 * Represents a leaf node in the Adaptive Prefix Partitioning histogram.
 * Contains the lexicographic interval [lowerBound, upperBound) that this leaf covers.
 */
public record APPLeaf(
        byte[] lowerBound,    // L_pad (padded to maxDepth)
        int depth,           // d (leaf depth)
        byte[] upperBound    // U_pad (computed, exclusive)
) {

    /**
     * Creates an APPLeaf with computed upper bound.
     */
    public static APPLeaf create(byte[] lowerBound, int depth, int maxDepth) {
        byte[] upperBound = APPKeySchema.computeUpperBound(lowerBound, depth, maxDepth);
        return new APPLeaf(lowerBound, depth, upperBound);
    }

    /**
     * Creates an APPLeaf from a compound key extracted from FoundationDB.
     */
    public static APPLeaf fromCompoundKey(byte[] compoundKey, int maxDepth) {
        int depth = APPKeySchema.extractDepth(compoundKey);
        byte[] lowerBound = APPKeySchema.extractLowerBound(compoundKey);
        return create(lowerBound, depth, maxDepth);
    }

    /**
     * Checks if this leaf contains the given key (canonicalized).
     */
    public boolean contains(byte[] keyPad) {
        // Special case for root leaf at depth 1 with all-zero lower bound
        if (depth == 1 && Arrays.equals(lowerBound, new byte[lowerBound.length])) {
            // Root leaf contains everything
            return true;
        }
        
        return Arrays.compareUnsigned(lowerBound, keyPad) <= 0 &&
               Arrays.compareUnsigned(keyPad, upperBound) < 0;
    }

    /**
     * Computes the parent leaf of this leaf.
     */
    public APPLeaf getParent(APPHistogramMetadata metadata) {
        if (depth <= 1) {
            throw new IllegalStateException("Cannot get parent of root leaf");
        }
        
        long parentWidth = metadata.leafWidth(depth - 1);
        long childWidth = metadata.leafWidth(depth);
        
        // Convert to long for arithmetic
        long currentLower = APPKeySchema.bytesToLong(lowerBound);
        
        // Find parent's lower bound
        long parentLower = (currentLower / parentWidth) * parentWidth;
        
        // Convert back to padded bytes
        byte[] parentLowerBound = APPKeySchema.longToBytes(parentLower, metadata.maxDepth());
        
        return create(parentLowerBound, depth - 1, metadata.maxDepth());
    }

    /**
     * Computes all sibling leaves of this leaf (including itself).
     */
    public APPLeaf[] getSiblings(APPHistogramMetadata metadata) {
        if (depth <= 1) {
            return new APPLeaf[]{this}; // Root has no siblings
        }
        
        APPLeaf parent = getParent(metadata);
        return parent.getChildren(metadata);
    }

    /**
     * Computes child leaves that would result from splitting this leaf.
     */
    public APPLeaf[] getChildren(APPHistogramMetadata metadata) {
        if (depth >= metadata.maxDepth()) {
            throw new IllegalStateException("Cannot split leaf at maximum depth");
        }
        
        long childWidth = metadata.childWidth(depth);
        long currentLower = APPKeySchema.bytesToLong(lowerBound);
        
        APPLeaf[] children = new APPLeaf[metadata.fanout()];
        
        for (int i = 0; i < metadata.fanout(); i++) {
            long childLower = currentLower + (i * childWidth);
            byte[] childLowerBound = APPKeySchema.longToBytes(childLower, metadata.maxDepth());
            children[i] = create(childLowerBound, depth + 1, metadata.maxDepth());
        }
        
        return children;
    }

    /**
     * Computes the covered portion of this leaf for a given range query.
     * Returns a value between 0.0 and 1.0 indicating the coverage ratio.
     */
    public double computeCoverageRatio(byte[] queryLowerBound, byte[] queryUpperBound) {
        // Find the actual intersection
        byte[] intersectionLower = maxBytes(queryLowerBound, lowerBound);
        byte[] intersectionUpper = minBytes(queryUpperBound, upperBound);
        
        // Check if there's any intersection
        if (Arrays.compareUnsigned(intersectionLower, intersectionUpper) >= 0) {
            return 0.0; // No intersection
        }
        
        // Convert to longs for arithmetic (works for small address spaces)
        long leafLower = APPKeySchema.bytesToLong(lowerBound);
        long leafUpper = APPKeySchema.bytesToLong(upperBound);
        long intLower = APPKeySchema.bytesToLong(intersectionLower);
        long intUpper = APPKeySchema.bytesToLong(intersectionUpper);
        
        long covered = intUpper - intLower;
        long total = leafUpper - leafLower;
        
        return (double) covered / total;
    }

    /**
     * Returns the lexicographically larger of two byte arrays.
     */
    private byte[] maxBytes(byte[] a, byte[] b) {
        return Arrays.compareUnsigned(a, b) >= 0 ? a : b;
    }

    /**
     * Returns the lexicographically smaller of two byte arrays.
     */
    private byte[] minBytes(byte[] a, byte[] b) {
        return Arrays.compareUnsigned(a, b) <= 0 ? a : b;
    }

    /**
     * Returns a unique identifier for this leaf (for use as map keys, etc.).
     */
    public String getLeafId() {
        return Arrays.toString(lowerBound) + "@" + depth;
    }

    /**
     * Custom toString for debugging.
     */
    @Override
    public String toString() {
        return String.format("APPLeaf{lower=%s, depth=%d, upper=%s}", 
                Arrays.toString(lowerBound), depth, Arrays.toString(upperBound));
    }

    /**
     * Custom equals that properly handles byte arrays.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        APPLeaf appLeaf = (APPLeaf) obj;
        return depth == appLeaf.depth &&
               Arrays.equals(lowerBound, appLeaf.lowerBound) &&
               Arrays.equals(upperBound, appLeaf.upperBound);
    }

    /**
     * Custom hashCode that properly handles byte arrays.
     */
    @Override
    public int hashCode() {
        int result = Arrays.hashCode(lowerBound);
        result = 31 * result + depth;
        result = 31 * result + Arrays.hashCode(upperBound);
        return result;
    }
}