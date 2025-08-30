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

import java.util.Arrays;

/**
 * Complete information about an APP histogram leaf, including its bounds.
 * 
 * This is returned by leaf lookup operations and contains everything needed
 * to work with the leaf: its identity, range boundaries, and metadata.
 * 
 * The leaf covers the range [lowerPad, upperPad) where upperPad is computed
 * as lowerPad + S(depth) using the APP geometric formulas.
 */
public record APPLeafInfo(
    APPLeafId leafId,
    byte[] lowerPad,
    byte[] upperPad,
    APPMetadata metadata
) {
    
    public APPLeafInfo {
        if (leafId == null) {
            throw new IllegalArgumentException("leafId cannot be null");
        }
        if (lowerPad == null) {
            throw new IllegalArgumentException("lowerPad cannot be null");
        }
        if (upperPad == null) {
            throw new IllegalArgumentException("upperPad cannot be null");
        }
        if (metadata == null) {
            throw new IllegalArgumentException("metadata cannot be null");
        }
        if (lowerPad.length != upperPad.length) {
            throw new IllegalArgumentException("lowerPad and upperPad must have same length");
        }
        if (!Arrays.equals(lowerPad, leafId.lowerPad())) {
            throw new IllegalArgumentException("lowerPad must match leafId.lowerPad");
        }
        if (metadata.depth() != leafId.depth()) {
            throw new IllegalArgumentException("metadata.depth must match leafId.depth");
        }
    }
    
    /**
     * Gets the depth of this leaf.
     */
    public int getDepth() {
        return leafId.depth();
    }
    
    /**
     * Gets the maximum depth this leaf was designed for.
     */
    public int getMaxDepth() {
        return lowerPad.length;
    }
    
    /**
     * Checks if the given key falls within this leaf's range.
     * The key should already be padded to the appropriate length.
     */
    public boolean containsKey(byte[] keyPad) {
        if (keyPad.length != lowerPad.length) {
            throw new IllegalArgumentException("keyPad length must match leaf padding length");
        }
        
        // Check if lowerPad <= keyPad < upperPad
        return compareBytes(lowerPad, keyPad) <= 0 && compareBytes(keyPad, upperPad) < 0;
    }
    
    /**
     * Gets the width of this leaf in terms of minimal cells.
     * This is S(depth) = 256^(maxDepth - depth).
     */
    public long getLeafWidth() {
        int maxDepth = getMaxDepth();
        int depth = getDepth();
        
        if (depth > maxDepth) {
            return 0; // Invalid leaf
        }
        
        long width = 1;
        for (int i = 0; i < (maxDepth - depth); i++) {
            width *= 256;
        }
        return width;
    }
    
    /**
     * Creates defensive copies of the boundary arrays to prevent external mutation.
     */
    public APPLeafInfo copy() {
        return new APPLeafInfo(
            leafId, 
            lowerPad.clone(), 
            upperPad.clone(), 
            metadata
        );
    }
    
    /**
     * Compares two byte arrays lexicographically.
     * Returns negative if a < b, zero if a == b, positive if a > b.
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
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("APPLeafInfo{");
        sb.append("leafId=").append(leafId);
        sb.append(", range=[");
        
        // Show first few bytes of range for readability
        int showBytes = Math.min(4, lowerPad.length);
        for (int i = 0; i < showBytes; i++) {
            if (i > 0) sb.append(" ");
            sb.append(String.format("%02X", lowerPad[i] & 0xFF));
        }
        if (lowerPad.length > showBytes) sb.append("...");
        
        sb.append(" - ");
        
        for (int i = 0; i < showBytes; i++) {
            if (i > 0) sb.append(" ");
            sb.append(String.format("%02X", upperPad[i] & 0xFF));
        }
        if (upperPad.length > showBytes) sb.append("...");
        
        sb.append("), width=").append(getLeafWidth());
        sb.append(", metadata=").append(metadata);
        sb.append('}');
        
        return sb.toString();
    }
}