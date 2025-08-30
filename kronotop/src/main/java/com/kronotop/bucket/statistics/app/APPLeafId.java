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
import java.util.Objects;

/**
 * Unique identifier for an APP histogram leaf node.
 * 
 * Each leaf is identified by:
 * - lowerPad: The lower bound of the leaf's range, right-padded to maxDepth bytes with 0x00
 * - depth: The depth of the leaf in the tree (1 to maxDepth)
 * 
 * The leaf covers the range [lowerPad, lowerPad + S(depth)) where S(depth) = 256^(maxDepth - depth).
 */
public record APPLeafId(byte[] lowerPad, int depth) {
    
    public APPLeafId {
        Objects.requireNonNull(lowerPad, "lowerPad cannot be null");
        if (depth < 1) {
            throw new IllegalArgumentException("depth must be >= 1, got: " + depth);
        }
        if (lowerPad.length == 0) {
            throw new IllegalArgumentException("lowerPad cannot be empty");
        }
    }
    
    /**
     * Creates a copy of this leaf ID with a new depth.
     * Used during split/merge operations when changing tree levels.
     */
    public APPLeafId withDepth(int newDepth) {
        return new APPLeafId(lowerPad.clone(), newDepth);
    }
    
    /**
     * Creates a copy of this leaf ID with a new lower bound.
     * Used during geometric calculations.
     */
    public APPLeafId withLowerPad(byte[] newLowerPad) {
        return new APPLeafId(newLowerPad.clone(), depth);
    }
    
    /**
     * Gets the maximum depth this leaf ID was designed for.
     * This is inferred from the lowerPad length.
     */
    public int getMaxDepth() {
        return lowerPad.length;
    }
    
    /**
     * Returns a defensive copy of the lower bound to prevent mutation.
     */
    public byte[] getLowerPadCopy() {
        return lowerPad.clone();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        APPLeafId that = (APPLeafId) obj;
        return depth == that.depth && Arrays.equals(lowerPad, that.lowerPad);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(lowerPad), depth);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("APPLeafId{lowerPad=");
        for (byte b : lowerPad) {
            sb.append(String.format("%02X", b & 0xFF));
        }
        sb.append(", depth=").append(depth).append('}');
        return sb.toString();
    }
}