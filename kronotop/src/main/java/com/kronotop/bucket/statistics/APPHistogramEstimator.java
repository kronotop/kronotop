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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;

/**
 * Estimator for APP (Adaptive Prefix Partitioning) histogram selectivity calculations.
 * <p>
 * Provides range selectivity estimation for predicates like =, <, <=, >, >=, BETWEEN
 * using the partial coverage ratio algorithm from the APP specification.
 * <p>
 * Algorithm:
 * 1. Canonicalize query range [A, B) to [A_pad, B_pad)
 * 2. Find first leaf overlapping with A_pad
 * 3. Iterate through all leaves that overlap [A_pad, B_pad)
 * 4. For each leaf, calculate overlap ratio and contribute to estimate
 * 5. Return total estimated selectivity
 */
public class APPHistogramEstimator {
    private final APPHistogramMetadata metadata;
    private final DirectorySubspace subspace;

    public APPHistogramEstimator(APPHistogramMetadata metadata, DirectorySubspace subspace) {
        this.metadata = metadata;
        this.subspace = subspace;
    }

    /**
     * Estimates selectivity for an equality predicate (= K).
     * Note: For planning correctness, this should be used as a hint only.
     * The planner should perform a small index peek for precise cardinality.
     */
    public double estimateEquality(Transaction tr, byte[] value) {
        // For equality, treat as a very small range
        byte[] valuePad = APPHistogramKeySchema.rightPad(value, metadata.maxDepth(), (byte) 0x00);
        byte[] valueNext = APPHistogramKeySchema.rightPad(value, metadata.maxDepth(), (byte) 0x00);

        // Increment by 1 to create a minimal range [value, value+1)
        valueNext = APPHistogramArithmetic.addToByteArray(valueNext, java.math.BigInteger.ONE);

        return estimateRange(tr, valuePad, valueNext);
    }

    /**
     * Estimates selectivity for a range predicate [start, end).
     * Uses the partial coverage ratio algorithm from APP specification.
     */
    public double estimateRange(Transaction tr, byte[] startInclusive, byte[] endExclusive) {
        // 1. Canonicalize to padded ranges
        byte[] startPad = APPHistogramKeySchema.rightPad(startInclusive, metadata.maxDepth(), (byte) 0x00);
        byte[] endPad = APPHistogramKeySchema.rightPad(endExclusive, metadata.maxDepth(), (byte) 0x00);

        // Validate range
        if (APPHistogramArithmetic.compareUnsigned(startPad, endPad) >= 0) {
            return 0.0; // Empty or invalid range
        }

        // 2. Find first leaf that overlaps with startPad
        APPHistogram.LeafInfo currentLeaf = findFirstOverlappingLeaf(tr, startPad);
        if (currentLeaf == null) {
            return 0.0; // No leaves found
        }

        double totalEstimate = 0.0;

        // 3. Iterate through all leaves that overlap [startPad, endPad)
        while (currentLeaf != null &&
               APPHistogramArithmetic.compareUnsigned(currentLeaf.lowerBound(), endPad) < 0) {

            // Calculate overlap between query range and current leaf
            long overlap = APPHistogramArithmetic.calculateOverlap(
                    startPad, endPad, currentLeaf.lowerBound(), currentLeaf.upperBound());

            if (overlap > 0) {
                // Calculate leaf width
                long leafWidth = metadata.leafWidth(currentLeaf.depth());

                // Calculate coverage ratio
                double ratio = (double) overlap / leafWidth;

                // Get leaf count and contribute to estimate
                long leafCount = estimateLeafCount(tr, currentLeaf);
                totalEstimate += leafCount * ratio;
            }

            // Move to next leaf
            currentLeaf = findNextLeaf(tr, currentLeaf);
        }

        return totalEstimate;
    }

    /**
     * Estimates selectivity for a less-than predicate (< value).
     */
    public double estimateLessThan(Transaction tr, byte[] value, boolean inclusive) {
        byte[] startPad = APPHistogramArithmetic.createGlobalLow(metadata.maxDepth());
        byte[] endPad;

        if (inclusive) {
            // For <= value, include the value itself
            endPad = APPHistogramKeySchema.rightPad(value, metadata.maxDepth(), (byte) 0xFF);
            endPad = APPHistogramArithmetic.addToByteArray(endPad, java.math.BigInteger.ONE);
        } else {
            // For < value, exclude the value
            endPad = APPHistogramKeySchema.rightPad(value, metadata.maxDepth(), (byte) 0x00);
        }

        return estimateRange(tr, startPad, endPad);
    }

    /**
     * Estimates selectivity for a greater-than predicate (> value).
     */
    public double estimateGreaterThan(Transaction tr, byte[] value, boolean inclusive) {
        byte[] startPad;
        byte[] endPad = APPHistogramArithmetic.createGlobalHigh(metadata.maxDepth());

        if (inclusive) {
            // For >= value, include the value itself
            startPad = APPHistogramKeySchema.rightPad(value, metadata.maxDepth(), (byte) 0x00);
        } else {
            // For > value, exclude the value
            startPad = APPHistogramKeySchema.rightPad(value, metadata.maxDepth(), (byte) 0xFF);
            startPad = APPHistogramArithmetic.addToByteArray(startPad, java.math.BigInteger.ONE);
        }

        return estimateRange(tr, startPad, endPad);
    }

    /**
     * Estimates selectivity for a BETWEEN predicate [start, end].
     */
    public double estimateBetween(Transaction tr, byte[] start, byte[] end,
                                 boolean startInclusive, boolean endInclusive) {
        byte[] startPad;
        byte[] endPad;

        if (startInclusive) {
            startPad = APPHistogramKeySchema.rightPad(start, metadata.maxDepth(), (byte) 0x00);
        } else {
            startPad = APPHistogramKeySchema.rightPad(start, metadata.maxDepth(), (byte) 0xFF);
            startPad = APPHistogramArithmetic.addToByteArray(startPad, java.math.BigInteger.ONE);
        }

        if (endInclusive) {
            endPad = APPHistogramKeySchema.rightPad(end, metadata.maxDepth(), (byte) 0xFF);
            endPad = APPHistogramArithmetic.addToByteArray(endPad, java.math.BigInteger.ONE);
        } else {
            endPad = APPHistogramKeySchema.rightPad(end, metadata.maxDepth(), (byte) 0x00);
        }

        return estimateRange(tr, startPad, endPad);
    }

    /**
     * Finds the first leaf that overlaps with the given key using reverse scan.
     */
    private APPHistogram.LeafInfo findFirstOverlappingLeaf(Transaction tr, byte[] keyPad) {
        // Reverse scan to find the boundary ≤ keyPad
        byte[] rangeBegin = APPHistogramKeySchema.leafBoundaryRangeBegin(subspace, keyPad);
        byte[] rangeEnd = APPHistogramKeySchema.leafBoundaryRangeEnd(subspace);

        var keyValues = tr.getRange(rangeEnd, rangeBegin, 1, true).asList().join();

        if (keyValues.isEmpty()) {
            return null; // No leaves found
        }

        // Decode the found boundary
        var keyValue = keyValues.get(0);
        byte[] boundaryKey = keyValue.getKey();

        var unpacked = subspace.unpack(boundaryKey);
        if (unpacked.size() < 2) {
            return null; // Invalid key format
        }

        byte[] leafId = (byte[]) unpacked.get(1);
        byte[] lowerBound = APPHistogramKeySchema.extractLowerBound(leafId);
        int depth = APPHistogramKeySchema.extractDepth(leafId);

        // Compute upper bound
        byte[] upperBound = APPHistogramArithmetic.calculateUpperBound(lowerBound, depth, metadata);

        // Check if this leaf is hot/sharded
        boolean isHotSharded = isLeafHotSharded(tr, lowerBound, depth);

        return new APPHistogram.LeafInfo(lowerBound, upperBound, depth, isHotSharded);
    }

    /**
     * Finds the next leaf after the given leaf.
     */
    private APPHistogram.LeafInfo findNextLeaf(Transaction tr, APPHistogram.LeafInfo currentLeaf) {
        // Find the next boundary after the current leaf's lower bound
        byte[] searchKey = APPHistogramArithmetic.addToByteArray(
                currentLeaf.lowerBound(), java.math.BigInteger.ONE);

        // Forward scan from current position to find next leaf
        byte[] rangeBegin = APPHistogramKeySchema.leafBoundaryKey(subspace, searchKey, 1);
        byte[] rangeEnd = APPHistogramKeySchema.leafBoundaryRangeBegin(
                subspace, APPHistogramArithmetic.createGlobalHigh(metadata.maxDepth()));

        var keyValues = tr.getRange(rangeBegin, rangeEnd, 1, false).asList().join();

        if (keyValues.isEmpty()) {
            return null; // No more leaves
        }

        // Decode the found boundary
        var keyValue = keyValues.get(0);
        byte[] boundaryKey = keyValue.getKey();

        var unpacked = subspace.unpack(boundaryKey);
        if (unpacked.size() < 2) {
            return null; // Invalid key format
        }

        byte[] leafId = (byte[]) unpacked.get(1);
        byte[] lowerBound = APPHistogramKeySchema.extractLowerBound(leafId);
        int depth = APPHistogramKeySchema.extractDepth(leafId);

        // Compute upper bound
        byte[] upperBound = APPHistogramArithmetic.calculateUpperBound(lowerBound, depth, metadata);

        // Check if this leaf is hot/sharded
        boolean isHotSharded = isLeafHotSharded(tr, lowerBound, depth);

        return new APPHistogram.LeafInfo(lowerBound, upperBound, depth, isHotSharded);
    }

    /**
     * Estimates the total count for a leaf by summing all its shards.
     */
    private long estimateLeafCount(Transaction tr, APPHistogram.LeafInfo leaf) {
        byte[] rangeBegin = APPHistogramKeySchema.leafCounterRangeBegin(subspace, leaf.lowerBound(), leaf.depth());
        byte[] rangeEnd = APPHistogramKeySchema.leafCounterRangeEnd(subspace, leaf.lowerBound(), leaf.depth());

        var keyValues = tr.getRange(rangeBegin, rangeEnd).asList().join();

        long total = 0;
        for (var kv : keyValues) {
            total += APPHistogramKeySchema.decodeCounterValue(kv.getValue());
        }

        return Math.max(0, total); // Ensure non-negative
    }

    /**
     * Checks if a leaf is marked as hot/sharded by examining its flags.
     */
    private boolean isLeafHotSharded(Transaction tr, byte[] lowerBound, int depth) {
        byte[] flagsKey = APPHistogramKeySchema.leafFlagsKey(subspace, lowerBound, depth);
        byte[] flagsData = tr.get(flagsKey).join();
        if (flagsData == null) {
            return false;
        }

        int flags = APPHistogramKeySchema.decodeFlags(flagsData);
        return (flags & APPHistogramKeySchema.HOT_SHARDED_FLAG) != 0;
    }

    /**
     * Gets the total estimated count across all leaves (useful for debugging).
     */
    public long estimateTotalCount(Transaction tr) {
        byte[] globalLow = APPHistogramArithmetic.createGlobalLow(metadata.maxDepth());
        byte[] globalHigh = APPHistogramArithmetic.createGlobalHigh(metadata.maxDepth());

        return (long) estimateRange(tr, globalLow, globalHigh);
    }
}