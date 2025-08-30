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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Provides selectivity estimation for APP histogram using coverage-based interpolation.
 * Supports range queries [lower, upper) and equality predicates.
 */
public class APPEstimator {
    
    private final FDBAdaptivePrefixHistogram histogram;
    private final String bucketName;
    private final String fieldName;

    public APPEstimator(FDBAdaptivePrefixHistogram histogram, String bucketName, String fieldName) {
        this.histogram = histogram;
        this.bucketName = bucketName;
        this.fieldName = fieldName;
    }

    /**
     * Estimates selectivity for range query [lower, upper).
     * Returns the estimated number of matching rows.
     */
    public double estimateRange(byte[] lower, byte[] upper) {
        if (Arrays.compareUnsigned(lower, upper) >= 0) {
            return 0.0; // Invalid or empty range
        }

        APPHistogramMetadata metadata = histogram.getMetadata(bucketName, fieldName);
        if (metadata == null) {
            return 0.0; // No histogram available
        }

        try (Transaction tr = histogram.getDatabase().createTransaction()) {
            DirectorySubspace subspace = histogram.getHistogramSubspace(tr, bucketName, fieldName);
            return estimateRangeInternal(tr, subspace, lower, upper, metadata);
        }
    }

    /**
     * Estimates selectivity for equality predicate.
     * For byte arrays, this provides an approximation based on the containing leaf.
     */
    public double estimateEquality(byte[] value) {
        APPHistogramMetadata metadata = histogram.getMetadata(bucketName, fieldName);
        if (metadata == null) {
            return 0.0;
        }

        try (Transaction tr = histogram.getDatabase().createTransaction()) {
            DirectorySubspace subspace = histogram.getHistogramSubspace(tr, bucketName, fieldName);
            
            // Find the leaf containing this value
            APPLeaf leaf = histogram.findLeaf(tr, subspace, value, metadata);
            if (leaf == null) {
                return 0.0;
            }
            
            // Get the leaf count
            long leafCount = histogram.getLeafCount(tr, subspace, leaf, metadata);
            
            // Estimate assuming uniform distribution within the leaf
            long leafWidth = metadata.leafWidth(leaf.depth());
            return (double) leafCount / leafWidth;
        }
    }

    /**
     * Estimates selectivity for greater-than query (value > threshold).
     */
    public double estimateGreaterThan(byte[] threshold) {
        // Create a range from threshold+1 to maximum possible value
        byte[] upper = new byte[threshold.length];
        Arrays.fill(upper, (byte) 0xFF);
        
        // Increment threshold by 1 for exclusive comparison
        byte[] lower = incrementByteArray(threshold);
        
        return estimateRange(lower, upper);
    }

    /**
     * Estimates selectivity for less-than query (value < threshold).
     */
    public double estimateLessThan(byte[] threshold) {
        // Create a range from minimum possible value to threshold
        byte[] lower = new byte[threshold.length]; // All zeros
        
        return estimateRange(lower, threshold);
    }

    /**
     * Internal method that performs the actual range estimation.
     */
    private double estimateRangeInternal(Transaction tr, DirectorySubspace subspace, 
                                       byte[] lower, byte[] upper, APPHistogramMetadata metadata) {
        double totalEstimate = 0.0;
        
        // Canonicalize bounds
        byte[] lowerPad = APPKeySchema.canonicalizeLowerBound(lower, metadata.maxDepth());
        byte[] upperPad = APPKeySchema.canonicalizeLowerBound(upper, metadata.maxDepth());
        
        // Find first candidate leaf
        APPLeaf currentLeaf = histogram.findLeaf(tr, subspace, lower, metadata);
        
        // Iterate through all relevant leaves
        while (currentLeaf != null && Arrays.compareUnsigned(currentLeaf.lowerBound(), upperPad) < 0) {
            // Compute coverage ratio for this leaf
            double coverageRatio = currentLeaf.computeCoverageRatio(lowerPad, upperPad);
            
            if (coverageRatio > 0.0) {
                // Get leaf count and add weighted contribution
                long leafCount = histogram.getLeafCount(tr, subspace, currentLeaf, metadata);
                totalEstimate += leafCount * coverageRatio;
            }
            
            // Move to next leaf
            currentLeaf = findNextLeaf(tr, subspace, currentLeaf, metadata);
        }
        
        return totalEstimate;
    }

    /**
     * Finds the next leaf in lexicographic order.
     */
    private APPLeaf findNextLeaf(Transaction tr, DirectorySubspace subspace, 
                               APPLeaf currentLeaf, APPHistogramMetadata metadata) {
        // Find the next leaf boundary after the current leaf's upper bound
        byte[] searchKey = currentLeaf.upperBound();
        
        // Forward range scan to find next leaf boundary
        byte[] beginKey = APPKeySchema.leafLookupRangeBegin(subspace, searchKey);
        byte[] endKey = subspace.pack(Tuple.from(APPKeySchema.LEAF_PREFIX + "\u0000"));
        
        Range range = new Range(beginKey, endKey);
        List<KeyValue> results = tr.getRange(range, 1).asList().join(); // limit=1
        
        if (results.isEmpty()) {
            return null; // No more leaves
        }
        
        // Extract compound key from the result
        KeyValue kv = results.get(0);
        Tuple tuple = subspace.unpack(kv.getKey());
        byte[] compoundKey = (byte[]) tuple.get(1);
        
        return APPLeaf.fromCompoundKey(compoundKey, metadata.maxDepth());
    }

    /**
     * Increments a byte array by 1 for exclusive range operations.
     */
    private byte[] incrementByteArray(byte[] input) {
        byte[] result = Arrays.copyOf(input, input.length);
        
        // Add 1 to the byte array (big-endian style)
        for (int i = result.length - 1; i >= 0; i--) {
            result[i]++;
            if (result[i] != 0) {
                break; // No carry needed
            }
            // Continue with carry
        }
        
        return result;
    }

    /**
     * Gets the total count across all leaves (for debugging/validation).
     */
    public long getTotalCount() {
        APPHistogramMetadata metadata = histogram.getMetadata(bucketName, fieldName);
        if (metadata == null) {
            return 0;
        }

        try (Transaction tr = histogram.getDatabase().createTransaction()) {
            DirectorySubspace subspace = histogram.getHistogramSubspace(tr, bucketName, fieldName);
            return getTotalCountInternal(tr, subspace, metadata);
        }
    }

    /**
     * Internal method to compute total count across all leaves.
     */
    private long getTotalCountInternal(Transaction tr, DirectorySubspace subspace, APPHistogramMetadata metadata) {
        long totalCount = 0;
        
        // Scan all leaf boundaries
        byte[] beginKey = subspace.pack(Tuple.from(APPKeySchema.LEAF_PREFIX));
        byte[] endKey = subspace.pack(Tuple.from(APPKeySchema.LEAF_PREFIX + "\u0000"));
        
        Range range = new Range(beginKey, endKey);
        List<KeyValue> results = tr.getRange(range).asList().join();
        
        for (KeyValue kv : results) {
            Tuple tuple = subspace.unpack(kv.getKey());
            byte[] compoundKey = (byte[]) tuple.get(1);
            APPLeaf leaf = APPLeaf.fromCompoundKey(compoundKey, metadata.maxDepth());
            
            long leafCount = histogram.getLeafCount(tr, subspace, leaf, metadata);
            totalCount += leafCount;
        }
        
        return totalCount;
    }

    /**
     * Returns statistics about the histogram structure (for debugging).
     */
    public APPHistogramStats getHistogramStats() {
        APPHistogramMetadata metadata = histogram.getMetadata(bucketName, fieldName);
        if (metadata == null) {
            return null;
        }

        try (Transaction tr = histogram.getDatabase().createTransaction()) {
            DirectorySubspace subspace = histogram.getHistogramSubspace(tr, bucketName, fieldName);
            return computeHistogramStats(tr, subspace, metadata);
        }
    }

    private APPHistogramStats computeHistogramStats(Transaction tr, DirectorySubspace subspace, APPHistogramMetadata metadata) {
        int leafCount = 0;
        long totalItems = 0;
        int maxDepth = 0;
        int minDepth = Integer.MAX_VALUE;
        
        // Scan all leaf boundaries
        byte[] beginKey = subspace.pack(Tuple.from(APPKeySchema.LEAF_PREFIX));
        byte[] endKey = subspace.pack(Tuple.from(APPKeySchema.LEAF_PREFIX + "\u0000"));
        
        Range range = new Range(beginKey, endKey);
        List<KeyValue> results = tr.getRange(range).asList().join();
        
        for (KeyValue kv : results) {
            Tuple tuple = subspace.unpack(kv.getKey());
            byte[] compoundKey = (byte[]) tuple.get(1);
            APPLeaf leaf = APPLeaf.fromCompoundKey(compoundKey, metadata.maxDepth());
            
            leafCount++;
            totalItems += histogram.getLeafCount(tr, subspace, leaf, metadata);
            maxDepth = Math.max(maxDepth, leaf.depth());
            minDepth = Math.min(minDepth, leaf.depth());
        }
        
        if (minDepth == Integer.MAX_VALUE) {
            minDepth = 0;
        }
        
        return new APPHistogramStats(leafCount, totalItems, minDepth, maxDepth, metadata);
    }

    /**
     * Statistics about the histogram structure.
     */
    public static class APPHistogramStats {
        public final int leafCount;
        public final long totalItems;
        public final int minDepth;
        public final int maxDepth;
        public final APPHistogramMetadata metadata;

        public APPHistogramStats(int leafCount, long totalItems, int minDepth, int maxDepth, APPHistogramMetadata metadata) {
            this.leafCount = leafCount;
            this.totalItems = totalItems;
            this.minDepth = minDepth;
            this.maxDepth = maxDepth;
            this.metadata = metadata;
        }

        @Override
        public String toString() {
            return String.format("APPHistogramStats{leaves=%d, items=%d, depth=[%d,%d], metadata=%s}", 
                    leafCount, totalItems, minDepth, maxDepth, metadata);
        }
    }
}