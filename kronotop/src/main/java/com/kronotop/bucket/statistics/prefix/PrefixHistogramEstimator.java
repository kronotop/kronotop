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

package com.kronotop.bucket.statistics.prefix;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.Arrays;

/**
 * Provides selectivity estimation using the Fixed N-Byte Prefix Histogram.
 * <p>
 * Key features:
 * - Equality estimation with peek-first optimization
 * - Range estimation with (N+1)th byte fractioning
 * - Uses snapshot reads to avoid transaction conflicts
 */
public class PrefixHistogramEstimator {

    private final FDBPrefixHistogram histogram;
    private final String indexName;

    public PrefixHistogramEstimator(FDBPrefixHistogram histogram, String indexName) {
        this.histogram = histogram;
        this.indexName = indexName;
    }

    /**
     * Estimates equality selectivity P(field = v) using peek-first approach.
     * 
     * Flow:
     * 1. Try index peek first (up to PEEK_CAP)
     * 2. If small enough → return EXACT count
     * 3. If large → fall back to histogram estimation (APPROX)
     */
    public EstimateResult estimateEq(DirectorySubspace indexSubspace, byte[] value) {
        try (Transaction tr = histogram.getDatabase().createTransaction()) {
            return estimateEq(tr, indexSubspace, value);
        }
    }

    /**
     * Estimates equality within an existing transaction
     */
    public EstimateResult estimateEq(Transaction tr, DirectorySubspace indexSubspace, byte[] value) {
        PrefixHistogramMetadata metadata = histogram.getMetadata(indexName);
        if (metadata == null) {
            return new EstimateResult(0, EstimateType.EXACT);
        }

        // 1. Index peek (exact up to a cap)
        var valueSubspace = indexSubspace.subspace(Tuple.from((Object) value));
        byte[] beginKey = valueSubspace.range().begin;
        byte[] endKey = valueSubspace.range().end;
        
        long peekCount = 0;
        int limit = metadata.peekCap() + 1;
        
        for (var kv : tr.getRange(beginKey, endKey)) {
            peekCount++;
            if (peekCount > metadata.peekCap()) {
                break;
            }
        }
        
        // If within peek limit → EXACT
        if (peekCount <= metadata.peekCap()) {
            return new EstimateResult(peekCount, EstimateType.EXACT);
        }
        
        // 2. If limit hit → estimate via histogram
        // Estimate the range [v_pad00..., v_padFF..._next)
        byte[] vPad00 = PrefixHistogramUtils.pN(value, metadata.N());
        byte[] vPadFF = PrefixHistogramUtils.padFF(value, metadata.N());
        byte[] vNext = PrefixHistogramUtils.lexSuccessor(vPadFF);
        
        double estimate = estimateRange(tr, vPad00, vNext);
        return new EstimateResult(Math.round(estimate), EstimateType.APPROX);
    }

    /**
     * Estimates range selectivity P(a <= field < b) using histogram.
     * 
     * Algorithm:
     * 1. Compute prefix buckets: a = pN(A), b = pN(B)  
     * 2. Compute edge fractions from (N+1)th byte
     * 3. If same bucket: single bucket formula
     * 4. Else: sum contributions from edge + middle buckets
     */
    public double estimateRange(byte[] A, byte[] B) {
        try (Transaction tr = histogram.getDatabase().createTransaction()) {
            return estimateRange(tr, A, B);
        }
    }

    /**
     * Estimates range within an existing transaction
     */
    public double estimateRange(Transaction tr, byte[] A, byte[] B) {
        PrefixHistogramMetadata metadata = histogram.getMetadata(indexName);
        if (metadata == null) {
            return 0.0;
        }

        // Bucket IDs
        byte[] a = PrefixHistogramUtils.pN(A, metadata.N());
        byte[] b = PrefixHistogramUtils.pN(B, metadata.N());

        // Edge fractions (simple and fast)
        double fracLeft = PrefixHistogramUtils.fracLeft(A, metadata.N());
        double fracRight = PrefixHistogramUtils.fracRight(B, metadata.N());

        DirectorySubspace subspace = histogram.getHistogramSubspace(tr, indexName, metadata.N());

        // Single bucket case
        if (Arrays.equals(a, b)) {
            double ratio = PrefixHistogramUtils.clamp01(fracRight - (1.0 - fracLeft));
            long Ta = getBucketCount(tr, subspace, a);
            return Ta * ratio;
        }

        // Multi-bucket case
        long Ta = getBucketCount(tr, subspace, a);
        long Tb = getBucketCount(tr, subspace, b);
        long sumMid = getBucketCountRange(tr, subspace, 
                                         PrefixHistogramKeySchema.nextFixedN(a), b);

        return Ta * fracLeft + sumMid + Tb * fracRight;
    }

    /**
     * Gets bucket count for a specific prefix (0 if not found)
     */
    private long getBucketCount(Transaction tr, DirectorySubspace subspace, byte[] pN) {
        byte[] data = tr.get(PrefixHistogramKeySchema.bucketCountKey(subspace, pN)).join();
        return PrefixHistogramKeySchema.decodeCounterValue(data);
    }

    /**
     * Gets sum of bucket counts in range [beginPN, endPN) - exclusive end
     */
    private long getBucketCountRange(Transaction tr, DirectorySubspace subspace, 
                                   byte[] beginPN, byte[] endPN) {
        long sum = 0;
        
        byte[] beginKey = PrefixHistogramKeySchema.bucketCountRangeBegin(subspace, beginPN);
        byte[] endKey = PrefixHistogramKeySchema.bucketCountRangeBegin(subspace, endPN);
        
        for (var kv : tr.getRange(beginKey, endKey)) {
            sum += PrefixHistogramKeySchema.decodeCounterValue(kv.getValue());
        }
        
        return sum;
    }

    /**
     * Gets the index name
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Result of estimation operation
     */
    public static class EstimateResult {
        private final long count;
        private final EstimateType type;

        public EstimateResult(long count, EstimateType type) {
            this.count = count;
            this.type = type;
        }

        public long getCount() {
            return count;
        }

        public EstimateType getType() {
            return type;
        }

        public boolean isExact() {
            return type == EstimateType.EXACT;
        }

        @Override
        public String toString() {
            return String.format("EstimateResult{count=%d, type=%s}", count, type);
        }
    }

    /**
     * Type of estimate
     */
    public enum EstimateType {
        EXACT,   // Small set, peek returned exact count
        APPROX   // Large set, histogram-based approximation
    }
}