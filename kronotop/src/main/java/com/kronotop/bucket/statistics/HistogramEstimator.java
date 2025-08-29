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
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

/**
 * Provides selectivity estimation using the FDB-based log10 histogram.
 * 
 * This class implements efficient range-based reads using FoundationDB's getScan pattern
 * to calculate selectivity estimates like P(field > threshold) and P(a <= field < b).
 * 
 * The estimation algorithm follows the original LogHistogramDynamic approach:
 * 1. Read overflow summary (single key)
 * 2. Scan decade sums for d > dT (range read)
 * 3. Scan group sums for d == dT, g > gT (range read)  
 * 4. Read individual buckets for same group as jT (few keys)
 * 5. Apply linear interpolation for partial bucket contribution
 */
public class HistogramEstimator {
    
    private final FDBLogHistogram histogram;
    private final String bucketName;
    private final String fieldName;
    
    public HistogramEstimator(FDBLogHistogram histogram, String bucketName, String fieldName) {
        this.histogram = histogram;
        this.bucketName = bucketName;
        this.fieldName = fieldName;
    }
    
    /**
     * Estimates P(field > threshold) selectivity
     */
    public double estimateGreaterThan(double threshold) {
        try (Transaction tr = histogram.getDatabase().createTransaction()) {
            return estimateGreaterThan(tr, threshold);
        }
    }
    
    /**
     * Estimates P(field > threshold) within an existing transaction
     */
    public double estimateGreaterThan(Transaction tr, double threshold) {
        HistogramMetadata metadata = histogram.getMetadata(bucketName, fieldName);
        if (metadata == null) {
            return 0.0; // No data
        }
        
        DirectorySubspace subspace = histogram.getHistogramSubspace(tr, bucketName, fieldName, metadata.m());
        
        // Get total count
        long totalCount = getTotalCount(tr, subspace, metadata.shardCount());
        if (totalCount == 0 || threshold <= 0) {
            return threshold <= 0 ? 1.0 : 0.0;
        }
        
        double logT = Math.log10(threshold);
        int dT = (int) Math.floor(logT);
        int jT = bucketIndexWithinDecade(logT, dT, metadata.m());
        int gT = jT / metadata.groupSize();
        
        long countAbove = 0;
        
        // 1. Add overflow summary (single key read)
        byte[] overflowData = tr.get(HistogramKeySchema.overflowSumKey(subspace)).join();
        if (overflowData != null) {
            countAbove += HistogramKeySchema.decodeCounterValue(overflowData);
        }
        
        // 2. Scan decade sums for d > dT (use individual reads instead of range scan for now)
        // Note: Range scan was including non-sum keys, so falling back to individual reads
        for (int d = dT + 1; d <= dT + 20; d++) { // Check reasonable range
            byte[] decadeSumData = tr.get(HistogramKeySchema.decadeSumKey(subspace, d)).join();
            if (decadeSumData != null) {
                long decadeSum = HistogramKeySchema.decodeCounterValue(decadeSumData);
                countAbove += decadeSum;
            }
        }
        
        // 3. Handle d == dT decade: use individual reads for group sums g > gT
        for (int g = gT + 1; g < metadata.groupsPerDecade(); g++) {
            byte[] groupSumData = tr.get(HistogramKeySchema.groupSumKey(subspace, dT, g)).join();
            if (groupSumData != null) {
                long groupSum = HistogramKeySchema.decodeCounterValue(groupSumData);
                countAbove += groupSum;
            }
        }
        
        // 4. Handle same group as jT: scan individual j > jT (few key reads)
        int groupStart = gT * metadata.groupSize();
        int groupEnd = Math.min(groupStart + metadata.groupSize() - 1, metadata.m() - 1);
        
        for (int j = Math.max(jT + 1, groupStart); j <= groupEnd; j++) {
            byte[] bucketData = tr.get(HistogramKeySchema.bucketCountKey(subspace, dT, j)).join();
            if (bucketData != null) {
                long count = HistogramKeySchema.decodeCounterValue(bucketData);
                countAbove += count;
            }
        }
        
        // 5. Partial contribution from j == jT bucket (linear interpolation in log space)
        byte[] bucketData = tr.get(HistogramKeySchema.bucketCountKey(subspace, dT, jT)).join();
        if (bucketData != null) {
            long bucketCount = HistogramKeySchema.decodeCounterValue(bucketData);
            if (bucketCount > 0) {
                double lower = dT + (double) jT / metadata.m();
                double upper = dT + (double) (jT + 1) / metadata.m();
                double ratio = (upper > lower) ? (upper - logT) / (upper - lower) : 0.0;
                ratio = Math.max(0.0, Math.min(1.0, ratio));
                countAbove += Math.round(bucketCount * ratio);
            }
        }
        
        return Math.max(0.0, Math.min(1.0, (double) countAbove / totalCount));
    }
    
    /**
     * Estimates P(a <= field < b) selectivity
     */
    public double estimateRange(double a, double b) {
        if (b <= 0 || a >= b) {
            return 0.0;
        }
        
        try (Transaction tr = histogram.getDatabase().createTransaction()) {
            // P([a,b)) = P(>=a) - P(>=b)
            double eps = 1e-12;
            double geA = estimateGreaterThan(tr, a - eps);
            double geB = estimateGreaterThan(tr, b - eps);
            return Math.max(0.0, Math.min(1.0, geA - geB));
        }
    }
    
    /**
     * Gets total count from all shards plus zero/negative count
     */
    private long getTotalCount(Transaction tr, DirectorySubspace subspace, int shardCount) {
        long total = 0;
        
        // Get zero/negative count
        byte[] zeroData = tr.get(HistogramKeySchema.zeroOrNegKey(subspace)).join();
        if (zeroData != null) {
            total += HistogramKeySchema.decodeCounterValue(zeroData);
        }
        
        // Sum all total shards
        for (int s = 0; s < shardCount; s++) {
            byte[] shardData = tr.get(HistogramKeySchema.totalShardKey(subspace, s)).join();
            if (shardData != null) {
                total += HistogramKeySchema.decodeCounterValue(shardData);
            }
        }
        
        return total;
    }
    
    /**
     * Calculates sub-bucket index within a decade (0 to m-1)
     */
    private int bucketIndexWithinDecade(double logValue, int decade, int m) {
        double frac = logValue - decade; // [0,1)
        int j = (int) Math.floor(m * frac);
        if (j < 0) j = 0;
        if (j >= m) j = m - 1;
        return j;
    }
    
    /**
     * Gets the bucket name
     */
    public String getBucketName() {
        return bucketName;
    }
    
    /**
     * Gets the field name
     */
    public String getFieldName() {
        return fieldName;
    }
}