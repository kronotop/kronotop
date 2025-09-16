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
 * Provides selectivity estimation using the FDB-based LogHistogramDynamic2 implementation.
 * <p>
 * This class implements the LogHistogramDynamic2 selectivity logic with separate positive
 * and negative histograms:
 * <p>
 * For P(field > threshold):
 * - If threshold > 0: use posHist.estimateGreaterThan(threshold) / totalCount
 * - If threshold = 0: count all positive values
 * - If threshold < 0: count all positives plus negatives with |v| < |threshold|
 * <p>
 * For P(a <= field < b): computed as P(field >= a) - P(field >= b)
 * <p>
 * Each histogram uses efficient individual key reads to avoid range scan issues.
 */
public class HistogramEstimator {

    private final DirectorySubspace subspace;
    private final HistogramMetadata metadata;

    HistogramEstimator(HistogramMetadata metadata, DirectorySubspace subspace) {
        this.subspace = subspace;
        this.metadata = metadata;
    }

    /**
     * Estimates P(field > threshold) within an existing transaction using LogHistogramDynamic2
     */
    public double estimateGreaterThan(Transaction tr, double threshold) {
        if (metadata == null) {
            return 0.0; // No data
        }

        // Get a total count across all histograms
        long totalCount = getTotalCount(tr, subspace, metadata.shardCount());
        if (totalCount == 0) {
            return 0.0;
        }

        long countAbove = 0;

        if (threshold > 0) {
            // Case 1: threshold > 0 - count positives greater than the threshold
            countAbove = estimateHistogramGreaterThan(tr, subspace, HistogramKeySchema.POS_HIST_PREFIX, threshold, metadata);

        } else if (threshold == 0) {
            // Case 2: threshold = 0 - count all positive values
            countAbove = getHistogramTotalCount(tr, subspace, HistogramKeySchema.POS_HIST_PREFIX);

        } else {
            // Case 3: threshold < 0 - count all positives + zeros + negatives with |v| < |threshold|
            // All positives count
            long positiveCount = getHistogramTotalCount(tr, subspace, HistogramKeySchema.POS_HIST_PREFIX);
            countAbove += positiveCount;

            // All zeros count (they are greater than any negative threshold)
            byte[] zeroData = tr.get(HistogramKeySchema.zeroCountKey(subspace)).join();
            if (zeroData != null) {
                countAbove += HistogramKeySchema.decodeCounterValue(zeroData);
            }

            // Negatives with |v| < |threshold| (i.e., values closer to zero than the threshold)
            // This means negative values with magnitude less than |threshold|
            double thresholdMagnitude = -threshold; // |threshold|
            long negLessThanThreshold = estimateHistogramLessThan(tr, subspace, thresholdMagnitude, metadata);
            countAbove += negLessThanThreshold;
        }

        return Math.max(0.0, Math.min(1.0, (double) countAbove / totalCount));
    }

    /**
     * Estimates P(a <= field < b) selectivity using LogHistogramDynamic2
     */
    public double estimateRange(Transaction tr, double a, double b) {
        if (a >= b) {
            return 0.0;
        }

        // P([a,b)) = P(field >= a) - P(field >= b)
        // For P(field >= x), we compute 1 - P(field < x) = 1 - P(field <= x-epsilon)
        // But it's easier to compute P(field > x-epsilon) directly

        // double eps = 1e-9;
        double geqA = estimateGreaterThanOrEqual(tr, a);
        double geqB = estimateGreaterThanOrEqual(tr, b);
        return Math.max(0.0, Math.min(1.0, geqA - geqB));
    }

    /**
     * Estimates P(field >= threshold)
     */
    private double estimateGreaterThanOrEqual(Transaction tr, double threshold) {
        // P(field >= threshold) = P(field > threshold - epsilon)
        double eps = 1e-12;
        return estimateGreaterThan(tr, threshold - eps);
    }

    /**
     * Estimates count > threshold for a specific histogram (pos or neg)
     */
    private long estimateHistogramGreaterThan(Transaction tr, DirectorySubspace subspace, String histType,
                                              double threshold, HistogramMetadata metadata) {
        double logT = Math.log10(threshold);
        int dT = (int) Math.floor(logT);
        int jT = bucketIndexWithinDecade(logT, dT, metadata.m());
        int gT = jT / metadata.groupSize();

        long countAbove = 0;

        // 1. Add overflow summary
        byte[] overflowData = tr.get(HistogramKeySchema.overflowSumKey(subspace, histType)).join();
        if (overflowData != null) {
            countAbove += HistogramKeySchema.decodeCounterValue(overflowData);
        }

        // 2. Add decade sums for d > dT
        for (int d = dT + 1; d <= dT + 20; d++) {
            byte[] decadeSumData = tr.get(HistogramKeySchema.decadeSumKey(subspace, histType, d)).join();
            if (decadeSumData != null) {
                countAbove += HistogramKeySchema.decodeCounterValue(decadeSumData);
            }
        }

        // 3. Add group sums for d == dT, g > gT
        for (int g = gT + 1; g < metadata.groupsPerDecade(); g++) {
            byte[] groupSumData = tr.get(HistogramKeySchema.groupSumKey(subspace, histType, dT, g)).join();
            if (groupSumData != null) {
                countAbove += HistogramKeySchema.decodeCounterValue(groupSumData);
            }
        }

        // 4. Add individual buckets for the same group, j > jT
        int groupStart = gT * metadata.groupSize();
        int groupEnd = Math.min(groupStart + metadata.groupSize() - 1, metadata.m() - 1);

        for (int j = Math.max(jT + 1, groupStart); j <= groupEnd; j++) {
            byte[] bucketData = tr.get(HistogramKeySchema.bucketCountKey(subspace, histType, dT, j)).join();
            if (bucketData != null) {
                countAbove += HistogramKeySchema.decodeCounterValue(bucketData);
            }
        }

        // 5. Partial contribution from j == jT bucket
        byte[] bucketData = tr.get(HistogramKeySchema.bucketCountKey(subspace, histType, dT, jT)).join();
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

        return countAbove;
    }

    /**
     * Estimates "count < threshold" for a specific histogram (for negative magnitude calculations)
     */
    private long estimateHistogramLessThan(Transaction tr, DirectorySubspace subspace, double threshold, HistogramMetadata metadata) {
        // Get the total for this histogram and subtract the >= portion
        long totalHist = getHistogramTotalCount(tr, subspace, HistogramKeySchema.NEG_HIST_PREFIX);
        long greaterEqual = estimateHistogramGreaterThan(tr, subspace, HistogramKeySchema.NEG_HIST_PREFIX, threshold, metadata);

        // Also need to account for values exactly equal to a threshold
        double logT = Math.log10(threshold);
        int dT = (int) Math.floor(logT);
        int jT = bucketIndexWithinDecade(logT, dT, metadata.m());

        byte[] bucketData = tr.get(HistogramKeySchema.bucketCountKey(subspace, HistogramKeySchema.NEG_HIST_PREFIX, dT, jT)).join();
        long equalCount = 0;
        if (bucketData != null) {
            long bucketCount = HistogramKeySchema.decodeCounterValue(bucketData);
            if (bucketCount > 0) {
                double lower = dT + (double) jT / metadata.m();
                double upper = dT + (double) (jT + 1) / metadata.m();
                double ratio = (upper > lower) ? (logT - lower) / (upper - lower) : 0.0;
                ratio = Math.max(0.0, Math.min(1.0, ratio));
                equalCount = Math.round(bucketCount * ratio);
            }
        }

        return totalHist - greaterEqual - equalCount;
    }

    /**
     * Gets total count for a specific histogram type (pos or neg)
     */
    private long getHistogramTotalCount(Transaction tr, DirectorySubspace subspace, String histType) {
        long total = 0;

        // Sum overflow and underflow
        byte[] overflowData = tr.get(HistogramKeySchema.overflowSumKey(subspace, histType)).join();
        if (overflowData != null) {
            total += HistogramKeySchema.decodeCounterValue(overflowData);
        }

        byte[] underflowData = tr.get(HistogramKeySchema.underflowSumKey(subspace, histType)).join();
        if (underflowData != null) {
            total += HistogramKeySchema.decodeCounterValue(underflowData);
        }

        // Sum all active decades (approximate by scanning reasonable range)
        for (int d = -10; d <= 10; d++) {
            byte[] decadeSumData = tr.get(HistogramKeySchema.decadeSumKey(subspace, histType, d)).join();
            if (decadeSumData != null) {
                total += HistogramKeySchema.decodeCounterValue(decadeSumData);
            }
        }

        return total;
    }

    /**
     * Gets total count from all shards (includes pos + neg + zero)
     */
    private long getTotalCount(Transaction tr, DirectorySubspace subspace, int shardCount) {
        long total = 0;

        // Sum positive histogram total shards
        for (int s = 0; s < shardCount; s++) {
            byte[] shardData = tr.get(HistogramKeySchema.totalShardKey(subspace, HistogramKeySchema.POS_HIST_PREFIX, s)).join();
            if (shardData != null) {
                total += HistogramKeySchema.decodeCounterValue(shardData);
            }
        }

        // Sum negative histogram total shards
        for (int s = 0; s < shardCount; s++) {
            byte[] shardData = tr.get(HistogramKeySchema.totalShardKey(subspace, HistogramKeySchema.NEG_HIST_PREFIX, s)).join();
            if (shardData != null) {
                total += HistogramKeySchema.decodeCounterValue(shardData);
            }
        }

        // Add zero counts
        byte[] zeroData = tr.get(HistogramKeySchema.zeroCountKey(subspace)).join();
        if (zeroData != null) {
            total += HistogramKeySchema.decodeCounterValue(zeroData);
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
}