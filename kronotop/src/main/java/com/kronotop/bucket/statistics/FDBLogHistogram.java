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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * FoundationDB-based log10 histogram implementation following LogHistogramDynamic2 design.
 * <p>
 * This implementation maintains two separate histograms:
 * - posHist: for positive values using log10 bucketing
 * - negHist: for negative values using log10 of magnitude (|v|)
 * - zeroCount: separate counter for zero values
 * <p>
 * Key benefits:
 * - O(1) write operations using atomic ADD mutations (no reads required)
 * - Correct handling of positive, negative, and zero values
 * - Efficient range reads for selectivity estimation
 * - Window management through background janitor tasks
 * - Hot key avoidance via sharded total counters
 * - ACID transactional consistency
 * <p>
 * Key Schema:
 * /stats/{bucketName}/{fieldName}/log10_hist/m/{m}/
 * pos/d/{d}/j/{j}         -> count (positive bucket counts)
 * pos/d/{d}/sum           -> count (positive decade sums)
 * pos/d/{d}/g/{g}         -> count (positive group sums)
 * pos/underflow_sum       -> count (positive underflow summary)
 * pos/overflow_sum        -> count (positive overflow summary)
 * neg/d/{d}/j/{j}         -> count (negative magnitude bucket counts)
 * neg/d/{d}/sum           -> count (negative magnitude decade sums)
 * neg/d/{d}/g/{g}         -> count (negative magnitude group sums)
 * neg/underflow_sum       -> count (negative magnitude underflow summary)
 * neg/overflow_sum        -> count (negative magnitude overflow summary)
 * zero_count              -> count (zero values)
 * total/{shardId}         -> count (total shards across all histograms)
 * meta                    -> JSON (metadata)
 */
public class FDBLogHistogram {

    private final DirectorySubspace subspace;
    private final HistogramMetadata metadata;

    public FDBLogHistogram(Transaction tr, List<String> root) {
        this.metadata = openMetadata(tr, root);
        this.subspace = openHistogramSubspace(tr, root, metadata);
    }

    public static void initialize(Transaction tr, List<String> root) {
        List<String> metaSubpath = new ArrayList<>(root);
        metaSubpath.addAll(Arrays.asList(
                "statistics", "log10_hist"
        ));
        DirectorySubspace metaSubspace = DirectoryLayer.getDefault().createOrOpen(tr, metaSubpath).join();
        byte[] metaKey = HistogramKeySchema.metadataKey(metaSubspace);
        HistogramMetadata defaultMetadata = HistogramMetadata.defaultMetadata();
        byte[] metaValue = HistogramKeySchema.encodeMetadata(HistogramMetadata.defaultMetadata());
        tr.set(metaKey, metaValue);

        List<String> histogramSubspace = new ArrayList<>(root);
        histogramSubspace.addAll(Arrays.asList("statistics", "log10_hist", "m", String.valueOf(defaultMetadata.m())));
        DirectoryLayer.getDefault().createOrOpen(tr, histogramSubspace).join();
    }

    private HistogramMetadata openMetadata(Transaction tr, List<String> root) {
        DirectorySubspace metaSubspace = openHistogramMetaSubspace(tr, root);
        byte[] metaData = tr.get(HistogramKeySchema.metadataKey(metaSubspace)).join();
        return HistogramKeySchema.decodeMetadata(metaData);
    }

    private DirectorySubspace openHistogramMetaSubspace(Transaction tr, List<String> root) {
        List<String> subpath = new ArrayList<>(root);
        subpath.addAll(Arrays.asList(
                "statistics", "log10_hist"
        ));
        return DirectoryLayer.getDefault().open(tr, subpath).join();
    }

    private DirectorySubspace openHistogramSubspace(Transaction tr, List<String> root, HistogramMetadata metadata) {
        List<String> subpath = new ArrayList<>(root);
        subpath.addAll(Arrays.asList(
                "statistics", "log10_hist", "m", String.valueOf(metadata.m())
        ));
        return DirectoryLayer.getDefault().open(tr, subpath).join();
    }

    /**
     * Adds a value within an existing transaction following LogHistogramDynamic2
     */
    public void addValue(Transaction tr, double value, HistogramMetadata metadata) {
        //DirectorySubspace subspace = getHistogramSubspace(tr, bucketName, fieldName, metadata.m());

        if (value == 0.0) {
            // Handle zero values separately
            tr.mutate(MutationType.ADD, HistogramKeySchema.zeroCountKey(subspace), HistogramKeySchema.ONE_LE);
            return;
        }

        // Determine histogram type and magnitude
        String histType;
        double magnitude;
        if (value > 0) {
            histType = HistogramKeySchema.POS_HIST_PREFIX;
            magnitude = value;
        } else {
            histType = HistogramKeySchema.NEG_HIST_PREFIX;
            magnitude = -value; // Use absolute value for negatives
        }

        // Calculate decade and sub-bucket using log10 of magnitude
        double log = Math.log10(magnitude);
        int decade = (int) Math.floor(log);
        int subBucket = bucketIndexWithinDecade(log, decade, metadata.m());
        int group = subBucket / metadata.groupSize();

        // Deterministic shard based on value
        int shard = computeShardId(value, metadata.shardCount());

        // Atomic ADD operations (no reads required)
        tr.mutate(MutationType.ADD, HistogramKeySchema.bucketCountKey(subspace, histType, decade, subBucket), HistogramKeySchema.ONE_LE);
        tr.mutate(MutationType.ADD, HistogramKeySchema.decadeSumKey(subspace, histType, decade), HistogramKeySchema.ONE_LE);
        tr.mutate(MutationType.ADD, HistogramKeySchema.groupSumKey(subspace, histType, decade, group), HistogramKeySchema.ONE_LE);
        tr.mutate(MutationType.ADD, HistogramKeySchema.totalShardKey(subspace, histType, shard), HistogramKeySchema.ONE_LE);
    }

    /**
     * Deletes a value using atomic ADD(-1) operations - exact inverse of insert
     */
    public void deleteValue(Transaction tr, double value, HistogramMetadata metadata) {
        //DirectorySubspace subspace = getHistogramSubspace(tr, bucketName, fieldName, metadata.m());

        if (value == 0.0) {
            // Handle zero values separately
            tr.mutate(MutationType.ADD, HistogramKeySchema.zeroCountKey(subspace), HistogramKeySchema.NEGATIVE_ONE_LE);
            return;
        }

        // Determine histogram type and magnitude
        String histType;
        double magnitude;
        if (value > 0) {
            histType = HistogramKeySchema.POS_HIST_PREFIX;
            magnitude = value;
        } else {
            histType = HistogramKeySchema.NEG_HIST_PREFIX;
            magnitude = -value; // Use absolute value for negatives
        }

        // Calculate decade and sub-bucket using log10 of magnitude
        double log = Math.log10(magnitude);
        int decade = (int) Math.floor(log);
        int subBucket = bucketIndexWithinDecade(log, decade, metadata.m());
        int group = subBucket / metadata.groupSize();

        // Deterministic shard based on value
        int shard = computeShardId(value, metadata.shardCount());

        // Atomic ADD(-1) operations - exact inverse of insert
        tr.mutate(MutationType.ADD, HistogramKeySchema.bucketCountKey(subspace, histType, decade, subBucket), HistogramKeySchema.NEGATIVE_ONE_LE);
        tr.mutate(MutationType.ADD, HistogramKeySchema.decadeSumKey(subspace, histType, decade), HistogramKeySchema.NEGATIVE_ONE_LE);
        tr.mutate(MutationType.ADD, HistogramKeySchema.groupSumKey(subspace, histType, decade, group), HistogramKeySchema.NEGATIVE_ONE_LE);
        tr.mutate(MutationType.ADD, HistogramKeySchema.totalShardKey(subspace, histType, shard), HistogramKeySchema.NEGATIVE_ONE_LE);
    }

    /**
     * Updates a value atomically (delete old + insert new in single transaction)
     */
    public void updateValue(Transaction tr, double oldValue, double newValue, HistogramMetadata metadata) {
        if (oldValue == newValue) {
            return; // No change needed
        }

        // Atomic delete old + insert new
        deleteValue(tr, oldValue, metadata);
        addValue(tr, newValue, metadata);
    }

    /**
     * Computes deterministic shard ID based on value hash
     */
    private int computeShardId(double value, int shardCount) {
        // Use Double.doubleToLongBits for deterministic hash of double values
        long hash = Double.doubleToLongBits(value);
        // Apply mask to ensure positive value, then mod by shard count
        return (int) ((hash & 0x7fffffffL) % shardCount);
    }

    /**
     * Creates histogram estimator for selectivity calculations
     */
    public HistogramEstimator createEstimator() {
        return new HistogramEstimator(metadata, subspace);
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