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

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Adaptive Prefix Partitioning (APP) histogram implementation for byte arrays on FoundationDB.
 * <p>
 * This implementation provides a lexicographic histogram for byte arrays with:
 * - Online split/merge maintenance (no background workers)
 * - Equal-width geometry with quartile splits (fanout=4)
 * - Exact recount on split to prevent drift
 * - Hysteresis split/merge thresholds to avoid oscillation
 * - Sharded counters for hotspot mitigation
 * - ACID transactional consistency
 * <p>
 * Key benefits:
 * - O(1) write operations using atomic ADD mutations
 * - Logarithmic depth tree structure
 * - Accurate range selectivity estimation
 * - No background maintenance threads required
 * - Deterministic geometry for predictable performance
 * <p>
 * Key Schema:
 * /stats/{bucketName}/{fieldName}/app_hist/
 * ├── L/<lowerPad><depthByte>                -> meta (leaf boundary record)
 * ├── C/<lowerPad><depthByte>/<shardId>     -> i64 (leaf counter shards)
 * ├── F/<lowerPad><depthByte>               -> flags (maintenance flags)
 * └── meta                                  -> JSON (metadata)
 */
public class APPHistogram {
    private final DirectorySubspace subspace;
    private final APPHistogramMetadata metadata;
    private final APPHistogramEstimator estimator;

    public APPHistogram(Transaction tr, List<String> root) {
        this.metadata = openMetadata(tr, root);
        this.subspace = openHistogramSubspace(tr, root);
        this.estimator = new APPHistogramEstimator(metadata, subspace);
    }

    public static void initialize(Transaction tr, List<String> root) {
        initialize(tr, root, APPHistogramMetadata.defaultMetadata());
    }

    public static void initialize(Transaction tr, List<String> root, APPHistogramMetadata metadata) {
        // Create the histogram subspace
        List<String> histogramSubpath = new ArrayList<>(root);
        histogramSubpath.addAll(Arrays.asList("statistics", "app_hist"));
        DirectorySubspace histogramSubspace = DirectoryLayer.getDefault().createOrOpen(tr, histogramSubpath).join();

        // Store metadata
        byte[] metaKey = APPHistogramKeySchema.metadataKey(histogramSubspace);
        byte[] metaValue = APPHistogramKeySchema.encodeMetadata(metadata);
        tr.set(metaKey, metaValue);

        // Initialize root leaf at depth 0 covering entire address space
        // This leaf spans from 0x00...00 to 0xFF...FF
        byte[] globalLow = APPHistogramArithmetic.createGlobalLow(metadata.maxDepth());
        byte[] rootBoundaryKey = APPHistogramKeySchema.leafBoundaryKey(histogramSubspace, globalLow, 0);
        byte[] rootMeta = APPHistogramKeySchema.encodeLeafMetadata(0);
        tr.set(rootBoundaryKey, rootMeta);

        // Initialize with zero count (shard 0)
        byte[] rootCounterKey = APPHistogramKeySchema.leafCounterKey(histogramSubspace, globalLow, 0, 0);
        tr.set(rootCounterKey, APPHistogramKeySchema.encodeCounterValue(0L));
    }

    private APPHistogramMetadata openMetadata(Transaction tr, List<String> root) {
        DirectorySubspace histogramSubspace = openHistogramSubspace(tr, root);
        byte[] metaData = tr.get(APPHistogramKeySchema.metadataKey(histogramSubspace)).join();
        return APPHistogramKeySchema.decodeMetadata(metaData);
    }

    private DirectorySubspace openHistogramSubspace(Transaction tr, List<String> root) {
        List<String> subpath = new ArrayList<>(root);
        subpath.addAll(Arrays.asList("statistics", "app_hist"));
        return DirectoryLayer.getDefault().open(tr, subpath).join();
    }

    /**
     * Adds a byte array value to the histogram within an existing transaction.
     */
    public void add(Transaction tr, byte[] value) {
        byte[] valuePad = APPHistogramKeySchema.rightPad(value, metadata.maxDepth(), (byte) 0x00);

        // Find the leaf that contains this value
        LeafInfo leaf = findLeaf(tr, valuePad);

        // Choose shard for this operation
        int shardId = chooseShard(value, leaf);

        // Atomically increment the counter
        byte[] counterKey = APPHistogramKeySchema.leafCounterKey(subspace, leaf.lowerBound, leaf.depth, shardId);
        tr.mutate(MutationType.ADD, counterKey, APPHistogramKeySchema.ONE_LE);

        // Check if split is needed (optional optimization)
        if (shouldCheckForSplit()) {
            long totalCount = estimateLeafCount(tr, leaf);
            if (totalCount >= metadata.splitThreshold()) {
                setMaintenanceFlag(tr, leaf, APPHistogramKeySchema.NEEDS_SPLIT_FLAG);
            }
        }
    }

    /**
     * Removes a byte array value from the histogram within an existing transaction.
     */
    public void delete(Transaction tr, byte[] value) {
        byte[] valuePad = APPHistogramKeySchema.rightPad(value, metadata.maxDepth(), (byte) 0x00);

        // Find the leaf that contains this value
        LeafInfo leaf = findLeaf(tr, valuePad);

        // Choose same shard as used for addition
        int shardId = chooseShard(value, leaf);

        // Atomically decrement the counter
        byte[] counterKey = APPHistogramKeySchema.leafCounterKey(subspace, leaf.lowerBound, leaf.depth, shardId);
        tr.mutate(MutationType.ADD, counterKey, APPHistogramKeySchema.NEGATIVE_ONE_LE);

        // Check if merge is needed (optional optimization)
        if (shouldCheckForMerge()) {
            long totalCount = estimateLeafCount(tr, leaf);
            if (totalCount <= metadata.mergeThreshold()) {
                setMaintenanceFlag(tr, leaf, APPHistogramKeySchema.NEEDS_MERGE_FLAG);
            }
        }
    }

    /**
     * Updates a value atomically (delete old + insert new in single transaction).
     */
    public void update(Transaction tr, byte[] oldValue, byte[] newValue) {
        if (Arrays.equals(oldValue, newValue)) {
            return; // No change needed
        }

        // Atomic delete old + insert new
        delete(tr, oldValue);
        add(tr, newValue);
    }

    /**
     * Finds the leaf that contains the given padded key using reverse scan.
     * Implementation of the findLeaf algorithm from APP specification.
     * <p>
     * Algorithm:
     * 1. Compute K_pad (already done by caller)
     * 2. Reverse scan one boundary ≤ K_pad
     * 3. Decode (L_pad, d) and compute U_pad = L_pad + S(d)
     * 4. Handle edge cases and return leaf info
     */
    private LeafInfo findLeaf(Transaction tr, byte[] keyPad) {
        // Reverse scan to find the boundary ≤ keyPad
        byte[] rangeBegin = APPHistogramKeySchema.leafBoundaryRangeBegin(subspace, keyPad);
        byte[] rangeEnd = APPHistogramKeySchema.leafBoundaryRangeEnd(subspace);

        // Perform a reverse scan with limit 1
        var keyValues = tr.getRange(rangeEnd, rangeBegin, 1, true).asList().join();

        if (keyValues.isEmpty()) {
            throw new IllegalStateException("No leaf boundary found - histogram may not be initialized properly");
        }

        // Decode the found boundary
        var keyValue = keyValues.get(0);
        byte[] boundaryKey = keyValue.getKey();

        // Extract leaf ID from the key (remove subspace prefix and LEAF_BOUNDARY_PREFIX)
        var unpacked = subspace.unpack(boundaryKey);
        if (unpacked.size() < 2) {
            throw new IllegalStateException("Invalid boundary key format");
        }

        byte[] leafId = (byte[]) unpacked.get(1);
        byte[] lowerBound = APPHistogramKeySchema.extractLowerBound(leafId);
        int depth = APPHistogramKeySchema.extractDepth(leafId);

        // Compute upper bound
        byte[] upperBound = APPHistogramArithmetic.calculateUpperBound(lowerBound, depth, metadata);

        // Verify that keyPad is actually within this leaf's bounds
        if (APPHistogramArithmetic.compareUnsigned(keyPad, lowerBound) < 0 ||
                APPHistogramArithmetic.compareUnsigned(keyPad, upperBound) >= 0) {

            // This is a rare concurrency edge case - try to find the next boundary
            var nextKeyValues = tr.getRange(boundaryKey, rangeBegin, 1, false).asList().join();
            if (!nextKeyValues.isEmpty()) {
                // Use the next boundary instead
                var nextKeyValue = nextKeyValues.get(0);
                byte[] nextBoundaryKey = nextKeyValue.getKey();

                var nextUnpacked = subspace.unpack(nextBoundaryKey);
                byte[] nextLeafId = (byte[]) nextUnpacked.get(1);
                lowerBound = APPHistogramKeySchema.extractLowerBound(nextLeafId);
                depth = APPHistogramKeySchema.extractDepth(nextLeafId);
                upperBound = APPHistogramArithmetic.calculateUpperBound(lowerBound, depth, metadata);
            } else {
                throw new IllegalStateException("Key " + Arrays.toString(keyPad) + " is outside histogram bounds");
            }
        }

        // Check if this leaf is hot/sharded
        boolean isHotSharded = isLeafHotSharded(tr, lowerBound, depth);

        return new LeafInfo(lowerBound, upperBound, depth, isHotSharded);
    }

    /**
     * Chooses the appropriate shard for a document reference.
     */
    private int chooseShard(byte[] value, LeafInfo leaf) {
        if (leaf.isHotSharded) {
            // Hash-based sharding for hot leaves
            return Math.abs(Arrays.hashCode(value)) % metadata.maxShardCount();
        } else {
            // Default shard 0 for non-hot leaves
            return 0;
        }
    }

    /**
     * Estimates the total count for a leaf by summing all its shards.
     */
    private long estimateLeafCount(Transaction tr, LeafInfo leaf) {
        byte[] rangeBegin = APPHistogramKeySchema.leafCounterRangeBegin(subspace, leaf.lowerBound, leaf.depth);
        byte[] rangeEnd = APPHistogramKeySchema.leafCounterRangeEnd(subspace, leaf.lowerBound, leaf.depth);

        var keyValues = tr.getRange(rangeBegin, rangeEnd).asList().join();

        long total = 0;
        for (var kv : keyValues) {
            total += APPHistogramKeySchema.decodeCounterValue(kv.getValue());
        }

        return total;
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
     * Sets a maintenance flag for a leaf.
     */
    private void setMaintenanceFlag(Transaction tr, LeafInfo leaf, int flag) {
        byte[] flagsKey = APPHistogramKeySchema.leafFlagsKey(subspace, leaf.lowerBound, leaf.depth);
        byte[] currentFlags = tr.get(flagsKey).join();
        int flags = APPHistogramKeySchema.decodeFlags(currentFlags);
        flags |= flag;
        tr.set(flagsKey, APPHistogramKeySchema.encodeFlags(flags));
    }

    /**
     * Determines if we should check for split conditions (optimization).
     */
    private boolean shouldCheckForSplit() {
        // Simple heuristic: check occasionally to avoid overhead
        return Math.random() < 0.1; // 10% chance
    }

    /**
     * Determines if we should check for merge conditions (optimization).
     */
    private boolean shouldCheckForMerge() {
        // Simple heuristic: check occasionally to avoid overhead
        return Math.random() < 0.1; // 10% chance
    }

    public APPHistogramMetadata getMetadata() {
        return metadata;
    }

    public APPHistogramEstimator getEstimator() {
        return estimator;
    }

    public DirectorySubspace getSubspace() {
        return subspace;
    }

    /**
     * Represents information about a leaf in the histogram.
     */
    public record LeafInfo(byte[] lowerBound, byte[] upperBound, int depth, boolean isHotSharded) {

        @Override
        @Nonnull
        public String toString() {
            return String.format("LeafInfo{bounds=[%s, %s), depth=%d, hotSharded=%s}",
                    Arrays.toString(lowerBound), Arrays.toString(upperBound), depth, isHotSharded);
        }
    }
}