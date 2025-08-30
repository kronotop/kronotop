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

import com.apple.foundationdb.*;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * FoundationDB-based Adaptive Prefix Partitioning (APP) histogram implementation.
 * 
 * This implementation maintains lexicographic histograms for byte array data:
 * - Deterministic geometric partitioning with configurable fanout
 * - Online split/merge maintenance on the write path
 * - Atomic counter operations for conflict-free concurrent updates
 * - Coverage-based selectivity estimation for range queries
 * 
 * Key Schema:
 * /stats/{bucketName}/{fieldName}/app_hist/
 * L/<lowerBoundPad><depthByte>         -> metadata (leaf boundary records)
 * C/<lowerBoundPad><depthByte>/<shardId> -> count (counter shards)
 * F/<lowerBoundPad><depthByte>         -> flags (maintenance flags)
 * meta                                 -> JSON (histogram metadata)
 */
public class FDBAdaptivePrefixHistogram {

    private final Database database;
    private final DirectoryLayer directoryLayer;
    private final APPMaintenanceOperations maintenanceOps;

    public FDBAdaptivePrefixHistogram(Database database) {
        this(database, DirectoryLayer.getDefault());
    }

    public FDBAdaptivePrefixHistogram(Database database, DirectoryLayer directoryLayer) {
        this.database = database;
        this.directoryLayer = directoryLayer;
        this.maintenanceOps = new APPMaintenanceOperations();
    }

    /**
     * Initializes a histogram for the given bucket and field with specified metadata.
     */
    public void initialize(String bucketName, String fieldName, APPHistogramMetadata metadata) {
        metadata.validate();
        
        try (Transaction tr = database.createTransaction()) {
            DirectorySubspace subspace = getHistogramSubspace(tr, bucketName, fieldName);
            
            // Store metadata
            byte[] metaKey = APPKeySchema.metadataKey(subspace);
            byte[] metaValue = APPKeySchema.encodeMetadata(metadata);
            tr.set(metaKey, metaValue);
            
            // Create initial sentinel leaves at depth 1
            byte[] lowSentinel = new byte[metadata.maxDepth()];  // All zeros
            byte[] highSentinel = new byte[metadata.maxDepth()]; // All 0xFF
            Arrays.fill(highSentinel, (byte) 0xFF);
            
            // Store the single leaf that covers the entire space [0x00...00, 0xFF...FF]
            byte[] leafKey = APPKeySchema.leafBoundaryKey(subspace, lowSentinel, 1);
            byte[] leafMeta = "{}".getBytes(); // Minimal metadata
            tr.set(leafKey, leafMeta);
            
            // Initialize counter shard for the root leaf
            byte[] counterKey = APPKeySchema.counterShardKey(subspace, lowSentinel, 1, 0);
            tr.set(counterKey, APPKeySchema.encodeCounterValue(0L));
            
            tr.commit().join();
        }
    }

    /**
     * Adds a value to the histogram using atomic mutations (O(1), read-free).
     */
    public void add(String bucketName, String fieldName, byte[] value) {
        APPHistogramMetadata metadata = getMetadata(bucketName, fieldName);
        if (metadata == null) {
            metadata = APPHistogramMetadata.defaultMetadata();
            initialize(bucketName, fieldName, metadata);
        }

        try (Transaction tr = database.createTransaction()) {
            addValue(tr, bucketName, fieldName, value, metadata);
            tr.commit().join();
        }
    }

    /**
     * Adds a value within an existing transaction.
     */
    public void addValue(Transaction tr, String bucketName, String fieldName, byte[] value, APPHistogramMetadata metadata) {
        DirectorySubspace subspace = getHistogramSubspace(tr, bucketName, fieldName);
        
        // Find the leaf that contains this value
        APPLeaf leaf = findLeaf(tr, subspace, value, metadata);
        
        // Determine shard based on value hash
        int shard = computeShardId(value, metadata.shardCount());
        
        // Atomic increment of the counter shard
        byte[] counterKey = APPKeySchema.counterShardKey(subspace, leaf.lowerBound(), leaf.depth(), shard);
        tr.mutate(MutationType.ADD, counterKey, APPKeySchema.ONE_LE);
        
        // Check if maintenance is needed (optional, can be deferred)
        checkSplitThreshold(tr, subspace, leaf, metadata);
    }

    /**
     * Deletes a value using atomic ADD(-1) operations.
     */
    public void deleteValue(Transaction tr, String bucketName, String fieldName, byte[] value, APPHistogramMetadata metadata) {
        DirectorySubspace subspace = getHistogramSubspace(tr, bucketName, fieldName);
        
        // Find the leaf that contains this value
        APPLeaf leaf = findLeaf(tr, subspace, value, metadata);
        
        // Determine shard based on value hash
        int shard = computeShardId(value, metadata.shardCount());
        
        // Atomic decrement of the counter shard
        byte[] counterKey = APPKeySchema.counterShardKey(subspace, leaf.lowerBound(), leaf.depth(), shard);
        tr.mutate(MutationType.ADD, counterKey, APPKeySchema.NEGATIVE_ONE_LE);
        
        // Check if maintenance is needed (optional, can be deferred)
        checkMergeThreshold(tr, subspace, leaf, metadata);
    }

    /**
     * Updates a value atomically (delete old + insert new in single transaction).
     */
    public void updateValue(Transaction tr, String bucketName, String fieldName, byte[] oldValue, byte[] newValue, APPHistogramMetadata metadata) {
        if (Arrays.equals(oldValue, newValue)) {
            return; // No change needed
        }
        
        deleteValue(tr, bucketName, fieldName, oldValue, metadata);
        addValue(tr, bucketName, fieldName, newValue, metadata);
    }

    /**
     * Finds the leaf that contains the given key using reverse range lookup.
     */
    public APPLeaf findLeaf(Transaction tr, DirectorySubspace subspace, byte[] key, APPHistogramMetadata metadata) {
        byte[] keyPad = APPKeySchema.canonicalizeLowerBound(key, metadata.maxDepth());
        
        // Reverse scan to find the greatest L_pad <= keyPad
        byte[] searchKey = APPKeySchema.createCompoundKey(keyPad, 0xFF); // Search with max depth byte
        byte[] beginKey = subspace.pack(Tuple.from(APPKeySchema.LEAF_PREFIX, searchKey));
        byte[] endKey = subspace.pack(Tuple.from(APPKeySchema.LEAF_PREFIX));
        
        Range range = new Range(endKey, beginKey); // Reversed
        List<KeyValue> results = tr.getRange(range, 1, true).asList().join(); // reverse=true, limit=1
        
        if (results.isEmpty()) {
            // Fallback: use forward scan to find any containing leaf
            return findLeafFallback(tr, subspace, key, metadata);
        }
        
        // Extract compound key from the result
        KeyValue kv = results.get(0);
        Tuple tuple = subspace.unpack(kv.getKey());
        byte[] compoundKey = (byte[]) tuple.get(1);
        
        APPLeaf leaf = APPLeaf.fromCompoundKey(compoundKey, metadata.maxDepth());
        
        // Verify the leaf actually contains the key
        if (!leaf.contains(keyPad)) {
            // Edge case: concurrent modification or boundary issues
            return findLeafFallback(tr, subspace, key, metadata);
        }
        
        return leaf;
    }
    
    /**
     * Fallback leaf lookup using forward scan (less efficient but reliable).
     */
    private APPLeaf findLeafFallback(Transaction tr, DirectorySubspace subspace, byte[] key, APPHistogramMetadata metadata) {
        byte[] keyPad = APPKeySchema.canonicalizeLowerBound(key, metadata.maxDepth());
        
        byte[] beginKey = subspace.pack(Tuple.from(APPKeySchema.LEAF_PREFIX));
        byte[] endKey = subspace.pack(Tuple.from(APPKeySchema.LEAF_PREFIX + "\u0000"));
        
        Range range = new Range(beginKey, endKey);
        List<KeyValue> results = tr.getRange(range).asList().join();
        
        APPLeaf bestMatch = null;
        
        for (KeyValue kv : results) {
            Tuple tuple = subspace.unpack(kv.getKey());
            byte[] compoundKey = (byte[]) tuple.get(1);
            APPLeaf candidate = APPLeaf.fromCompoundKey(compoundKey, metadata.maxDepth());
            
            // Check if this leaf contains the key
            if (candidate.contains(keyPad)) {
                // If we already have a match, pick the one at greater depth (more specific)
                if (bestMatch == null || candidate.depth() > bestMatch.depth()) {
                    bestMatch = candidate;
                }
            }
        }
        
        if (bestMatch == null) {
            throw new IllegalStateException("No leaf found for key: " + Arrays.toString(key));
        }
        
        return bestMatch;
    }

    /**
     * Gets the total count for a leaf by summing all its counter shards.
     */
    public long getLeafCount(Transaction tr, DirectorySubspace subspace, APPLeaf leaf, APPHistogramMetadata metadata) {
        long totalCount = 0;
        
        // Read all counter shards for this leaf
        for (int shard = 0; shard < metadata.shardCount(); shard++) {
            byte[] counterKey = APPKeySchema.counterShardKey(subspace, leaf.lowerBound(), leaf.depth(), shard);
            byte[] countBytes = tr.get(counterKey).join();
            totalCount += APPKeySchema.decodeCounterValue(countBytes);
        }
        
        return totalCount;
    }

    /**
     * Checks if a leaf needs to be split and sets maintenance flag if needed.
     */
    private void checkSplitThreshold(Transaction tr, DirectorySubspace subspace, APPLeaf leaf, APPHistogramMetadata metadata) {
        if (leaf.depth() >= metadata.maxDepth()) {
            return; // Cannot split at maximum depth
        }
        
        // Quick estimate using limited shard reads to avoid heavy operations
        long estimatedCount = getLeafCount(tr, subspace, leaf, metadata);
        
        if (estimatedCount >= metadata.splitThreshold()) {
            // Set maintenance flag for later processing
            byte[] flagKey = APPKeySchema.maintenanceFlagsKey(subspace, leaf.lowerBound(), leaf.depth());
            int currentFlags = APPKeySchema.decodeFlags(tr.get(flagKey).join());
            int newFlags = currentFlags | APPKeySchema.FLAG_NEEDS_SPLIT;
            tr.set(flagKey, APPKeySchema.encodeFlags(newFlags));
        }
    }

    /**
     * Checks if a leaf needs to be merged and sets maintenance flag if needed.
     */
    private void checkMergeThreshold(Transaction tr, DirectorySubspace subspace, APPLeaf leaf, APPHistogramMetadata metadata) {
        if (leaf.depth() <= 1) {
            return; // Cannot merge root level
        }
        
        long estimatedCount = getLeafCount(tr, subspace, leaf, metadata);
        
        if (estimatedCount <= metadata.mergeThreshold()) {
            // Set maintenance flag for later processing
            byte[] flagKey = APPKeySchema.maintenanceFlagsKey(subspace, leaf.lowerBound(), leaf.depth());
            int currentFlags = APPKeySchema.decodeFlags(tr.get(flagKey).join());
            int newFlags = currentFlags | APPKeySchema.FLAG_NEEDS_MERGE;
            tr.set(flagKey, APPKeySchema.encodeFlags(newFlags));
        }
    }

    /**
     * Performs maintenance operations (split/merge) for flagged leaves.
     */
    public void performMaintenance(String bucketName, String fieldName) {
        APPHistogramMetadata metadata = getMetadata(bucketName, fieldName);
        if (metadata == null) {
            return;
        }

        try (Transaction tr = database.createTransaction()) {
            DirectorySubspace subspace = getHistogramSubspace(tr, bucketName, fieldName);
            maintenanceOps.processPendingMaintenance(tr, subspace, metadata);
            tr.commit().join();
        }
    }

    /**
     * Computes deterministic shard ID based on value hash.
     */
    private int computeShardId(byte[] value, int shardCount) {
        int hash = Arrays.hashCode(value);
        return (hash & 0x7fffffff) % shardCount; // Ensure positive
    }

    /**
     * Gets histogram metadata, returning null if not found.
     */
    public APPHistogramMetadata getMetadata(String bucketName, String fieldName) {
        try (Transaction tr = database.createTransaction()) {
            DirectorySubspace subspace = getHistogramSubspace(tr, bucketName, fieldName);
            byte[] metaData = tr.get(APPKeySchema.metadataKey(subspace)).join();
            return APPKeySchema.decodeMetadata(metaData);
        }
    }

    /**
     * Creates estimator for selectivity calculations.
     */
    public APPEstimator createEstimator(String bucketName, String fieldName) {
        return new APPEstimator(this, bucketName, fieldName);
    }

    /**
     * Gets the FoundationDB database instance.
     */
    public Database getDatabase() {
        return database;
    }

    /**
     * Gets the directory layer instance.
     */
    public DirectoryLayer getDirectoryLayer() {
        return directoryLayer;
    }

    /**
     * Gets or creates the histogram subspace for given parameters.
     */
    public DirectorySubspace getHistogramSubspace(Transaction tr, String bucketName, String fieldName) {
        return directoryLayer.createOrOpen(tr, Arrays.asList(
                "stats", bucketName, fieldName, "app_hist"
        )).join();
    }

    /**
     * Public method to access findLeaf for estimator.
     */
    public APPLeaf findLeaf(String bucketName, String fieldName, byte[] key) {
        APPHistogramMetadata metadata = getMetadata(bucketName, fieldName);
        if (metadata == null) {
            return null;
        }

        try (Transaction tr = database.createTransaction()) {
            DirectorySubspace subspace = getHistogramSubspace(tr, bucketName, fieldName);
            return findLeaf(tr, subspace, key, metadata);
        }
    }

    /**
     * Public method to access leaf count for estimator.
     */
    public long getLeafCount(String bucketName, String fieldName, APPLeaf leaf) {
        APPHistogramMetadata metadata = getMetadata(bucketName, fieldName);
        if (metadata == null) {
            return 0;
        }

        try (Transaction tr = database.createTransaction()) {
            DirectorySubspace subspace = getHistogramSubspace(tr, bucketName, fieldName);
            return getLeafCount(tr, subspace, leaf, metadata);
        }
    }
}