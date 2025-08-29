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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

/**
 * FoundationDB-based log10 histogram implementation.
 * 
 * This class implements the log10-dekad + sub-bucket histogram algorithm originally designed 
 * in LogHistogramDynamic, but adapted for FoundationDB storage with the following benefits:
 * 
 * - O(1) write operations using atomic ADD mutations (no reads required)
 * - Efficient range reads using FoundationDB's getScan pattern
 * - Window management through background janitor tasks 
 * - Hot key avoidance via sharded total counters
 * - ACID transactional consistency
 * 
 * Key Schema:
 * /stats/{bucketName}/{fieldName}/log10_hist/m/{m}/
 *   d/{d}/j/{j}         -> count (bucket counts)
 *   d/{d}/sum           -> count (decade sums)  
 *   d/{d}/g/{g}         -> count (group sums)
 *   total/{shardId}     -> count (total shards)
 *   underflow_sum       -> count (underflow summary)
 *   overflow_sum        -> count (overflow summary)
 *   zero_or_neg         -> count (zero/negative values)
 *   meta                -> JSON (metadata)
 */
public class FDBLogHistogram {
    
    private final Database database;
    private final DirectoryLayer directoryLayer;
    
    public FDBLogHistogram(Database database) {
        this(database, DirectoryLayer.getDefault());
    }
    
    public FDBLogHistogram(Database database, DirectoryLayer directoryLayer) {
        this.database = database;
        this.directoryLayer = directoryLayer;
    }
    
    /**
     * Initializes a histogram for the given bucket and field with specified metadata.
     * This should be called once during bucket/field setup.
     */
    public void initialize(String bucketName, String fieldName, HistogramMetadata metadata) {
        try (Transaction tr = database.createTransaction()) {
            // Store metadata at a path that doesn't depend on m value
            DirectorySubspace metaSubspace = getHistogramMetaSubspace(tr, bucketName, fieldName);
            byte[] metaKey = HistogramKeySchema.metadataKey(metaSubspace);
            byte[] metaValue = HistogramKeySchema.encodeMetadata(metadata);
            tr.set(metaKey, metaValue);
            
            tr.commit().join();
        }
    }
    
    /**
     * Adds a value to the histogram using atomic mutations (O(1), read-free).
     * 
     * For values <= 0: increments zeroOrNeg counter
     * For values > 0: calculates decade (d) and sub-bucket (j), then increments:
     *   - counts[d][j] 
     *   - decadeSum[d]
     *   - groupSum[d][g] where g = j / groupSize
     *   - totalShards[randomShard]
     */
    public void add(String bucketName, String fieldName, double value) {
        HistogramMetadata metadata = getMetadata(bucketName, fieldName);
        if (metadata == null) {
            metadata = HistogramMetadata.defaultMetadata();
            initialize(bucketName, fieldName, metadata);
        }
        
        try (Transaction tr = database.createTransaction()) {
            addValue(tr, bucketName, fieldName, value, metadata);
            tr.commit().join();
        }
    }
    
    /**
     * Adds a value within an existing transaction
     */
    public void addValue(Transaction tr, String bucketName, String fieldName, double value, HistogramMetadata metadata) {
        DirectorySubspace subspace = getHistogramSubspace(tr, bucketName, fieldName, metadata.m());
        
        if (value <= 0) {
            // Handle zero/negative values
            tr.mutate(MutationType.ADD, HistogramKeySchema.zeroOrNegKey(subspace), HistogramKeySchema.ONE_LE);
            
            // Add to random total shard
            int shard = ThreadLocalRandom.current().nextInt(metadata.shardCount());
            tr.mutate(MutationType.ADD, HistogramKeySchema.totalShardKey(subspace, shard), HistogramKeySchema.ONE_LE);
            return;
        }
        
        // Calculate decade and sub-bucket
        double log = Math.log10(value);
        int decade = (int) Math.floor(log);
        int subBucket = bucketIndexWithinDecade(log, decade, metadata.m());
        int group = subBucket / metadata.groupSize();
        
        // Atomic ADD operations (no reads required)
        tr.mutate(MutationType.ADD, HistogramKeySchema.bucketCountKey(subspace, decade, subBucket), HistogramKeySchema.ONE_LE);
        tr.mutate(MutationType.ADD, HistogramKeySchema.decadeSumKey(subspace, decade), HistogramKeySchema.ONE_LE);
        tr.mutate(MutationType.ADD, HistogramKeySchema.groupSumKey(subspace, decade, group), HistogramKeySchema.ONE_LE);
        
        // Add to random total shard to avoid hot keys
        int shard = ThreadLocalRandom.current().nextInt(metadata.shardCount());
        tr.mutate(MutationType.ADD, HistogramKeySchema.totalShardKey(subspace, shard), HistogramKeySchema.ONE_LE);
    }
    
    /**
     * Gets histogram metadata, returning null if not found
     */
    public HistogramMetadata getMetadata(String bucketName, String fieldName) {
        try (Transaction tr = database.createTransaction()) {
            DirectorySubspace metaSubspace = getHistogramMetaSubspace(tr, bucketName, fieldName);
            byte[] metaData = tr.get(HistogramKeySchema.metadataKey(metaSubspace)).join();
            return HistogramKeySchema.decodeMetadata(metaData);
        }
    }
    
    /**
     * Creates histogram estimator for selectivity calculations
     */
    public HistogramEstimator createEstimator(String bucketName, String fieldName) {
        return new HistogramEstimator(this, bucketName, fieldName);
    }
    
    /**
     * Creates window manager for background maintenance
     */
    public HistogramWindowManager createWindowManager() {
        return new HistogramWindowManager(database, directoryLayer);
    }
    
    /**
     * Gets the FoundationDB database instance
     */
    public Database getDatabase() {
        return database;
    }
    
    /**
     * Gets the directory layer instance
     */
    public DirectoryLayer getDirectoryLayer() {
        return directoryLayer;
    }
    
    /**
     * Gets or creates the histogram subspace for given parameters
     */
    public DirectorySubspace getHistogramSubspace(Transaction tr, String bucketName, String fieldName, int m) {
        return directoryLayer.createOrOpen(tr, Arrays.asList(
                "stats", bucketName, fieldName, "log10_hist", "m", String.valueOf(m)
        )).join();
    }
    
    /**
     * Gets or creates the histogram metadata subspace (independent of m parameter)
     */
    public DirectorySubspace getHistogramMetaSubspace(Transaction tr, String bucketName, String fieldName) {
        return directoryLayer.createOrOpen(tr, Arrays.asList(
                "stats", bucketName, fieldName, "log10_hist"
        )).join();
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