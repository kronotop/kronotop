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
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;

import java.util.*;
import java.util.logging.Logger;

/**
 * Manages histogram window size and performs background maintenance.
 * 
 * This class implements the background janitor approach to maintain the active decade window
 * while keeping write operations read-free. It periodically:
 * 
 * 1. Discovers active decades using range scans
 * 2. Evicts oldest decades when window size exceeds limit  
 * 3. Migrates evicted decade sums to underflow/overflow summaries
 * 4. Clears evicted decade data using range operations
 * 
 * The window manager runs as a background task to avoid impacting write performance.
 */
public class HistogramWindowManager {
    
    private static final Logger logger = Logger.getLogger(HistogramWindowManager.class.getName());
    
    private final Database database;
    private final DirectoryLayer directoryLayer;
    
    public HistogramWindowManager(Database database, DirectoryLayer directoryLayer) {
        this.database = database;
        this.directoryLayer = directoryLayer;
    }
    
    /**
     * Maintains window size for a specific histogram.
     * This method should be called periodically by a background task.
     */
    public void maintainWindow(String bucketName, String fieldName, HistogramMetadata metadata) {
        try (Transaction tr = database.createTransaction()) {
            DirectorySubspace subspace = directoryLayer.createOrOpen(tr, Arrays.asList(
                    "stats", bucketName, fieldName, "log10_hist", "m", String.valueOf(metadata.m())
            )).join();
            
            maintainWindow(tr, subspace, metadata);
            tr.commit().join();
            
        } catch (Exception e) {
            logger.warning("Failed to maintain window for " + bucketName + "." + fieldName + ": " + e.getMessage());
        }
    }
    
    /**
     * Maintains window size within an existing transaction
     */
    public void maintainWindow(Transaction tr, DirectorySubspace subspace, HistogramMetadata metadata) {
        // 1. Discover active decades
        Set<Integer> activeDecades = getActiveDecades(tr, subspace);
        
        if (activeDecades.size() <= metadata.windowDecades()) {
            return; // No maintenance needed
        }
        
        logger.info("Maintaining window: found " + activeDecades.size() + 
                   " decades, limit is " + metadata.windowDecades());
        
        // 2. Sort decades and determine which to evict
        List<Integer> sortedDecades = new ArrayList<>(activeDecades);
        Collections.sort(sortedDecades);
        
        // 3. Evict oldest decades first (can be enhanced with better heuristics)
        while (sortedDecades.size() > metadata.windowDecades()) {
            int evictDecade = sortedDecades.remove(0); // Remove oldest
            evictDecadeToSummary(tr, subspace, evictDecade, false); // to underflow
            logger.info("Evicted decade " + evictDecade + " to underflow");
        }
    }
    
    /**
     * Discovers all active decades by scanning decade entries
     */
    private Set<Integer> getActiveDecades(Transaction tr, DirectorySubspace subspace) {
        Set<Integer> decades = new HashSet<>();
        
        // Scan all "d" entries to find active decades
        byte[] beginKey = subspace.pack(Tuple.from(HistogramKeySchema.COUNTS_PREFIX));
        byte[] endKey = ByteArrayUtil.strinc(beginKey);
        AsyncIterable<KeyValue> entries = tr.getRange(beginKey, endKey);
        
        for (KeyValue kv : entries) {
            try {
                Tuple tuple = subspace.unpack(kv.getKey());
                if (tuple.size() >= 2 && HistogramKeySchema.COUNTS_PREFIX.equals(tuple.getString(0))) {
                    // Handle both Integer and Long from tuple decoding
                    Number decadeNum = (Number) tuple.get(1);
                    Integer decade = decadeNum.intValue();
                    decades.add(decade);
                }
            } catch (Exception e) {
                // Skip malformed keys
                logger.warning("Skipping malformed key during decade discovery: " + e.getMessage());
            }
        }
        
        return decades;
    }
    
    /**
     * Evicts a decade to summary and clears its data
     */
    private void evictDecadeToSummary(Transaction tr, DirectorySubspace subspace, int decade, boolean toOverflow) {
        // 1. Get decade sum
        byte[] decadeSumData = tr.get(HistogramKeySchema.decadeSumKey(subspace, decade)).join();
        if (decadeSumData == null) {
            logger.warning("No decade sum found for decade " + decade);
            return;
        }
        
        long decadeSum = HistogramKeySchema.decodeCounterValue(decadeSumData);
        if (decadeSum <= 0) {
            logger.info("Decade " + decade + " has zero sum, skipping");
            return;
        }
        
        // 2. Add to appropriate summary using atomic ADD
        String summaryKey = toOverflow ? HistogramKeySchema.OVERFLOW_KEY : HistogramKeySchema.UNDERFLOW_KEY;
        byte[] summaryDelta = HistogramKeySchema.encodeCounterValue(decadeSum);
        
        if (toOverflow) {
            tr.mutate(MutationType.ADD, HistogramKeySchema.overflowSumKey(subspace), summaryDelta);
        } else {
            tr.mutate(MutationType.ADD, HistogramKeySchema.underflowSumKey(subspace), summaryDelta);
        }
        
        // 3. Clear all decade entries using range clear
        byte[] decadeBegin = HistogramKeySchema.decadeRangeBegin(subspace, decade);
        byte[] decadeEnd = HistogramKeySchema.decadeRangeEnd(subspace, decade);
        tr.clear(decadeBegin, decadeEnd);
        
        logger.info("Evicted decade " + decade + " with sum " + decadeSum + 
                   " to " + (toOverflow ? "overflow" : "underflow"));
    }
    
    /**
     * Maintains windows for all histograms in a bucket.
     * This is a convenience method for bucket-level maintenance.
     */
    public void maintainBucketWindows(String bucketName, List<String> fieldNames, HistogramMetadata metadata) {
        for (String fieldName : fieldNames) {
            maintainWindow(bucketName, fieldName, metadata);
        }
    }
    
    /**
     * Gets statistics about active decades for monitoring
     */
    public WindowStats getWindowStats(String bucketName, String fieldName, HistogramMetadata metadata) {
        try (Transaction tr = database.createTransaction()) {
            DirectorySubspace subspace = directoryLayer.createOrOpen(tr, Arrays.asList(
                    "stats", bucketName, fieldName, "log10_hist", "m", String.valueOf(metadata.m())
            )).join();
            
            Set<Integer> activeDecades = getActiveDecades(tr, subspace);
            
            // Get summary counts
            long underflowSum = 0;
            byte[] underflowData = tr.get(HistogramKeySchema.underflowSumKey(subspace)).join();
            if (underflowData != null) {
                underflowSum = HistogramKeySchema.decodeCounterValue(underflowData);
            }
            
            long overflowSum = 0;
            byte[] overflowData = tr.get(HistogramKeySchema.overflowSumKey(subspace)).join();
            if (overflowData != null) {
                overflowSum = HistogramKeySchema.decodeCounterValue(overflowData);
            }
            
            return new WindowStats(
                    activeDecades.size(),
                    activeDecades.isEmpty() ? null : Collections.min(activeDecades),
                    activeDecades.isEmpty() ? null : Collections.max(activeDecades),
                    underflowSum,
                    overflowSum
            );
            
        } catch (Exception e) {
            logger.warning("Failed to get window stats for " + bucketName + "." + fieldName + ": " + e.getMessage());
            return new WindowStats(0, null, null, 0, 0);
        }
    }
    
    /**
     * Statistics about histogram window state
     */
    public static record WindowStats(
            int activeDecadeCount,
            Integer minActiveDecade, 
            Integer maxActiveDecade,
            long underflowSum,
            long overflowSum
    ) {}
}