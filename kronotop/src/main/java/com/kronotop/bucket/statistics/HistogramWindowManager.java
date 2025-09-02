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
 * <p>
 * This class implements the background janitor approach to maintain the active decade window
 * while keeping write operations read-free. It periodically:
 * <p>
 * 1. Discovers active decades using range scans
 * 2. Evicts oldest decades when window size exceeds limit
 * 3. Migrates evicted decade sums to underflow/overflow summaries
 * 4. Clears evicted decade data using range operations
 * <p>
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
        // 1. Discover active decades across both histograms
        Set<Integer> activeDecades = getAllActiveDecades(tr, subspace);

        if (activeDecades.size() <= metadata.windowDecades()) {
            return; // No maintenance needed
        }

        logger.info("Maintaining window: found " + activeDecades.size() +
                " decades, limit is " + metadata.windowDecades());

        // 2. Sort decades and determine which to evict
        List<Integer> sortedDecades = new ArrayList<>(activeDecades);
        Collections.sort(sortedDecades);

        // 3. Evict oldest decades first from both histograms
        while (sortedDecades.size() > metadata.windowDecades()) {
            int evictDecade = sortedDecades.remove(0); // Remove oldest
            // Evict from both positive and negative histograms
            evictDecadeToSummary(tr, subspace, HistogramKeySchema.POS_HIST_PREFIX, evictDecade, false);
            evictDecadeToSummary(tr, subspace, HistogramKeySchema.NEG_HIST_PREFIX, evictDecade, false);
            logger.info("Evicted decade " + evictDecade + " from both histograms to underflow");
        }
    }

    /**
     * Discovers all active decades by scanning both positive and negative histogram entries
     */
    private Set<Integer> getActiveDecades(Transaction tr, DirectorySubspace subspace, String histType) {
        Set<Integer> decades = new HashSet<>();

        // Scan all histogram type entries to find active decades
        byte[] beginKey = subspace.pack(Tuple.from(histType, HistogramKeySchema.COUNTS_PREFIX));
        byte[] endKey = ByteArrayUtil.strinc(beginKey);
        AsyncIterable<KeyValue> entries = tr.getRange(beginKey, endKey);

        for (KeyValue kv : entries) {
            try {
                Tuple tuple = subspace.unpack(kv.getKey());
                if (tuple.size() >= 3 && histType.equals(tuple.getString(0)) &&
                        HistogramKeySchema.COUNTS_PREFIX.equals(tuple.getString(1))) {
                    // Handle both Integer and Long from tuple decoding
                    Number decadeNum = (Number) tuple.get(2);
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
     * Gets combined active decades from both histograms
     */
    private Set<Integer> getAllActiveDecades(Transaction tr, DirectorySubspace subspace) {
        Set<Integer> allDecades = new HashSet<>();
        allDecades.addAll(getActiveDecades(tr, subspace, HistogramKeySchema.POS_HIST_PREFIX));
        allDecades.addAll(getActiveDecades(tr, subspace, HistogramKeySchema.NEG_HIST_PREFIX));
        return allDecades;
    }

    /**
     * Evicts a decade from a specific histogram to summary and clears its data
     */
    private void evictDecadeToSummary(Transaction tr, DirectorySubspace subspace, String histType, int decade, boolean toOverflow) {
        // 1. Get decade sum for this histogram type
        byte[] decadeSumData = tr.get(HistogramKeySchema.decadeSumKey(subspace, histType, decade)).join();
        if (decadeSumData == null) {
            // No data for this decade in this histogram, skip silently
            return;
        }

        long decadeSum = HistogramKeySchema.decodeCounterValue(decadeSumData);
        if (decadeSum <= 0) {
            return; // Skip if zero sum
        }

        // 2. Add to appropriate summary using atomic ADD
        byte[] summaryDelta = HistogramKeySchema.encodeCounterValue(decadeSum);

        if (toOverflow) {
            tr.mutate(MutationType.ADD, HistogramKeySchema.overflowSumKey(subspace, histType), summaryDelta);
        } else {
            tr.mutate(MutationType.ADD, HistogramKeySchema.underflowSumKey(subspace, histType), summaryDelta);
        }

        // 3. Clear all decade entries for this histogram type using range clear
        byte[] decadeBegin = HistogramKeySchema.decadeRangeBegin(subspace, histType, decade);
        byte[] decadeEnd = HistogramKeySchema.decadeRangeEnd(subspace, histType, decade);
        tr.clear(decadeBegin, decadeEnd);

        logger.info("Evicted " + histType + " decade " + decade + " with sum " + decadeSum +
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
     * Gets statistics about active decades for monitoring across both histograms
     */
    public WindowStats getWindowStats(String bucketName, String fieldName, HistogramMetadata metadata) {
        try (Transaction tr = database.createTransaction()) {
            DirectorySubspace subspace = directoryLayer.createOrOpen(tr, Arrays.asList(
                    "stats", bucketName, fieldName, "log10_hist", "m", String.valueOf(metadata.m())
            )).join();

            Set<Integer> activeDecades = getAllActiveDecades(tr, subspace);

            // Get summary counts from both histograms
            long underflowSum = 0;
            byte[] posUnderflowData = tr.get(HistogramKeySchema.underflowSumKey(subspace, HistogramKeySchema.POS_HIST_PREFIX)).join();
            if (posUnderflowData != null) {
                underflowSum += HistogramKeySchema.decodeCounterValue(posUnderflowData);
            }
            byte[] negUnderflowData = tr.get(HistogramKeySchema.underflowSumKey(subspace, HistogramKeySchema.NEG_HIST_PREFIX)).join();
            if (negUnderflowData != null) {
                underflowSum += HistogramKeySchema.decodeCounterValue(negUnderflowData);
            }

            long overflowSum = 0;
            byte[] posOverflowData = tr.get(HistogramKeySchema.overflowSumKey(subspace, HistogramKeySchema.POS_HIST_PREFIX)).join();
            if (posOverflowData != null) {
                overflowSum += HistogramKeySchema.decodeCounterValue(posOverflowData);
            }
            byte[] negOverflowData = tr.get(HistogramKeySchema.overflowSumKey(subspace, HistogramKeySchema.NEG_HIST_PREFIX)).join();
            if (negOverflowData != null) {
                overflowSum += HistogramKeySchema.decodeCounterValue(negOverflowData);
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
    public record WindowStats(
            int activeDecadeCount,
            Integer minActiveDecade,
            Integer maxActiveDecade,
            long underflowSum,
            long overflowSum
    ) {
    }
}