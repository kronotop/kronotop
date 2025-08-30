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

package com.kronotop.bucket.statistics.app;

/**
 * Configuration constants for Adaptive Prefix Partitioning (APP) histogram algorithm.
 * 
 * These defaults are based on the APP specification:
 * - MAX_DEPTH = 3 provides good balance for most workloads (optionally 4 for hot subtrees)
 * - FANOUT = 4 enables quartile splitting for deterministic geometry
 * - T_SPLIT/T_MERGE ratio of 4:1 provides hysteresis to avoid oscillation
 * - Sharded counters help mitigate hotspots on high-traffic leaves
 */
public final class APPConfiguration {
    
    /**
     * Maximum tree depth. Controls the finest granularity of partitioning.
     * Each level provides 256x more granular partitioning.
     * Depth 3 covers 256^3 = 16M distinct prefixes.
     */
    public static final int DEFAULT_MAX_DEPTH = 3;
    
    /**
     * Optional extended depth for very hot subtrees that continue hitting split thresholds.
     * Use sparingly to avoid excessive tree depth.
     */
    public static final int EXTENDED_MAX_DEPTH = 4;
    
    /**
     * Number of children created during split operation.
     * Fixed at 4 for quartile splitting geometry.
     */
    public static final int FANOUT = 4;
    
    /**
     * Split threshold - when leaf count reaches this value, split is triggered.
     * Sized to keep recount scan within FoundationDB transaction limits.
     */
    public static final int DEFAULT_T_SPLIT = 4096;
    
    /**
     * Merge threshold - when all 4 siblings total count drops below this, merge is triggered.
     * Set to 1/4 of T_SPLIT to provide hysteresis and avoid split-merge oscillation.
     */
    public static final int DEFAULT_T_MERGE = 1024;
    
    /**
     * Number of counter shards to create for hot leaves to reduce write contention.
     * Powers of 2 work best for hash-based shard selection.
     */
    public static final int DEFAULT_SHARD_COUNT = 8;
    
    /**
     * Global sentinel values for the address space boundaries.
     * LOW represents the minimum possible key (all zero bytes).
     * These are conceptual - actual implementation may only materialize real boundaries.
     */
    public static final class Sentinels {
        /**
         * Global low boundary - minimum possible key padded to MAX_DEPTH
         */
        public static byte[] globalLow(int maxDepth) {
            return new byte[maxDepth]; // All zeros
        }
        
        /**
         * Global high boundary - maximum possible key (conceptual upper fence)
         */
        public static byte[] globalHigh(int maxDepth) {
            byte[] high = new byte[maxDepth];
            java.util.Arrays.fill(high, (byte) 0xFF);
            return high;
        }
    }
    
    private APPConfiguration() {
        // Utility class
    }
}