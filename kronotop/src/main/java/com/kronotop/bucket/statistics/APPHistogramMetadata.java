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

/**
 * Immutable metadata for Adaptive Prefix Partitioning (APP) histogram.
 * Contains all configuration parameters needed for APP operations.
 *
 * APP maintains a lexicographic histogram for byte arrays with:
 * - Online split/merge maintenance (no background workers)
 * - Equal-width geometry with quartile splits
 * - Exact recount on split to prevent drift
 * - Hysteresis split/merge thresholds to avoid oscillation
 */
public record APPHistogramMetadata(
        int maxDepth,           // Maximum tree depth (D_max) - typically 3 or 4
        int fanout,             // Split fanout factor - fixed at 4 for quartile splits
        int splitThreshold,     // T_SPLIT - trigger split when count >= this
        int mergeThreshold,     // T_MERGE - trigger merge when count <= this
        int maxShardCount,      // Maximum number of counter shards for hot leaves
        int version             // Versioning for schema changes
) {

    public static final int DEFAULT_MAX_DEPTH = 3;
    public static final int DEFAULT_FANOUT = 4;
    public static final int DEFAULT_SPLIT_THRESHOLD = 4096;
    public static final int DEFAULT_MERGE_THRESHOLD = 1024;
    public static final int DEFAULT_MAX_SHARD_COUNT = 8;
    public static final int CURRENT_VERSION = 1;

    public APPHistogramMetadata {
        if (maxDepth <= 0 || maxDepth > 6) {
            throw new IllegalArgumentException("maxDepth must be in range [1, 6]");
        }
        if (fanout != 4) {
            throw new IllegalArgumentException("fanout must be 4 for APP implementation");
        }
        if (splitThreshold <= mergeThreshold) {
            throw new IllegalArgumentException("splitThreshold must be > mergeThreshold for hysteresis");
        }
        if (mergeThreshold <= 0) {
            throw new IllegalArgumentException("mergeThreshold must be > 0");
        }
        if (maxShardCount <= 0 || maxShardCount > 256) {
            throw new IllegalArgumentException("maxShardCount must be in range [1, 256]");
        }
        if (version <= 0) {
            throw new IllegalArgumentException("version must be > 0");
        }

        // Validate hysteresis ratio (4:1 recommended)
        double hysteresisRatio = (double) splitThreshold / mergeThreshold;
        if (hysteresisRatio < 2.0) {
            throw new IllegalArgumentException("splitThreshold/mergeThreshold ratio should be >= 2.0 to avoid oscillation");
        }
    }

    /**
     * Creates default APP histogram metadata
     */
    public static APPHistogramMetadata defaultMetadata() {
        return new APPHistogramMetadata(
                DEFAULT_MAX_DEPTH,
                DEFAULT_FANOUT,
                DEFAULT_SPLIT_THRESHOLD,
                DEFAULT_MERGE_THRESHOLD,
                DEFAULT_MAX_SHARD_COUNT,
                CURRENT_VERSION
        );
    }

    /**
     * Calculates the width of a leaf at given depth.
     * S(d) = 256^(D_max - d)
     */
    public long leafWidth(int depth) {
        if (depth < 0 || depth > maxDepth) {
            throw new IllegalArgumentException("depth must be in range [0, " + maxDepth + "]");
        }
        return (long) Math.pow(256, maxDepth - depth);
    }

    /**
     * Calculates the parent leaf width for a given depth.
     * S_parent = fanout * S(d)
     */
    public long parentWidth(int depth) {
        return fanout * leafWidth(depth);
    }

    /**
     * Gets the hysteresis ratio between split and merge thresholds
     */
    public double hysteresisRatio() {
        return (double) splitThreshold / mergeThreshold;
    }

    /**
     * Checks if the given depth is at the maximum allowed depth
     */
    public boolean isMaxDepth(int depth) {
        return depth >= maxDepth;
    }
}