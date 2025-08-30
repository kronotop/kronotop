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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration metadata for Adaptive Prefix Partitioning (APP) histogram.
 * Defines parameters that control the histogram's behavior and performance characteristics.
 */
public record APPHistogramMetadata(
        @JsonProperty("maxDepth") int maxDepth,
        @JsonProperty("fanout") int fanout,
        @JsonProperty("splitThreshold") int splitThreshold,
        @JsonProperty("mergeThreshold") int mergeThreshold,
        @JsonProperty("shardCount") int shardCount
) {

    @JsonCreator
    public APPHistogramMetadata(
            @JsonProperty("maxDepth") int maxDepth,
            @JsonProperty("fanout") int fanout,
            @JsonProperty("splitThreshold") int splitThreshold,
            @JsonProperty("mergeThreshold") int mergeThreshold,
            @JsonProperty("shardCount") int shardCount) {
        this.maxDepth = maxDepth;
        this.fanout = fanout;
        this.splitThreshold = splitThreshold;
        this.mergeThreshold = mergeThreshold;
        this.shardCount = shardCount;
    }

    /**
     * Creates default APP histogram metadata with conservative parameters.
     */
    public static APPHistogramMetadata defaultMetadata() {
        return new APPHistogramMetadata(
                3,      // maxDepth: 3-byte address space
                4,      // fanout: quartile splits
                4096,   // splitThreshold: T_SPLIT
                1024,   // mergeThreshold: T_MERGE 
                1       // shardCount: start unsharded
        );
    }

    /**
     * Creates APP histogram metadata optimized for hot workloads.
     */
    public static APPHistogramMetadata hotWorkloadMetadata() {
        return new APPHistogramMetadata(
                4,      // maxDepth: allow deeper trees for hot data
                4,      // fanout: quartile splits
                8192,   // splitThreshold: higher threshold for hot keys
                2048,   // mergeThreshold: higher merge threshold
                8       // shardCount: pre-shard for hot workloads
        );
    }

    /**
     * Validates metadata parameters for correctness.
     */
    public void validate() {
        if (maxDepth < 1 || maxDepth > 8) {
            throw new IllegalArgumentException("maxDepth must be between 1 and 8, got: " + maxDepth);
        }
        if (fanout < 2 || fanout > 16) {
            throw new IllegalArgumentException("fanout must be between 2 and 16, got: " + fanout);
        }
        if (splitThreshold <= mergeThreshold) {
            throw new IllegalArgumentException("splitThreshold (" + splitThreshold + 
                    ") must be greater than mergeThreshold (" + mergeThreshold + ")");
        }
        if (shardCount < 1 || shardCount > 64) {
            throw new IllegalArgumentException("shardCount must be between 1 and 64, got: " + shardCount);
        }
        if ((shardCount & (shardCount - 1)) != 0) {
            throw new IllegalArgumentException("shardCount must be a power of 2, got: " + shardCount);
        }
    }

    /**
     * Returns the width of a single leaf at the given depth in the address space.
     */
    public long leafWidth(int depth) {
        if (depth < 1 || depth > maxDepth) {
            throw new IllegalArgumentException("depth must be between 1 and " + maxDepth + ", got: " + depth);
        }
        return (long) Math.pow(256, maxDepth - depth);
    }

    /**
     * Returns the width of a child leaf after splitting a parent at the given depth.
     */
    public long childWidth(int parentDepth) {
        return leafWidth(parentDepth) / fanout;
    }
}