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
 * Immutable metadata for log10-based histograms.
 * Contains all configuration parameters needed for histogram operations.
 */
public record HistogramMetadata(
        int m,                  // sub-buckets per decade
        int groupSize,          // group size for fast aggregation (m % groupSize == 0)
        int windowDecades,      // active decade window size (W)
        int shardCount,         // total counter sharding to avoid hot keys
        int version             // versioning for schema changes
) {

    public static final int DEFAULT_M = 16;
    public static final int DEFAULT_GROUP_SIZE = 4;
    public static final int DEFAULT_WINDOW_DECADES = 8;
    public static final int DEFAULT_SHARD_COUNT = 16;
    public static final int CURRENT_VERSION = 1;

    public HistogramMetadata {
        if (m <= 0) {
            throw new IllegalArgumentException("m must be > 0");
        }
        if (groupSize <= 0 || (m % groupSize) != 0) {
            throw new IllegalArgumentException("groupSize must divide m");
        }
        if (windowDecades <= 0) {
            throw new IllegalArgumentException("windowDecades must be > 0");
        }
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount must be > 0");
        }
        if (version <= 0) {
            throw new IllegalArgumentException("version must be > 0");
        }
    }

    /**
     * Creates default histogram metadata
     */
    public static HistogramMetadata defaultMetadata() {
        return new HistogramMetadata(
                DEFAULT_M,
                DEFAULT_GROUP_SIZE,
                DEFAULT_WINDOW_DECADES,
                DEFAULT_SHARD_COUNT,
                CURRENT_VERSION
        );
    }

    /**
     * Number of groups per decade
     */
    public int groupsPerDecade() {
        return m / groupSize;
    }
}