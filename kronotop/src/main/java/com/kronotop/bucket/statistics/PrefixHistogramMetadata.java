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
 * Immutable metadata for prefix-based histograms with fixed depth.
 * Used for approximate range selectivity estimation over string or binary keys.
 */
public record PrefixHistogramMetadata(
        int maxDepth,          // maximum depth (bytes) to analyze (default: 8)
        int version            // versioning for schema changes
) {

    public static final int DEFAULT_MAX_DEPTH = 8;
    public static final int CURRENT_VERSION = 1;

    public PrefixHistogramMetadata {
        if (maxDepth <= 0 || maxDepth > 8) {
            throw new IllegalArgumentException("maxDepth must be between 1 and 8");
        }
        if (version <= 0) {
            throw new IllegalArgumentException("version must be > 0");
        }
    }

    /**
     * Creates default prefix histogram metadata
     */
    public static PrefixHistogramMetadata defaultMetadata() {
        return new PrefixHistogramMetadata(
                DEFAULT_MAX_DEPTH,
                CURRENT_VERSION
        );
    }
}