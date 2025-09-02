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

package com.kronotop.bucket.statistics.prefix;

/**
 * Immutable metadata for Fixed N-Byte Prefix Histogram.
 * Simple configuration without sharding complexity.
 */
public record PrefixHistogramMetadata(
        int N,           // Number of prefix bytes (2 or 3)
        int peekCap,     // Maximum KV to read for equality peek
        int version      // Schema version
) {

    public static final int DEFAULT_N = 2;
    public static final int DEFAULT_PEEK_CAP = 1024;
    public static final int CURRENT_VERSION = 1;

    public PrefixHistogramMetadata {
        if (N <= 0) {
            throw new IllegalArgumentException("N must be > 0");
        }
        if (peekCap <= 0) {
            throw new IllegalArgumentException("peekCap must be > 0");
        }
        if (version <= 0) {
            throw new IllegalArgumentException("version must be > 0");
        }
    }

    /**
     * Creates default prefix histogram metadata
     */
    public static PrefixHistogramMetadata defaultMetadata() {
        return new PrefixHistogramMetadata(DEFAULT_N, DEFAULT_PEEK_CAP, CURRENT_VERSION);
    }

    /**
     * Creates metadata with custom N value
     */
    public static PrefixHistogramMetadata withN(int N) {
        return new PrefixHistogramMetadata(N, DEFAULT_PEEK_CAP, CURRENT_VERSION);
    }
}