/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.index.statistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Container for histogram buckets representing index value distribution.
 * Wraps ArrayList to provide a specialized type for histogram operations.
 */
public class Histogram extends ArrayList<HistogramBucket> implements List<HistogramBucket> {
    // Extends ArrayList intentionally to avoid extra object wrappers in the hot path.
    // This class behaves both as a histogram container and as a list of buckets.
    // Not ideal from an OOP perspective, but chosen for performance-critical use.
    private final long version;

    private Histogram(long version) {
        this.version = version;
    }

    /**
     * Creates a new empty histogram.
     *
     * @return a new Histogram instance
     */
    public static Histogram create(long version) {
        return new Histogram(version);
    }

    public static Histogram create() {
        return new Histogram(0);
    }

    public long version() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Histogram that)) return false;
        if (this.version != that.version) return false;
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), version);
    }
}
