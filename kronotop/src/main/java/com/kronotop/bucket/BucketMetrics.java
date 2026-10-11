/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.bucket;

import java.util.concurrent.atomic.LongAdder;

/**
 * Counts Bucket command calls on this member. One call is one count, whatever the number of documents.
 * The counters start at zero on restart.
 */
public class BucketMetrics {
    private final LongAdder inserts = new LongAdder();
    private final LongAdder queries = new LongAdder();
    private final LongAdder updates = new LongAdder();
    private final LongAdder deletes = new LongAdder();
    private final LongAdder advances = new LongAdder();
    private final LongAdder vectorSearches = new LongAdder();
    private final LongAdder purges = new LongAdder();
    private final LongAdder removes = new LongAdder();
    private final LongAdder planCacheHits = new LongAdder();
    private final LongAdder planCacheMisses = new LongAdder();
    private final LongAdder vectorRetriedOps = new LongAdder();

    public void incrementInserts() {
        inserts.increment();
    }

    public void incrementQueries() {
        queries.increment();
    }

    public void incrementUpdates() {
        updates.increment();
    }

    public void incrementDeletes() {
        deletes.increment();
    }

    public long getInserts() {
        return inserts.sum();
    }

    public long getQueries() {
        return queries.sum();
    }

    public long getUpdates() {
        return updates.sum();
    }

    public long getDeletes() {
        return deletes.sum();
    }

    public void incrementAdvances() {
        advances.increment();
    }

    public void incrementVectorSearches() {
        vectorSearches.increment();
    }

    public void incrementPurges() {
        purges.increment();
    }

    public void incrementRemoves() {
        removes.increment();
    }

    public void incrementPlanCacheHits() {
        planCacheHits.increment();
    }

    public void incrementPlanCacheMisses() {
        planCacheMisses.increment();
    }

    public long getAdvances() {
        return advances.sum();
    }

    public long getVectorSearches() {
        return vectorSearches.sum();
    }

    public long getPurges() {
        return purges.sum();
    }

    public long getRemoves() {
        return removes.sum();
    }

    public long getPlanCacheHits() {
        return planCacheHits.sum();
    }

    public long getPlanCacheMisses() {
        return planCacheMisses.sum();
    }

    public void addVectorRetriedOps(long count) {
        vectorRetriedOps.add(count);
    }

    public long getVectorRetriedOps() {
        return vectorRetriedOps.sum();
    }
}
