/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.zmap;

import java.util.concurrent.atomic.LongAdder;

/**
 * Counts ZMap command calls on this member. One call is one count.
 * The counters start at zero on restart.
 */
public class ZMapMetrics {
    private final LongAdder sets = new LongAdder();
    private final LongAdder gets = new LongAdder();
    private final LongAdder rangeReads = new LongAdder();
    private final LongAdder deletes = new LongAdder();
    private final LongAdder mutations = new LongAdder();
    private final LongAdder watches = new LongAdder();

    public void incrementSets() {
        sets.increment();
    }

    public void incrementGets() {
        gets.increment();
    }

    public void incrementRangeReads() {
        rangeReads.increment();
    }

    public void incrementDeletes() {
        deletes.increment();
    }

    public void incrementMutations() {
        mutations.increment();
    }

    public void incrementWatches() {
        watches.increment();
    }

    public long getSets() {
        return sets.sum();
    }

    public long getGets() {
        return gets.sum();
    }

    public long getRangeReads() {
        return rangeReads.sum();
    }

    public long getDeletes() {
        return deletes.sum();
    }

    public long getMutations() {
        return mutations.sum();
    }

    public long getWatches() {
        return watches.sum();
    }
}
