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


package com.kronotop.bucket.index;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;

import java.util.Map;

/**
 * Thread-safe registry for single-field indexes, keyed by selector.
 * Extends shared registry logic and adds statistics tracking.
 */
public class SingleFieldIndexRegistry extends AbstractIndexRegistry<Index, SingleFieldIndexDefinition> {
    private final Context context;
    private volatile Map<Long, IndexStatistics> statistics;
    private volatile long statsLastRefreshedAt;

    public SingleFieldIndexRegistry(Context context) {
        this.context = context;
    }

    @Override
    protected String extractKey(SingleFieldIndexDefinition definition) {
        return definition.selector();
    }

    @Override
    protected Index createHolder(SingleFieldIndexDefinition definition, DirectorySubspace subspace) {
        return new Index(definition, subspace);
    }

    @Override
    public void deregister(SingleFieldIndexDefinition definition) {
        super.deregister(definition);
        if (statistics != null) {
            statistics.remove(definition.id());
        }
    }

    /**
     * Retrieves an index by its selector with policy filtering.
     */
    public Index getIndex(String selector, IndexSelectionPolicy policy) {
        lock.readLock().lock();
        try {
            return filterByPolicy(entries.get(selector), policy);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Updates the statistics for all indexes atomically.
     */
    public void updateStatistics(Map<Long, IndexStatistics> statistics) {
        this.statistics = statistics;
        this.statsLastRefreshedAt = context.now();
    }

    /**
     * Retrieves statistics for a specific index.
     */
    public IndexStatistics getStatistics(long id) {
        if (statistics != null) {
            return statistics.get(id);
        }
        return null;
    }

    /**
     * Returns the timestamp when statistics were last refreshed.
     */
    public long getStatsLastRefreshedAt() {
        return statsLastRefreshedAt;
    }
}
