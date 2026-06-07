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
 * Thread-safe registry for compound indexes, keyed by name.
 * Includes statistics tracking for histogram-based query optimization.
 */
public class CompoundIndexRegistry extends AbstractIndexRegistry<CompoundIndex, CompoundIndexDefinition> {
    private final Context context;
    private volatile Map<Long, IndexStatistics> statistics;
    private volatile long statsLastRefreshedAt;

    public CompoundIndexRegistry(Context context) {
        this.context = context;
    }

    @Override
    protected String extractKey(CompoundIndexDefinition definition) {
        return definition.name();
    }

    @Override
    protected CompoundIndex createHolder(CompoundIndexDefinition definition, DirectorySubspace subspace) {
        return new CompoundIndex(definition, subspace);
    }

    /**
     * Retrieves a compound index by its name with policy filtering.
     */
    public CompoundIndex getIndexByName(String name, IndexSelectionPolicy policy) {
        lock.readLock().lock();
        try {
            return filterByPolicy(entries.get(name), policy);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Updates the statistics for all compound indexes atomically.
     */
    public void updateStatistics(Map<Long, IndexStatistics> statistics) {
        this.statistics = statistics;
        this.statsLastRefreshedAt = context.now();
    }

    /**
     * Retrieves statistics for a specific compound index.
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
