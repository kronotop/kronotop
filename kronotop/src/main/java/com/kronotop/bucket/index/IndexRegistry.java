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


package com.kronotop.bucket.index;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.CachedTimeService;
import com.kronotop.Context;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class IndexRegistry {
    private final CachedTimeService cachedTime;
    private final Map<IndexDefinition, DirectorySubspace> registry = new HashMap<>();
    private volatile Map<Long, IndexStatistics> statistics;
    private volatile long statsLastRefreshedAt;

    public IndexRegistry(Context context) {
        this.cachedTime = context.getService(CachedTimeService.NAME);
    }

    public void register(IndexDefinition index, DirectorySubspace subspace) {
        registry.put(index, subspace);
    }

    public DirectorySubspace getSubspace(IndexDefinition index) {
        return registry.get(index);
    }

    public Set<IndexDefinition> getDefinitions() {
        return registry.keySet();
    }

    public void updateStatistics(Map<Long, IndexStatistics> statistics) {
        this.statistics = statistics;
        this.statsLastRefreshedAt = cachedTime.getCurrentTimeInMilliseconds();
    }

    public IndexStatistics getStatistics(long id) {
        return statistics.get(id);
    }

    public long getStatsLastRefreshedAt() {
        return statsLastRefreshedAt;
    }
}
