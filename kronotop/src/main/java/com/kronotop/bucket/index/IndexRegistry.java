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

import java.util.*;

/**
 * The IndexRegistry class is responsible for managing and organizing index-related operations
 * within a specific context. It allows the registration and retrieval of indexes and their
 * associated subspaces, as well as management of index statistics and their refresh timings.
 */
public class IndexRegistry {
    private final CachedTimeService cachedTime;
    private final Map<String, IndexBundle> indexBundleBySelectors = new HashMap<>();
    private Set<String> selectors = new HashSet<>();
    private volatile Map<Long, IndexStatistics> statistics;
    private volatile long statsLastRefreshedAt;

    public IndexRegistry(Context context) {
        this.cachedTime = context.getService(CachedTimeService.NAME);
    }

    public void register(IndexDefinition definition, DirectorySubspace subspace) {
        IndexBundle bundle = new IndexBundle(definition, subspace);
        indexBundleBySelectors.put(definition.selector(), bundle);
        selectors = Collections.unmodifiableSet(indexBundleBySelectors.keySet());
    }

    public DirectorySubspace getSubspace(String selector) {
        IndexBundle bundle = indexBundleBySelectors.get(selector);
        if (bundle == null) {
            return null;
        }
        return bundle.subspace;
    }

    public IndexDefinition getIndexBySelector(String selector) {
        IndexBundle bundle = indexBundleBySelectors.get(selector);
        if (bundle == null) {
            return null;
        }
        return bundle.definition;
    }

    public void updateStatistics(Map<Long, IndexStatistics> statistics) {
        this.statistics = statistics;
        this.statsLastRefreshedAt = cachedTime.getCurrentTimeInMilliseconds();
    }

    public Set<String> getSelectors() {
        return selectors;
    }

    public IndexStatistics getStatistics(long id) {
        return statistics.get(id);
    }

    public long getStatsLastRefreshedAt() {
        return statsLastRefreshedAt;
    }

    record IndexBundle(IndexDefinition definition, DirectorySubspace subspace) {
    }
}
