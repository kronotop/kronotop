// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.


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
