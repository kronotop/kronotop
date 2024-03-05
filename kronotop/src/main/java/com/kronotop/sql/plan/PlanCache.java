/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.sql.plan;

import com.kronotop.sql.Plan;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PlanCache is a class that provides caching functionality for relational query plans.
 * It uses a ConcurrentHashMap to store the plans in memory.
 */
public class PlanCache {
    private static final int DEFAULT_CACHE_SIZE = 10_000;
    private final int maxCacheSize;
    ConcurrentHashMap<String, Cache> schemas = new ConcurrentHashMap<>();

    public PlanCache() {
        this(0);
    }

    public PlanCache(int maxCacheSize) {
        if (maxCacheSize > 0) {
            this.maxCacheSize = maxCacheSize;
        } else {
            this.maxCacheSize = DEFAULT_CACHE_SIZE;
        }
    }

    public Plan getPlan(String schema, String query) {
        Cache cache = schemas.get(schema);
        if (cache == null) {
            return null;
        }
        return cache.get(query);
    }

    public void putPlan(String schema, String query, Plan plan) {
        Cache cache = schemas.computeIfAbsent(schema, s -> new Cache());
        cache.put(query, plan);
        shrinkIfNeeded(cache);
    }

    public void invalidateSchema(String schema) {
        schemas.remove(schema);
    }

    public void invalidateQuery(String schema, String query) {
        Cache cache = schemas.get(schema);
        if (cache == null) {
            return;
        }
        cache.remove(query);
    }

    /**
     * Shrink the cache if its size exceeds the maximum cache size.
     * The cache entries will be ordered by the last accessed time, and a portion of the entries will be removed.
     *
     * @param cache The cache to be checked and potentially shrunken
     */
    private void shrinkIfNeeded(Cache cache) {
        if (cache.size() > maxCacheSize) {
            cache.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.comparingLong(Plan::getLastAccessed)))
                    .limit(maxCacheSize / 10)
                    .map(Map.Entry::getKey)
                    .forEach(cache::remove);
        }
    }

    private static class Cache extends ConcurrentHashMap<String, Plan> {
    }
}
