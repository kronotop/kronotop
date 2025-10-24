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
import com.kronotop.KronotopException;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The IndexRegistry class is responsible for managing and organizing index-related operations
 * within a specific context. It allows the registration and retrieval of indexes and their
 * associated subspaces, as well as management of index statistics and their refresh timings.
 */
public class IndexRegistry {
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final CachedTimeService cachedTime;
    private final Map<String, Index> bySelector = new LinkedHashMap<>();
    private List<Index> readonly = new ArrayList<>();
    private List<Index> readwrite = new ArrayList<>();
    private List<Index> all = new ArrayList<>();
    private volatile Map<Long, IndexStatistics> statistics;
    private volatile long statsLastRefreshedAt;

    public IndexRegistry(Context context) {
        this.cachedTime = context.getService(CachedTimeService.NAME);
    }

    public void register(IndexDefinition definition, DirectorySubspace subspace) {
        lock.writeLock().lock();
        try {
            Index index = new Index(definition, subspace);
            if (bySelector.containsKey(definition.selector())) {
                throw new IndexAlreadyRegisteredException(definition.selector());
            }
            bySelector.put(definition.selector(), index);
            segregateIndexesByPolicy();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void deregister(IndexDefinition definition) {
        lock.writeLock().lock();
        try {
            bySelector.remove(definition.selector());
            if (statistics != null) {
                statistics.remove(definition.id());
            }
            segregateIndexesByPolicy();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void updateIndexDefinition(IndexDefinition definition) {
        // Useful for index status changes
        lock.writeLock().lock();
        try {
            Index index = bySelector.get(definition.selector());
            if (index == null) {
                throw new KronotopException("Index with '" + definition.selector() + "' could not be found");
            }
            Index refreshed = new Index(definition, index.subspace());
            bySelector.put(definition.selector(), refreshed);
            segregateIndexesByPolicy();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void segregateIndexesByPolicy() {
        List<Index> readonly = new ArrayList<>();
        List<Index> readwrite = new ArrayList<>();
        List<Index> all = new ArrayList<>();
        for (Index index : bySelector.values()) {
            all.add(index);
            if (index.definition().status() == IndexStatus.READY) {
                readonly.add(index);
                readwrite.add(index);
            } else {
                switch (index.definition().status()) {
                    case WAITING, BUILDING -> readwrite.add(index);
                }
            }
        }
        this.readonly = Collections.unmodifiableList(readonly);
        this.readwrite = Collections.unmodifiableList(readwrite);
        this.all = Collections.unmodifiableList(all);
    }

    private Index filterIndexByPolicy(Index index, IndexSelectionPolicy policy) {
        if (index == null) {
            return null;
        }

        IndexStatus status = index.definition().status();

        if (policy == IndexSelectionPolicy.READONLY) {
            return status == IndexStatus.READY ? index : null;
        }

        if (policy == IndexSelectionPolicy.READWRITE) {
            return switch (status) {
                case WAITING, BUILDING, READY -> index;
                case DROPPED, FAILED -> null;
            };
        }

        if (policy == IndexSelectionPolicy.ALL) {
            return index;
        }

        throw new IllegalArgumentException("Unknown policy: " + policy);
    }

    public Index getIndexById(long id, IndexSelectionPolicy policy) {
        lock.readLock().lock();
        try {
            for (Index index : bySelector.values()) {
                if (index.definition().id() == id) {
                    return filterIndexByPolicy(index, policy);
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    public Index getIndex(String selector, IndexSelectionPolicy policy) {
        lock.readLock().lock();
        Index index = bySelector.get(selector);
        try {
            return filterIndexByPolicy(index, policy);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Collection<Index> getIndexes(IndexSelectionPolicy policy) {
        return switch (policy) {
            case ALL -> all;
            case READONLY -> readonly;
            case READWRITE -> readwrite;
        };
    }

    public void updateStatistics(Map<Long, IndexStatistics> statistics) {
        this.statistics = statistics;
        this.statsLastRefreshedAt = cachedTime.getCurrentTimeInMilliseconds();
    }

    public IndexStatistics getStatistics(long id) {
        if (statistics != null) {
            return statistics.get(id);
        }
        return null;
    }

    public long getStatsLastRefreshedAt() {
        return statsLastRefreshedAt;
    }
}
