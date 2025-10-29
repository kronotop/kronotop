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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe registry managing bucket indexes with policy-based access control.
 *
 * <p>Maintains indexes in memory and provides filtered access based on {@link IndexSelectionPolicy}
 * and {@link IndexStatus}. Indexes are segregated into three precomputed lists (readonly, readwrite, all)
 * for fast retrieval without repeated filtering.
 *
 * <p><b>Core Responsibilities:</b>
 * <ul>
 *   <li>Register/deregister indexes with uniqueness enforcement on selectors</li>
 *   <li>Provide policy-based index retrieval (by ID, selector, or collection)</li>
 *   <li>Maintain index statistics with refresh tracking</li>
 *   <li>Automatically segregate indexes by status after mutations</li>
 * </ul>
 *
 * <p><b>Thread Safety:</b> Uses a fair {@link ReentrantReadWriteLock} for concurrent reads
 * and exclusive writes. Index collections are immutable snapshots updated atomically.
 *
 * @see IndexSelectionPolicy
 * @see Index
 * @see IndexStatistics
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

    /**
     * Creates a new index registry.
     *
     * @param context the application context for accessing services
     */
    public IndexRegistry(Context context) {
        this.cachedTime = context.getService(CachedTimeService.NAME);
    }

    /**
     * Registers an index with its associated FoundationDB subspace.
     *
     * <p>Adds the index to the registry and automatically segregates it into appropriate
     * policy lists based on its status. Index selectors must be unique across the registry.
     *
     * @param definition the index definition containing metadata and configuration
     * @param subspace   the FoundationDB directory subspace for storing index data
     * @throws IndexAlreadyRegisteredException if an index with the same selector already exists
     */
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

    /**
     * Removes an index from the registry.
     *
     * <p>Removes the index from all internal structures including the selector map,
     * policy lists, and statistics. If statistics are present, the index's statistics
     * are also removed.
     *
     * @param definition the index definition to deregister
     */
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

    /**
     * Rebuilds policy-based index lists after registry mutations.
     *
     * <p>Categorizes indexes into three immutable lists based on {@link IndexStatus}:
     * <ul>
     *   <li><b>readonly:</b> {@link IndexStatus#READY} indexes only</li>
     *   <li><b>readwrite:</b> {@link IndexStatus#READY}, {@link IndexStatus#BUILDING},
     *       or {@link IndexStatus#WAITING} indexes</li>
     *   <li><b>all:</b> Every registered index regardless of status</li>
     * </ul>
     *
     * <p>Called automatically after {@link #register} and {@link #deregister} to maintain
     * consistency. List updates are atomic via volatile reference assignment.
     */
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

    /**
     * Applies selection policy filtering to an index.
     *
     * <p>Returns the index if its status matches the policy requirements, otherwise returns null.
     * Used by ID and selector-based lookups to enforce policy constraints.
     *
     * @param index  the index to filter (may be null)
     * @param policy the selection policy to apply
     * @return the index if it passes the policy filter, null otherwise
     * @throws IllegalArgumentException if the policy is not recognized
     * @see IndexSelectionPolicy
     */
    private Index filterIndexByPolicy(Index index, IndexSelectionPolicy policy) {
        if (index == null) {
            return null;
        }

        IndexStatus status = index.definition().status();

        if (policy == IndexSelectionPolicy.READ) {
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

    /**
     * Retrieves an index by its unique identifier with policy filtering.
     *
     * @param id     the index ID to look up
     * @param policy the selection policy to apply
     * @return the index if found and passes the policy filter, null otherwise
     */
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

    /**
     * Retrieves an index by its selector with policy filtering.
     *
     * @param selector the index selector (typically the field name or path)
     * @param policy   the selection policy to apply
     * @return the index if found and passes the policy filter, null otherwise
     */
    public Index getIndex(String selector, IndexSelectionPolicy policy) {
        lock.readLock().lock();
        Index index = bySelector.get(selector);
        try {
            return filterIndexByPolicy(index, policy);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Retrieves all indexes matching the specified policy.
     *
     * <p>Returns a precomputed immutable collection for efficient bulk access
     * without per-index filtering overhead. The returned collection is a snapshot
     * that won't reflect subsequent registry changes.
     *
     * @param policy the selection policy determining which indexes to include
     * @return an immutable collection of indexes matching the policy
     */
    public Collection<Index> getIndexes(IndexSelectionPolicy policy) {
        return switch (policy) {
            case ALL -> all;
            case READ -> readonly;
            case READWRITE -> readwrite;
        };
    }

    /**
     * Updates the statistics for all indexes atomically.
     *
     * <p>Replaces the current statistics map and records the refresh timestamp.
     * Statistics updates are typically triggered by background maintenance routines.
     *
     * @param statistics a map of index IDs to their statistics
     */
    public void updateStatistics(Map<Long, IndexStatistics> statistics) {
        this.statistics = statistics;
        this.statsLastRefreshedAt = cachedTime.getCurrentTimeInMilliseconds();
    }

    /**
     * Retrieves statistics for a specific index.
     *
     * @param id the index ID
     * @return the index statistics, or null if not available
     */
    public IndexStatistics getStatistics(long id) {
        if (statistics != null) {
            return statistics.get(id);
        }
        return null;
    }

    /**
     * Returns the timestamp when statistics were last refreshed.
     *
     * @return the last refresh timestamp in milliseconds, or 0 if never refreshed
     */
    public long getStatsLastRefreshedAt() {
        return statsLastRefreshedAt;
    }
}
