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

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe base registry for indexes with policy-based access control.
 *
 * @param <I> the index holder type
 * @param <D> the index definition type
 */
public abstract class AbstractIndexRegistry<I extends IndexHolder<D>, D extends IndexDefinition> {
    protected final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    protected final Map<String, I> entries = new LinkedHashMap<>();
    private List<I> readonly = new ArrayList<>();
    private List<I> readwrite = new ArrayList<>();
    private List<I> all = new ArrayList<>();

    /**
     * Extracts the map key from an index definition (e.g., selector or name).
     */
    protected abstract String extractKey(D definition);

    /**
     * Creates an index holder from a definition and subspace.
     */
    protected abstract I createHolder(D definition, DirectorySubspace subspace);

    /**
     * Registers an index with its associated FoundationDB subspace.
     *
     * @param definition the index definition
     * @param subspace   the FoundationDB directory subspace for storing index data
     * @throws IndexAlreadyRegisteredException if an index with the same key already exists
     */
    public void register(D definition, DirectorySubspace subspace) {
        lock.writeLock().lock();
        try {
            String key = extractKey(definition);
            if (entries.containsKey(key)) {
                throw new IndexAlreadyRegisteredException(key);
            }
            entries.put(key, createHolder(definition, subspace));
            segregateIndexesByPolicy();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes an index from the registry.
     *
     * @param definition the index definition to deregister
     */
    public void deregister(D definition) {
        lock.writeLock().lock();
        try {
            entries.remove(extractKey(definition));
            segregateIndexesByPolicy();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void segregateIndexesByPolicy() {
        List<I> readonly = new ArrayList<>();
        List<I> readwrite = new ArrayList<>();
        List<I> all = new ArrayList<>();
        for (I holder : entries.values()) {
            all.add(holder);
            if (holder.definition().status() == IndexStatus.READY) {
                readonly.add(holder);
                readwrite.add(holder);
            } else if (holder.definition().status() == IndexStatus.BUILDING) {
                readwrite.add(holder);
            }
        }
        this.readonly = Collections.unmodifiableList(readonly);
        this.readwrite = Collections.unmodifiableList(readwrite);
        this.all = Collections.unmodifiableList(all);
    }

    protected I filterByPolicy(I holder, IndexSelectionPolicy policy) {
        if (holder == null) {
            return null;
        }
        IndexStatus status = holder.definition().status();
        return switch (policy) {
            case READ -> status == IndexStatus.READY ? holder : null;
            case READWRITE -> (status == IndexStatus.READY || status == IndexStatus.BUILDING) ? holder : null;
            case ALL -> holder;
        };
    }

    /**
     * Retrieves an index by its unique identifier with policy filtering.
     */
    public I getIndexById(long id, IndexSelectionPolicy policy) {
        lock.readLock().lock();
        try {
            for (I holder : entries.values()) {
                if (holder.definition().id() == id) {
                    return filterByPolicy(holder, policy);
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Retrieves all indexes matching the specified policy.
     */
    public Collection<I> getIndexes(IndexSelectionPolicy policy) {
        return switch (policy) {
            case ALL -> all;
            case READ -> readonly;
            case READWRITE -> readwrite;
        };
    }
}
