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

/**
 * Thread-safe registry for vector indexes, keyed by name.
 */
public class VectorIndexRegistry extends AbstractIndexRegistry<VectorIndex, VectorIndexDefinition> {

    @Override
    protected String extractKey(VectorIndexDefinition definition) {
        return definition.name();
    }

    @Override
    protected VectorIndex createHolder(VectorIndexDefinition definition, DirectorySubspace subspace) {
        return new VectorIndex(definition, subspace);
    }

    /**
     * Retrieves a vector index by its name with policy filtering.
     */
    public VectorIndex getIndexByName(String name, IndexSelectionPolicy policy) {
        lock.readLock().lock();
        try {
            return filterByPolicy(entries.get(name), policy);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean isEmpty() {
        lock.readLock().lock();
        try {
            return entries.isEmpty();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Retrieves a vector index by its selector (field path) with policy filtering.
     */
    public VectorIndex getIndexBySelector(String selector, IndexSelectionPolicy policy) {
        lock.readLock().lock();
        try {
            for (VectorIndex holder : entries.values()) {
                if (holder.definition().selector().equals(selector)) {
                    return filterByPolicy(holder, policy);
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }
}
