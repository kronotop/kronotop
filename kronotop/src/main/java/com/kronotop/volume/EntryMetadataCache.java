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

package com.kronotop.volume;

import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.kronotop.Context;

import javax.annotation.Nonnull;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class provides a caching mechanism for managing {@link EntryMetadata} objects
 * within a Volume. The cache is organized per {@link Prefix}, ensuring that each unique prefix
 * has its own {@link LoadingCache}. It leverages a {@link ConcurrentHashMap}
 * to store and manage these caches, and the caches themselves are configured with a
 * time-based expiration policy for efficient memory utilization.
 * <p>
 * The {@link EntryMetadataCache} class is designed to be thread-safe, making it suitable
 * for concurrent environments. Caches are lazily initialized upon first access using the
 * {@link #load(Prefix)} method, with new {@link LoadingCache} instances being created
 * if no cache currently exists for a given prefix.
 * <p>
 * The caching mechanism ensures high performance by reducing the need to repeatedly fetch
 * or compute {@link EntryMetadata} for the same prefix and {@link Versionstamp}.
 */
public class EntryMetadataCache {
    private final Context context;
    private final VolumeSubspace subspace;
    private final ConcurrentHashMap<Long, LoadingCache<Versionstamp, EntryMetadata>> cache = new ConcurrentHashMap<>();

    public EntryMetadataCache(@Nonnull Context context, @Nonnull VolumeSubspace subspace) {
        this.context = context;
        this.subspace = subspace;
    }

    /**
     * Loads a {@link LoadingCache} instance associated with the provided {@link Prefix}.
     * If the cache for the given prefix does not already exist, a new cache is created
     * and configured with an expiration policy.
     *
     * @param prefix the {@link Prefix} used to uniquely identify the cache entry.
     * @return a {@link LoadingCache} instance mapped to the given {@link Prefix}.
     */
    public LoadingCache<Versionstamp, EntryMetadata> load(Prefix prefix) {
        return cache.computeIfAbsent(prefix.asLong(), prefixId -> CacheBuilder.
                newBuilder().
                expireAfterAccess(EntryMetadataCacheLoader.EXPIRE_AFTER_ACCESS).
                build(new EntryMetadataCacheLoader(context, subspace, prefix)));
    }
}

