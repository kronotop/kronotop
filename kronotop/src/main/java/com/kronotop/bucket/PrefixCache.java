// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket;

import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.kronotop.Context;
import com.kronotop.common.KronotopException;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.volume.Prefix;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public final class PrefixCache {
    private final Context context;
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, List<String>>> buckets = new ConcurrentHashMap<>();

    private final LoadingCache<List<String>, Prefix> cache = CacheBuilder.newBuilder()
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .build(new BucketSubspaceLoader());

    public PrefixCache(Context context) {
        this.context = context;
    }

    public Prefix get(String namespace, String bucket) {
        List<String> subpath = buckets.computeIfAbsent(namespace, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(bucket, k -> {
                    KronotopDirectoryNode node = KronotopDirectory.
                            kronotop().
                            cluster(context.getClusterName()).
                            namespaces().
                            namespace(namespace).
                            buckets().
                            bucket(bucket);
                    return node.toList();
                });
        try {
            return cache.get(subpath);
        } catch (ExecutionException | UncheckedExecutionException e) {
            throw new KronotopException(e);
        }
    }

    private class BucketSubspaceLoader extends CacheLoader<List<String>, Prefix> {
        @Override
        public @Nonnull Prefix load(@Nonnull List<String> path) {
            DirectorySubspace subspace = context.getFoundationDB().run(tr -> DirectoryLayer.
                    getDefault().
                    createOrOpen(tr, path).
                    join());
            return new Prefix(subspace.pack());
        }
    }
}
