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

package com.kronotop.core.cluster.journal;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.kronotop.common.utils.ByteUtils;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.Context;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class BaseJournal {
    protected final Context context;
    protected final LoadingCache<String, JournalMetadata> cache;

    public BaseJournal(Context context) {
        this.context = context;
        this.cache = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .build(new JournalMetadataLoader());
    }

    protected long lastIndex(Transaction tr, JournalMetadata journalMetadata) {
        tr.mutate(MutationType.ADD, journalMetadata.getIndexKey(), ByteUtils.fromLong(1L));
        byte[] value = tr.get(journalMetadata.getIndexKey()).join();
        return ByteUtils.toLong(value);
    }

    protected KeyValue firstItem(Transaction tr, JournalMetadata journalMetadata, long offset) {
        if (offset == 0) {
            throw new IllegalArgumentException("0 is an illegal offset");
        }
        for (KeyValue kv : tr.getRange(journalMetadata.getSubspace().range(Tuple.from(offset)), 1)) {
            return kv;
        }
        return null;
    }

    // See https://github.com/google/guava/wiki/CachesExplained#when-does-cleanup-happen

    public JournalMetadata getJournalMetadata(String journal) throws ExecutionException {
        return cache.get(journal);
    }

    private class JournalMetadataLoader extends CacheLoader<String, JournalMetadata> {
        @Override
        public @Nonnull JournalMetadata load(@Nonnull String journal) {
            Subspace subspace = context.getFoundationDB().run(tr -> {
                List<String> subpath = DirectoryLayout.
                        Builder.
                        clusterName(context.getClusterName()).
                        internal().
                        cluster().asList();
                DirectorySubspace directorySubspace = DirectoryLayer.getDefault().createOrOpen(tr, subpath).join();
                return directorySubspace.subspace(Tuple.from(journal));
            });
            return new JournalMetadata(journal, subspace);
        }
    }
}
