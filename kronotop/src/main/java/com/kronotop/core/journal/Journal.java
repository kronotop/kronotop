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

package com.kronotop.core.journal;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.subspace.Subspace;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.kronotop.MissingConfigException;
import com.kronotop.common.utils.DirectoryLayout;
import com.typesafe.config.Config;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Journal class represents a journal that stores events and allows consumers to consume them.
 */
public class Journal {
    private final Publisher publisher;
    private final Consumer consumer;
    private final String clusterName;
    private final Database database;
    private final LoadingCache<String, JournalMetadata> journalMetadataCache;

    public Journal(Config config, Database database) {
        if (!config.hasPath("cluster.name")) {
            throw new MissingConfigException("cluster.name is missing in configuration");
        }
        this.clusterName = config.getString("cluster.name");
        this.database = database;
        this.journalMetadataCache = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .build(new JournalMetadataLoader());
        this.publisher = new Publisher(database, journalMetadataCache);
        this.consumer = new Consumer(database, journalMetadataCache);

    }

    /**
     * Retrieves the Consumer object associated with the Journal.
     *
     * @return The Consumer object.
     */
    public Consumer getConsumer() {
        return consumer;
    }

    /**
     * Returns the Publisher object associated with the Journal.
     *
     * @return The Publisher object.
     */
    public Publisher getPublisher() {
        return publisher;
    }

    /**
     * Retrieves the metadata of a journal from the cache.
     *
     * @param journal The name of the journal.
     * @return An instance of {@link JournalMetadata} representing the metadata of the journal.
     * @throws ExecutionException if an error occurs while retrieving the metadata.
     */
    public JournalMetadata getJournalMetadata(String journal) throws ExecutionException {
        return journalMetadataCache.get(journal);
    }

    /**
     * The JournalMetadataLoader is a private nested class within the Journal class that extends CacheLoader<String, JournalMetadata>.
     * It is responsible for loading the JournalMetadata for a specific journal from the database.
     */
    // See https://github.com/google/guava/wiki/CachesExplained#when-does-cleanup-happen
    private class JournalMetadataLoader extends CacheLoader<String, JournalMetadata> {
        @Override
        public @Nonnull JournalMetadata load(@Nonnull String journal) {
            Subspace subspace = database.run(transaction -> {
                List<String> subpath = DirectoryLayout.
                        Builder.
                        clusterName(clusterName).
                        internal().
                        cluster().
                        journals().
                        addAll(List.of(journal)).
                        asList();
                return DirectoryLayer.getDefault().createOrOpen(transaction, subpath).join();
            });
            return new JournalMetadata(subspace);
        }
    }
}
