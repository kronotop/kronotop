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

package com.kronotop.journal;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.subspace.Subspace;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.kronotop.MissingConfigException;
import com.kronotop.common.KronotopException;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.typesafe.config.Config;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Journal class represents a journal that stores events and allows consumers to consume them.
 */
public class Journal {
    protected final String cluster;
    protected final Database database;
    private final Publisher publisher;
    private final LoadingCache<String, JournalMetadata> journalMetadataCache;

    public Journal(Config config, Database database) {
        if (!config.hasPath("cluster.name")) {
            throw new MissingConfigException("cluster.name is missing in configuration");
        }
        this.cluster = config.getString("cluster.name");
        this.database = database;
        this.journalMetadataCache = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .build(new JournalMetadataLoader());
        this.publisher = new Publisher(database, journalMetadataCache);
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
     * Retrieves the metadata of a given journal.
     *
     * @param journal The name of the journal for which metadata is to be retrieved.
     * @return The JournalMetadata object containing metadata of the specified journal.
     */
    public JournalMetadata getJournalMetadata(String journal) {
        try {
            return journalMetadataCache.get(journal);
        } catch (ExecutionException e) {
            throw new KronotopException(e);
        }
    }

    /**
     * Lists all available journals within the context of the current transaction.
     *
     * @param tr The transaction to use for retrieving the list of journals.
     * @return A list of journal names as strings.
     */
    protected List<String> listJournals(Transaction tr) {
        KronotopDirectoryNode directory = KronotopDirectory.kronotop().cluster(cluster).journals();
        try {
            DirectorySubspace root = DirectoryLayer.getDefault().open(tr, directory.toList()).join();
            return root.list(tr).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                return List.of();
            }
            throw e;
        }
    }

    /**
     * Lists all available journals within the current database context.
     *
     * @return a list of journal names as strings.
     */
    public List<String> listJournals() {
        try (Transaction tr = database.createTransaction()) {
            return listJournals(tr);
        }
    }

    /**
     * The JournalMetadataLoader is a private nested class within the Journal class that extends
     * CacheLoader<String, JournalMetadata>. It is responsible for loading the JournalMetadata for
     * a specific journal from the database.
     */
    // See https://github.com/google/guava/wiki/CachesExplained#when-does-cleanup-happen
    private class JournalMetadataLoader extends CacheLoader<String, JournalMetadata> {
        @Override
        public @Nonnull JournalMetadata load(@Nonnull String name) {
            Subspace subspace = database.run(tr -> {
                KronotopDirectoryNode directory =
                        KronotopDirectory.
                                kronotop().
                                cluster(cluster).
                                journals().
                                journal(name);
                return DirectoryLayer.getDefault().createOrOpen(tr, directory.toList()).join();
            });
            return new JournalMetadata(subspace);
        }
    }
}
