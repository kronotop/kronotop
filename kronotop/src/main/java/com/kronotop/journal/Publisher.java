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
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.internal.JSONUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;


/**
 * Publishes events to journals with automatic versionstamping and ordering guarantees.
 * Uses FoundationDB's versionstamp mechanism to ensure events are uniquely identified and ordered.
 */
public class Publisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(Publisher.class);
    private static final byte[] TRIGGER_DELTA = new byte[]{1, 0, 0, 0}; // 1, byte order: little-endian
    private final Database database;
    private final LoadingCache<String, JournalMetadata> cache;
    private final LoadingCache<Long, AtomicInteger> userVersions;

    Publisher(Database database, LoadingCache<String, JournalMetadata> cache) {
        this.database = database;
        this.cache = cache;
        this.userVersions = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.SECONDS)
                .build(new UserVersionLoader());
    }

    /**
     * Core publishing logic that writes an event to a journal with versionstamp-based ordering.
     * Generates a unique user version per read version to ensure multiple events in the same
     * transaction are ordered correctly. Increments the journal trigger counter for monitoring.
     *
     * @param tr Transaction for event publication.
     * @param journal Journal name.
     * @param event Event object to publish (will be JSON-encoded).
     * @return Container with versionstamp and user version for the published event.
     * @throws KronotopException if publication fails.
     */
    private VersionstampContainer publish_internal(Transaction tr, String journal, Object event) {
        try {
            JournalMetadata journalMetadata = cache.get(journal);
            long readVersion = tr.getReadVersion().join();
            int userVersion = userVersions.get(readVersion).getAndIncrement();

            Subspace subspace = journalMetadata.eventsSubspace();
            Tuple tuple = Tuple.from(Versionstamp.incomplete(userVersion));

            Entry entry = new Entry(JSONUtil.writeValueAsBytes(event), Instant.now().toEpochMilli());

            tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, subspace.packWithVersionstamp(tuple), entry.encode());
            tr.mutate(MutationType.ADD, journalMetadata.trigger(), TRIGGER_DELTA);

            return new VersionstampContainer(tr.getVersionstamp(), userVersion);
        } catch (Exception e) {
            LOGGER.error("Failed to publish event: {}", e.getMessage());
            throw new KronotopException(e);
        }
    }

    /**
     * Publishes an event within an existing transaction.
     *
     * @param tr Transaction for event publication.
     * @param journal Journal name.
     * @param event Event object to publish.
     * @return Container with versionstamp and user version.
     * @throws KronotopException if publication fails.
     */
    public VersionstampContainer publish(Transaction tr, String journal, Object event) {
        return publish_internal(tr, journal, event);
    }

    /**
     * Publishes an event within an existing transaction using typed journal name.
     *
     * @param tr Transaction for event publication.
     * @param journal Typed journal name.
     * @param event Event object to publish.
     * @return Container with versionstamp and user version.
     * @throws KronotopException if publication fails.
     */
    public VersionstampContainer publish(Transaction tr, JournalName journal, Object event) {
        return publish_internal(tr, journal.getValue(), event);
    }

    /**
     * Publishes an event in a new auto-committed transaction.
     * Creates and commits the transaction automatically.
     *
     * @param journal Journal name.
     * @param event Event object to publish.
     * @return Container with versionstamp and user version.
     * @throws KronotopException if publication fails.
     */
    public VersionstampContainer publish(String journal, Object event) {
        return executeThenCommit((Transaction tr) -> publish_internal(tr, journal, event));
    }

    /**
     * Publishes an event in a new auto-committed transaction using typed journal name.
     * Creates and commits the transaction automatically.
     *
     * @param journal Typed journal name.
     * @param event Event object to publish.
     * @return Container with versionstamp and user version.
     * @throws KronotopException if publication fails.
     */
    public VersionstampContainer publish(JournalName journal, Object event) {
        return publish(journal.getValue(), event);
    }

    /**
     * Executes a provided function within the context of a transactional operation
     * and commits the transaction upon successful execution.
     *
     * @param <T> The type of the result produced by the provided function.
     * @param action The function to execute within the transaction. It accepts a
     *               {@link Transaction} as input and returns a result of type {@code T}.
     * @return The result produced by the provided function after the transaction is committed.
     */
    private <T> T executeThenCommit(Function<? super Transaction, T> action) {
        try (Transaction tr = database.createTransaction()) {
            T result = action.apply(tr);
            tr.commit().join();
            return result;
        }
    }

    /**
     * Cache loader for user version counters.
     * Each read version gets an atomic counter starting at 0 to disambiguate events within the same transaction.
     */
    private static class UserVersionLoader extends CacheLoader<Long, AtomicInteger> {
        @Override
        public @Nonnull AtomicInteger load(@Nonnull Long key) {
            return new AtomicInteger(0);
        }
    }
}
