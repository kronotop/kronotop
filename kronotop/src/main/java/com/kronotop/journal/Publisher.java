/*
 * Copyright (c) 2023-2024 Kronotop
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
import com.kronotop.JSONUtils;
import com.kronotop.common.KronotopException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Publisher class represents a publisher that publishes events to a journal.
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
     * Publishes an event to a journal.
     *
     * @param tr      Transaction to use for publishing the event
     * @param journal Name of the journal to publish the event to
     * @param event   Event to publish
     * @return Versionstamp container holding the Versionstamp and user version of the published event
     * @throws KronotopException if an error occurs while publishing the event
     */
    private VersionstampContainer publish_internal(Transaction tr, String journal, Object event) {
        try {
            JournalMetadata journalMetadata = cache.get(journal);
            long readVersion = tr.getReadVersion().join();
            int userVersion = userVersions.get(readVersion).getAndIncrement();

            Subspace subspace = journalMetadata.getEventsSubspace();
            Tuple tuple = Tuple.from(Versionstamp.incomplete(userVersion));

            Entry entry = new Entry(JSONUtils.writeValueAsBytes(event), Instant.now().toEpochMilli());

            tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, subspace.packWithVersionstamp(tuple), entry.encode());
            tr.mutate(MutationType.ADD, journalMetadata.getTrigger(), TRIGGER_DELTA);

            return new VersionstampContainer(tr.getVersionstamp(), userVersion);
        } catch (Exception e) {
            LOGGER.error("Failed to publish event: {}", e.getMessage());
            throw new KronotopException(e);
        }
    }

    /**
     * Publishes an event to a journal.
     *
     * @param tr      Transaction to use for publishing the event
     * @param journal Name of the journal to publish the event to
     * @param event   Event to publish
     * @return Versionstamp container holding the Versionstamp and user version of the published event
     * @throws KronotopException if an error occurs while publishing the event
     */
    public VersionstampContainer publish(Transaction tr, String journal, Object event) {
        return publish_internal(tr, journal, event);
    }

    /**
     * Publishes an event to a journal.
     *
     * @param journal Name of the journal to publish the event to
     * @param event   Event to publish
     * @return {@link VersionstampContainer} holding the Versionstamp and user version of the published event
     * @throws KronotopException if an error occurs while publishing the event
     */
    public VersionstampContainer publish(String journal, Object event) {
        return database.run((Transaction tr) -> publish_internal(tr, journal, event));
    }

    private static class UserVersionLoader extends CacheLoader<Long, AtomicInteger> {
        @Override
        public @Nonnull AtomicInteger load(@Nonnull Long key) {
            return new AtomicInteger(0);
        }
    }
}
