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

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.cache.LoadingCache;
import com.kronotop.common.KronotopException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

/**
 * The Consumer class is responsible for consuming events from a journal.
 */
public class Consumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
    private final Database database;
    private final LoadingCache<String, JournalMetadata> cache;

    Consumer(Database database, LoadingCache<String, JournalMetadata> cache) {
        this.database = database;
        this.cache = cache;
    }

    /**
     * Retrieves the latest event key from a journal.
     *
     * @param tr      The ReadTransaction to perform the operation on.
     * @param journal The journal to retrieve the event key from.
     * @return The latest event key, or null if there is no event.
     * @throws KronotopException If there is an error retrieving the event key.
     */
    public byte[] getLatestEventKey(ReadTransaction tr, String journal) {
        try {
            JournalMetadata journalMetadata = cache.get(journal);
            Subspace subspace = new Subspace(journalMetadata.getIndexKey());

            AsyncIterator<KeyValue> iterator = tr.getRange(subspace.range(), 1, true).iterator();
            if (!iterator.hasNext()) {
                return null;
            }
            return iterator.next().getKey();
        } catch (ExecutionException e) {
            LOGGER.error("Failed to get latest index: {}", e.getMessage());
            throw new KronotopException(e.getCause());
        }
    }

    /**
     * Retrieves the versionstamp from the given key in a journal.
     *
     * @param journal The journal to retrieve the versionstamp from.
     * @param key     The key to extract the versionstamp from.
     * @return The versionstamp extracted from the key.
     * @throws KronotopException If there is an error retrieving the versionstamp.
     */
    public Versionstamp getVersionstampFromKey(String journal, byte[] key) {
        try {
            JournalMetadata journalMetadata = cache.get(journal);
            Subspace subspace = new Subspace(journalMetadata.getIndexKey());
            return subspace.unpack(key).getVersionstamp(0);
        } catch (ExecutionException e) {
            LOGGER.error("Failed to get latest index: {}", e.getMessage());
            throw new KronotopException(e.getCause());
        }
    }

    /**
     * Consumes the next event from a journal based on the given versionstamp.
     * <p>
     * If there is no next event, returns null.
     *
     * @param tr           The transaction to perform the operation on.
     * @param journal      The journal to consume the event from.
     * @param versionstamp The versionstamp to use for consuming the event.
     * @return The consumed event, or null if there is no next event.
     * @throws KronotopException if there is an error consuming the event.
     */
    public Event consumeByVersionstamp(Transaction tr, String journal, Versionstamp versionstamp) {
        try {
            JournalMetadata journalMetadata = cache.get(journal);
            Subspace subspace = new Subspace(Tuple.fromBytes(journalMetadata.getIndexKey()));

            AsyncIterator<KeyValue> iterator = tr.getRange(subspace.range(Tuple.from(versionstamp)), 1).iterator();
            if (!iterator.hasNext()) {
                return null;
            }
            KeyValue next = iterator.next();
            return new Event(next.getKey(), next.getValue());
        } catch (ExecutionException e) {
            // Possible problem in loading JournalMetadata from FoundationDB
            LOGGER.error(e.getCause().getMessage());
            throw new KronotopException(e.getCause());
        } catch (Exception e) {
            LOGGER.error("Failed to consume the next event", e);
            throw e;
        }
    }

    public Event consumeByVersionstamp(String journal, Versionstamp versionstamp) {
        return database.run(tr -> consumeByVersionstamp(tr, journal, versionstamp));
    }

    /**
     * Consumes the next event from a journal based on the given key.
     * <p>
     * If there is no next event, returns null.
     *
     * @param tr      The ReadTransaction to perform the operation on.
     * @param journal The journal to consume the event from.
     * @param key     The key to use for consuming the event.
     * @return The consumed event, or null if there is no next event.
     * @throws KronotopException If there is an error consuming the event.
     */
    public Event consumeNext(ReadTransaction tr, String journal, byte[] key) {
        try {
            JournalMetadata journalMetadata = cache.get(journal);
            Subspace subspace = new Subspace(journalMetadata.getIndexKey());
            if (key == null) {
                key = subspace.pack();
            }
            KeySelector begin = KeySelector.firstGreaterThan(key);
            KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(subspace.pack()));

            AsyncIterator<KeyValue> iterable = tr.getRange(begin, end, 1).iterator();
            if (!iterable.hasNext()) {
                return null;
            }
            KeyValue next = iterable.next();
            return new Event(next.getKey(), next.getValue());
        } catch (ExecutionException e) {
            // Possible problem in loading JournalMetadata from FoundationDB
            LOGGER.error(e.getCause().getMessage());
            throw new KronotopException(e.getCause());
        } catch (Exception e) {
            LOGGER.error("Failed to consume the next event", e);
            throw e;
        }
    }

    /**
     * Consumes the next event from a journal based on the given key.
     * <p>
     * If there is no next event, returns null.
     *
     * @param journal The journal to consume the event from.
     * @param key     The key to use for consuming the event.
     * @return The consumed event, or null if there is no next event.
     * @throws KronotopException If there is an error consuming the event.
     */
    public Event consumeNext(String journal, byte[] key) {
        return database.run(tr -> consumeNext(tr, journal, key));
    }
}