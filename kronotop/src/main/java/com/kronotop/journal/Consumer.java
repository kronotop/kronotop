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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.Context;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class Consumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
    private final static byte OFFSET_KEY = 0x01;
    private final Context context;
    private final ConsumerConfig config;
    private final JournalMetadata journalMetadata;
    private final DirectorySubspace consumerSubspace;
    private byte[] offset;
    private boolean started;
    private boolean stopped;


    public Consumer(Context context, ConsumerConfig config) {
        this.context = context;
        this.config = config;
        this.journalMetadata = this.loadJournalMetadata();
        this.consumerSubspace = this.createOrOpenConsumerSubspace();
    }

    public void start() {
        // start method is required for fine-grained flow control
        offset = this.inspectOffset();
        started = true;
    }

    /**
     * Creates or opens a consumer subspace in FoundationDB based on the consumer's configuration
     * and cluster context. The method ensures that the required directory structure is created
     * or retrieved for the consumer to operate within the appropriate subspace.
     *
     * @return an instance of {@code DirectorySubspace} representing the consumer's subspace
     * in FoundationDB, which allows for interaction with the underlying database.
     */
    private DirectorySubspace createOrOpenConsumerSubspace() {
        assert context != null;
        assert config != null;

        KronotopDirectoryNode directory =
                KronotopDirectory.
                        kronotop().
                        cluster(context.getClusterName()).
                        journals().
                        journal(config.journal()).
                        consumers().
                        consumer(config.id());
        return context.getFoundationDB().run(tr -> DirectoryLayer.getDefault().createOrOpen(tr, directory.toList()).join());
    }

    /**
     * Loads the metadata associated with a journal. This method interacts with the FoundationDB
     * database to create or open the specified journal's subspace and constructs the journal's metadata
     * using the resolved subspace.
     *
     * @return an instance of {@code JournalMetadata} containing the metadata of the journal.
     */
    private JournalMetadata loadJournalMetadata() {
        assert context != null;
        assert config != null;

        KronotopDirectoryNode directory =
                KronotopDirectory.
                        kronotop().
                        cluster(context.getClusterName()).
                        journals().
                        journal(config.journal());
        return context.getFoundationDB().run(tr -> {
            DirectorySubspace subspace = DirectoryLayer.getDefault().createOrOpen(tr, directory.toList()).join();
            return new JournalMetadata(subspace);
        });
    }

    /**
     * Inspects the current offset in the journal and determines the appropriate starting point
     * based on the consumer configuration. This method interacts with the underlying FoundationDB
     * instance to resolve the offset to a byte array.
     *
     * @return A byte array representing the resolved offset key. This is determined based on the
     * consumer's `offset` setting: EARLIEST, LATEST, or RESUME. Throws an exception for
     * unsupported offset types.
     */
    private byte[] inspectOffset() {
        return context.getFoundationDB().read(tr -> {
            Subspace subspace = journalMetadata.eventsSubspace();
            if (config.offset().equals(ConsumerConfig.Offset.LATEST)) {
                AsyncIterator<KeyValue> iterator = tr.getRange(subspace.range(), 1, true).iterator();
                if (!iterator.hasNext()) {
                    return subspace.pack();
                }
                return iterator.next().getKey();
            } else if (config.offset().equals(ConsumerConfig.Offset.EARLIEST)) {
                return subspace.pack();
            } else if (config.offset().equals(ConsumerConfig.Offset.RESUME)) {
                byte[] key = consumerSubspace.pack(Tuple.from(OFFSET_KEY));
                byte[] value = tr.get(key).join();
                return value == null ? subspace.pack() : value;
            } else {
                throw new IllegalStateException("Unsupported offset: " + config.offset());
            }
        });
    }

    /**
     * Consumes the next available event from the journal by interacting
     * with the provided read transaction. If no more events are available
     * for consumption, returns {@code null}.
     *
     * @param tr The read transaction used to fetch the next event from the journal.
     * @return An {@code Event} object representing the consumed event, or
     * {@code null} if no events are available.
     * @throws IllegalStateException if the consumer is not in a valid state
     *                               to consume events (e.g., not started or already stopped).
     */
    public Event consume(ReadTransaction tr) {
        checkOperationStatus();

        try {
            Subspace subspace = journalMetadata.eventsSubspace();
            KeySelector begin = KeySelector.firstGreaterThan(offset);
            KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(subspace.pack()));

            AsyncIterator<KeyValue> iterable = tr.getRange(begin, end, 1).iterator();
            if (!iterable.hasNext()) {
                return null;
            }
            KeyValue next = iterable.next();
            Entry entry = Entry.decode(ByteBuffer.wrap(next.getValue()));
            return new Event(next.getKey(), entry.event());
        } catch (Exception e) {
            LOGGER.error("Failed to consume the next event for journal '{}', key: {}", config.journal(), ByteArrayUtil.printable(offset), e);
            throw e;
        }
    }

    /**
     * Marks a specific event as consumed by recording its key in the consumer's offset and updating
     * the transaction state.
     *
     * @param tr    The transaction in which the event consumption is recorded.
     * @param event The event that has been consumed, whose key is to be recorded.
     */
    public void markConsumed(Transaction tr, Event event) {
        checkOperationStatus();

        byte[] key = consumerSubspace.pack(Tuple.from(OFFSET_KEY));
        tr.set(key, event.key());
        offset = event.key();
    }

    private void checkOperationStatus() {
        if (!started) {
            throw new IllegalConsumerStateException("Consumer is not started");
        }
        if (stopped) {
            throw new IllegalConsumerStateException("Consumer is already stopped");
        }
    }

    public void stop() {
        if (!started) {
            throw new IllegalStateException("Consumer is not started");
        }
        stopped = true;
    }
}
