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

/**
 * The Consumer class provides functionality to interact with a journal by consuming events,
 * managing offsets, and ensuring proper record of progress within a FoundationDB database setup.
 * The Consumer is configured with a specific journal and operates in a dedicated subspace
 * for tracking its activity.
 * <p>
 * This class includes methods for lifecycle management (starting and stopping),
 * offset inspection and updates, and consuming events from the associated journal.
 */
public class Consumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
    private final static byte OFFSET_KEY = 0x01;
    private final Context context;
    private final ConsumerConfig config;
    private final JournalMetadata journalMetadata;
    private final byte[] offsetKey;
    private byte[] offset;
    private boolean started;
    private boolean stopped;


    public Consumer(Context context, ConsumerConfig config) {
        this.context = context;
        this.config = config;
        this.journalMetadata = loadJournalMetadata();
        DirectorySubspace consumerSubspace = createOrOpenConsumerSubspace();
        this.offsetKey = consumerSubspace.pack(Tuple.from(OFFSET_KEY));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            this.offset = inspectOffset(tr);
        }
    }

    /**
     * Initiates the consumer by setting its internal state to started.
     * This method is essential for enabling operations like consuming events
     * and managing offsets. After calling this method, the consumer is considered
     * ready to interact with the journal.
     *
     * @throws IllegalStateException if the consumer is already started or in an invalid state to start.
     */
    public void start() {
        // start method is required for fine-grained flow control
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
     * Inspects the offset in the journal based on the configured offset type (LATEST, EARLIEST, or RESUME).
     * Determines the appropriate starting position for the consumer to interact with the journal.
     *
     * @param tr The read transaction used to interact with the journal and retrieve information about the current offset.
     * @return A byte array representing the resolved offset key for the journal based on the given configuration.
     * @throws IllegalStateException if the offset type is unsupported or invalid.
     */
    private byte[] inspectOffset(ReadTransaction tr) {
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
            byte[] value = tr.get(offsetKey).join();
            return value == null ? subspace.pack() : value;
        } else {
            throw new IllegalStateException("Unsupported offset: " + config.offset());
        }
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
            LOGGER.error("Failed to consume the next event for journal '{}'", config.journal(), e);
            throw e;
        }
    }

    /**
     * Updates the internal offset using the key of the provided event. Before updating, this method
     * checks whether the consumer is in a valid state for the operation.
     *
     * @param event The event whose key will be used to update the internal offset.
     *              The key represents the position of the event in the journal.
     * @throws IllegalConsumerStateException if the consumer has not been started or is already stopped.
     */
    public void setOffset(Event event) {
        checkOperationStatus();
        offset = event.key();
    }

    /**
     * Marks the completion of an operation by updating the transaction with the current offset.
     * Ensures that the consumer is in a valid operational state before proceeding.
     *
     * @param tr The transaction to be updated with the current offset value.
     *           This transaction is used to record the consumer's progress.
     * @throws IllegalConsumerStateException if the consumer has not been started
     *                                       or is already stopped.
     */
    public void complete(Transaction tr) {
        checkOperationStatus();
        tr.set(offsetKey, offset);
    }

    private void checkOperationStatus() {
        if (!started) {
            throw new IllegalConsumerStateException("Consumer is not started");
        }
        if (stopped) {
            throw new IllegalConsumerStateException("Consumer is already stopped");
        }
    }

    /**
     * Stops the consumer and marks it as no longer operational.
     * This method should be called when the consumer has completed its operations.
     *
     * @throws IllegalStateException if the consumer has not been started prior to invoking this method.
     */
    public void stop() {
        if (!started) {
            throw new IllegalStateException("Consumer is not started");
        }
        stopped = true;
    }
}
