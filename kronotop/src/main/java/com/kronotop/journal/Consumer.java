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
 * Consumes events from a journal with at-least-once delivery semantics.
 * Tracks consumption progress in FoundationDB and automatically recovers from transaction failures.
 */
public class Consumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
    private final static byte OFFSET_KEY = 0x01;
    private final Context context;
    private final ConsumerConfig config;
    private final JournalMetadata journalMetadata;
    private final byte[] offsetKey;
    private final byte[] initialOffset;
    private volatile byte[] offset;
    private volatile boolean started;
    private volatile boolean stopped;


    public Consumer(Context context, ConsumerConfig config) {
        this.context = context;
        this.config = config;
        this.journalMetadata = loadJournalMetadata();
        DirectorySubspace consumerSubspace = createOrOpenConsumerSubspace();
        this.offsetKey = consumerSubspace.pack(Tuple.from(OFFSET_KEY));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            this.initialOffset = inspectOffset(tr);
            this.offset = initialOffset;
        }
    }

    /**
     * Starts the consumer, enabling event consumption.
     */
    public void start() {
        // the start method is required for fine-grained flow control
        started = true;
    }

    /**
     * Creates or opens the FoundationDB subspace for this consumer's offset tracking.
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
     * Loads journal metadata from FoundationDB.
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
     * Resolves the initial offset based on configuration (LATEST, EARLIEST, or RESUME).
     *
     * @param tr Transaction for reading offset state.
     * @return Initial offset key.
     * @throws IllegalStateException if offset type is unsupported.
     */
    private byte[] inspectOffset(Transaction tr) {
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
     * Consumes the next event from the journal. Reloads offset checkpoint to recover from failed transactions.
     *
     * @param tr Transaction for reading events and offset state.
     * @return Next event, or null if no events available.
     * @throws IllegalConsumerStateException if consumer is not started or already stopped.
     */
    public Event consume(Transaction tr) {
        checkOperationStatus();

        try {
            // Reload offset from FDB to rewind to last committed checkpoint if previous transaction failed
            byte[] checkpoint = tr.get(offsetKey).join();
            if (checkpoint != null) {
                offset = checkpoint;
            } else {
                // No checkpoint in FDB yet, use initial offset
                offset = initialOffset;
            }

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
     * Marks an event as successfully processed by updating the offset. Changes are only persisted if transaction commits.
     *
     * @param tr Transaction to record offset update.
     * @param event Event that was processed.
     * @throws IllegalConsumerStateException if consumer is not started or already stopped.
     */
    public void markConsumed(Transaction tr, Event event) {
        checkOperationStatus();
        offset = event.key();
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
     * Stops the consumer, disabling further event consumption.
     *
     * @throws IllegalStateException if consumer was not started.
     */
    public void stop() {
        if (!started) {
            throw new IllegalStateException("Consumer is not started");
        }
        stopped = true;
    }
}
