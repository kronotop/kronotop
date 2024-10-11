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

package com.kronotop.volume.replication;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.KeyWatcher;
import com.kronotop.cluster.client.StatefulInternalConnection;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.NotEnoughSpaceException;
import com.kronotop.volume.VersionstampedKeySelector;
import com.kronotop.volume.segment.Segment;
import com.kronotop.volume.segment.SegmentConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.kronotop.volume.Subspaces.ENTRY_SUBSPACE;
import static com.kronotop.volume.Subspaces.VOLUME_STREAMING_SUBSCRIBERS_TRIGGER_SUBSPACE;

/**
 * This class represents a stage runner for watching changes in a database segment log.
 */
public class StreamingStageRunner extends ReplicationStageRunner implements StageRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingStageRunner.class);
    private final KeyWatcher keyWatcher = new KeyWatcher();
    private final AtomicBoolean isStreaming = new AtomicBoolean();

    public StreamingStageRunner(Context context, ReplicationConfig config, StatefulInternalConnection<byte[], byte[]> connection) {
        super(context, config, connection);
    }

    public String name() {
        return "Streaming";
    }

    /**
     * Iterates over the segment log entries.
     *
     * @param tr        the transaction
     * @param segmentId the ID of the segment
     * @param key       the key used to find the next segment ID
     * @return an IterationResult containing the latest key and the number of processed keys
     * @throws IOException             if an I/O error occurs
     * @throws NotEnoughSpaceException if there is not enough space
     */
    private IterationResult iterateSegmentLogEntries(Transaction tr, long segmentId, Versionstamp key) throws IOException, NotEnoughSpaceException {
        Segment segment = openSegments.get(segmentId);
        if (segment == null) {
            SegmentConfig segmentConfig = new SegmentConfig(segmentId, config.dataDir(), config.segmentSize());
            segment = new Segment(segmentConfig);
            openSegments.put(segmentId, segment);
        }

        // (begin, ...)
        VersionstampedKeySelector begin = null;
        if (key != null) {
            begin = VersionstampedKeySelector.firstGreaterThan(key);
            // There is no difference between firstGreaterThan and firstGreaterOrEqual. firstGreaterThan still returns the
            // begin-key. I don't understand why but calling add(1) fixes the problem.
            begin = begin.add(1);
        }

        return iterate(tr, segment, begin, null, MAXIMUM_BATCH_SIZE);
    }

    /**
     * Finds the next segment ID based on the provided transaction and key.
     *
     * @param tr  the transaction
     * @param key the key used to find the next segment ID
     * @return the next segment ID
     * @throws IllegalStateException if no new segment exists
     */
    private long findNextSegmentId(Transaction tr, Versionstamp key) {
        byte[] packedKey = config.subspace().pack(Tuple.from(ENTRY_SUBSPACE, key));
        KeySelector begin = KeySelector.firstGreaterThan(packedKey);
        begin.add(1);

        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(config.subspace().pack(Tuple.from(ENTRY_SUBSPACE))));
        AsyncIterable<KeyValue> iterable = tr.getRange(begin, end, 1);

        for (KeyValue keyValue : iterable) {
            EntryMetadata metadata = EntryMetadata.decode(ByteBuffer.wrap(keyValue.getValue()));
            return Segment.extractIdFromName(metadata.segment());
        }
        throw new NoSegmentExistsException();
    }

    /**
     * Fetches changes by iterating over segment log entries and finding the next segment ID.
     */
    private void streamChanges() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationSlot.compute(tr, config, (slot) -> {
                Versionstamp key = slot.getLatestVersionstampedKey() != null ? Versionstamp.fromBytes(slot.getLatestVersionstampedKey()) : null;
                try {
                    IterationResult iterationResult = iterateSegmentLogEntries(tr, slot.getLatestSegmentId(), key);
                    if (iterationResult.processedKeys() != 0) {
                        // Segment id not changed yet
                        slot.setLatestVersionstampedKey(iterationResult.latestKey().getBytes());
                        return;
                    }

                    if (key == null) {
                        LOGGER.atDebug()
                                .setMessage("It's not possible to find a new segmentId because key is null, slotId = {}")
                                .addArgument(config.stringifySlotId())
                                .log();
                        return;
                    }

                    // Need to find a new segment id
                    long segmentId = findNextSegmentId(tr, key);
                    slot.setLatestSegmentId(segmentId);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } catch (NotEnoughSpaceException e) {
                    throw new RuntimeException(e);
                } catch (NoSegmentExistsException e) {
                    LOGGER.atDebug()
                            .setMessage("No new segment found, slotId = {}")
                            .addArgument(config.stringifySlotId())
                            .log();
                }
            });
            tr.commit().join();
        }
    }

    protected boolean isStreaming() {
        return isStreaming.get();
    }

    /**
     * Watches for changes by continuously iterating over segment log entries and fetching events.
     * When the stage is stopped, the method terminates.
     */
    private void startStreaming() {
        while (!isStopped()) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                CompletableFuture<Void> watcher = keyWatcher.watch(tr, config.subspace().pack(Tuple.from(VOLUME_STREAMING_SUBSCRIBERS_TRIGGER_SUBSPACE)));
                tr.commit().join();

                try {
                    // stream segment log entries here
                    streamChanges();
                    isStreaming.set(true);
                    watcher.join();
                } catch (CancellationException e) {
                    LOGGER.atInfo()
                            .setMessage("{} stage has cancelled, slotId = {}")
                            .addArgument(name())
                            .addArgument(config.stringifySlotId())
                            .log();
                    return; // cancelled
                } finally {
                    isStreaming.set(false);
                }
                // fetch events here
                streamChanges();
            } catch (Exception e) {
                LOGGER.atError()
                        .setMessage("Error while watching changes, slotId = {}")
                        .addArgument(config.stringifySlotId())
                        .log();
                // Retrying...
            }
        }
    }

    /**
     * Finds the starting point for the replication slot by computing the latest segment ID and the latest versionstamped key.
     *
     * @throws RuntimeException if an error occurs during computation or transaction commit.
     */
    private void findStartingPoint() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationSlot.compute(tr, config, (slot) -> {
                if (slot.getSnapshots().isEmpty()) {
                    return;
                }
                Map.Entry<Long, Snapshot> entry = slot.getSnapshots().lastEntry();
                Snapshot snapshot = entry.getValue();
                slot.setLatestSegmentId(snapshot.getSegmentId());
                slot.setLatestVersionstampedKey(snapshot.getEnd());
            });
            tr.commit().join();
        }
    }

    @Override
    public void stop() {
        keyWatcher.unwatch(config.subspace().pack(Tuple.from(VOLUME_STREAMING_SUBSCRIBERS_TRIGGER_SUBSPACE)));
        super.stop();
    }

    @Override
    public void run() {
        if (isStopped()) {
            return;
        }

        try {
            findStartingPoint();
            startStreaming();
        } catch (Exception e) {
            LOGGER.atError().setMessage("{} stage has failed, slotId = {}").
                    addArgument(name()).
                    addArgument(config.stringifySlotId()).
                    setCause(e).
                    log();
        }
    }
}