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
import com.kronotop.common.KronotopException;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.NotEnoughSpaceException;
import com.kronotop.volume.VersionstampedKeySelector;
import com.kronotop.volume.segment.Segment;
import com.kronotop.volume.segment.SegmentConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static com.kronotop.volume.Subspaces.ENTRY_SUBSPACE;
import static com.kronotop.volume.Subspaces.STREAMING_SUBSCRIBERS_SUBSPACE;

/**
 * This class represents a stage runner for watching changes in a database segment log.
 */
public class StreamingStageRunner extends ReplicationStageRunner implements StageRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingStageRunner.class);

    public StreamingStageRunner(Context context, ReplicationContext replicationContext) {
        super(context, replicationContext);
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
            SegmentConfig segmentConfig = new SegmentConfig(segmentId, volumeConfig.dataDir(), volumeConfig.segmentSize());
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
        // TODO: Should not use ENTRY_SUBSPACE
        byte[] packedKey = volumeConfig.subspace().pack(Tuple.from(ENTRY_SUBSPACE, key));
        KeySelector begin = KeySelector.firstGreaterThan(packedKey);
        begin.add(1);

        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(volumeConfig.subspace().pack(Tuple.from(ENTRY_SUBSPACE))));
        AsyncIterable<KeyValue> iterable = tr.getRange(begin, end, 1);

        for (KeyValue keyValue : iterable) {
            EntryMetadata metadata = EntryMetadata.decode(ByteBuffer.wrap(keyValue.getValue()));
            return Segment.extractIdFromName(metadata.segment());
        }
        throw new NoSegmentExistsException();
    }

    /**
     * Fetches and processes changes from the segment log.
     * This method continuously attempts to read from the segment log and update the replication slot
     * with the latest processed key or segment ID. If no new entries are found in the current segment,
     * it attempts to find the next segment to continue processing.
     * <p>
     * In case there are no more segments to process, the method exits the loop and waits for new changes.
     *
     * @throws NotEnoughSpaceException if there is not enough space to process the segment log entries.
     * @throws IOException             if an I/O error occurs during processing.
     */
    private void fetchChangesFromSegmentLog() throws NotEnoughSpaceException, IOException {
        while (true) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                ReplicationSlot slot = ReplicationSlot.load(tr, config, slotId);

                Versionstamp latestKey = slot.getReceivedVersionstampedKey() != null ? Versionstamp.fromBytes(slot.getReceivedVersionstampedKey()) : null;
                IterationResult iterationResult = iterateSegmentLogEntries(tr, slot.getLatestSegmentId(), latestKey);
                if (iterationResult.processedKeys() != 0) {
                    // Segment id not changed yet
                    ReplicationSlot.compute(tr, config, slotId, (replicationSlot) -> {
                        replicationSlot.setReceivedVersionstampedKey(iterationResult.latestKey().getBytes());
                    });
                    tr.commit().join();
                } else {
                    if (latestKey == null) {
                        // Nothing to stream from the primary owner, wait for changes.
                        break;
                    }
                    try {
                        long segmentId = findNextSegmentId(tr, latestKey);
                        ReplicationSlot.compute(tr, config, slotId, (replicationSlot) -> {
                            replicationSlot.setLatestSegmentId(segmentId);
                        });
                        tr.commit().join();
                    } catch (NoSegmentExistsException e) {
                        // Nothing to stream from the primary owner, wait for changes.
                        break;
                    }
                }
            }
        }
    }

    @Override
    public void run() {
        if (isStopped()) {
            return;
        }

        setActive(true);

        // Try to re-connect for half an hour.
        keepRunningWithMaxAttempt(360, Duration.ofSeconds(5), () -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                CompletableFuture<Void> watcher = keyWatcher.watch(tr, volumeConfig.subspace().pack(Tuple.from(STREAMING_SUBSCRIBERS_SUBSPACE)));
                tr.commit().join();

                fetchChangesFromSegmentLog();
                watcher.join();

                fetchChangesFromSegmentLog();
            } catch (NotEnoughSpaceException | IOException e) {
                // will be retried
                throw new KronotopException(e);
            }
        });
    }


    @Override
    public void stop() {
        keyWatcher.unwatch(volumeConfig.subspace().pack(Tuple.from(STREAMING_SUBSCRIBERS_SUBSPACE)));
        super.stop();
    }
}