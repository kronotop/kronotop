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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.KeyWatcher;
import com.kronotop.cluster.client.protocol.SegmentRange;
import com.kronotop.volume.NotEnoughSpaceException;
import com.kronotop.volume.OperationKind;
import com.kronotop.volume.VersionstampedKeySelector;
import com.kronotop.volume.VolumeConfig;
import com.kronotop.volume.segment.Segment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CancellationException;

/**
 * Class responsible for managing and executing replication stages for segments.
 */
public class ReplicationStageRunner {
    protected static final int MAXIMUM_BATCH_SIZE = 100;
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationStageRunner.class);
    protected final Context context;
    protected final KeyWatcher keyWatcher = new KeyWatcher();
    protected final ReplicationConfig config;
    protected final VolumeConfig volumeConfig;
    protected final ReplicationClient client;
    protected final HashMap<Long, Segment> openSegments = new HashMap<>();
    protected Versionstamp slotId;
    private volatile boolean stopped = false;

    public ReplicationStageRunner(Context context, ReplicationContext replicationContext) {
        this.context = context;
        this.config = replicationContext.config();
        this.volumeConfig = replicationContext.volumeConfig();
        this.client = replicationContext.client();
        this.slotId = replicationContext.slotId();
    }

    protected ReplicationSlot loadReplicationSlot() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return loadReplicationSlot(tr);
        }
    }

    protected ReplicationSlot loadReplicationSlot(Transaction tr) {
        return ReplicationSlot.load(tr, config, slotId);
    }

    protected void setActive(boolean active) {
        context.getFoundationDB().run(tr -> {
            ReplicationSlot.compute(tr, config, slotId, (slot) -> {
                slot.setActive(active);
            });
            return null;
        });
    }

    /**
     * Iterates over log entries for a specified segment, fetching data ranges and inserting them into the segment.
     *
     * @param tr      The transaction context within which the iteration occurs.
     * @param segment The target segment for processing log entries and data ranges.
     * @param begin   The starting key selector for the iteration.
     * @param end     The ending key selector for the iteration.
     * @param limit   The maximum number of log entries to process in one iteration.
     * @return An IterationResult containing the latest processed key and the number of processed keys.
     * @throws NotEnoughSpaceException If there is insufficient space to process the entries.
     * @throws IOException             If an I/O error occurs during processing.
     */
    protected IterationResult iterate(Transaction tr, Segment segment, VersionstampedKeySelector begin, VersionstampedKeySelector end, int limit) throws NotEnoughSpaceException, IOException {
        SegmentLogIterable iterable = new SegmentLogIterable(tr, volumeConfig.subspace(), segment.getName(), begin, end, limit);
        List<SegmentLogEntry> segmentLogEntries = new ArrayList<>();
        for (SegmentLogEntry entry : iterable) {
            segmentLogEntries.add(entry);
        }

        if (segmentLogEntries.isEmpty()) {
            return new IterationResult(null, 0);
        }

        List<Object> dataRanges = fetchSegmentRange(segment.getName(), segmentLogEntries);
        insertSegmentRange(segment, segmentLogEntries, dataRanges);
        return new IterationResult(segmentLogEntries.getLast().key(), segmentLogEntries.size());
    }

    /**
     * Inserts a range of data into the specified segment based on the given log entries.
     *
     * @param segment   The segment into which the data will be inserted.
     * @param entries   The list of log entries that describe how the data should be inserted.
     * @param dataRange The list of data objects to be inserted into the segment.
     * @throws IOException             If an I/O error occurs during the insert operation.
     * @throws NotEnoughSpaceException If there is not enough space in the segment to insert the data.
     */
    protected void insertSegmentRange(Segment segment, List<SegmentLogEntry> entries, List<Object> dataRange) throws IOException, NotEnoughSpaceException {
        for (int i = 0; i < dataRange.size(); i++) {
            SegmentLogEntry entry = entries.get(i);
            byte[] data = (byte[]) dataRange.get(i);
            segment.insert(ByteBuffer.wrap(data), entry.value().position());
        }
        segment.flush(true);
    }

    /**
     * Fetches the data ranges for a specified segment from the log entries and returns them as a list of objects.
     * Skips entries marked as DELETE since they are meant for the vacuuming process.
     *
     * @param segmentName the name of the segment for which data ranges need to be fetched
     * @param entries     the list of log entries from which data ranges are to be derived
     * @return a list of objects representing the fetched data ranges for the specified segment
     */
    protected List<Object> fetchSegmentRange(String segmentName, List<SegmentLogEntry> entries) {
        int size = 0;
        for (SegmentLogEntry entry : entries) {
            if (entry.value().kind().equals(OperationKind.APPEND) || entry.value().kind().equals(OperationKind.VACUUM)) {
                // Do not need to fetch the deleted entry, OperationKind.Delete should be
                // used for the vacuuming process.
                size++;
            }
        }

        int index = 0;
        SegmentRange[] segmentRanges = new SegmentRange[size];
        for (SegmentLogEntry entry : entries) {
            if (entry.value().kind().equals(OperationKind.DELETE)) {
                // Do not need to fetch the deleted entry, OperationKind.Delete should be
                // used for the vacuuming process.
                continue;
            }
            segmentRanges[index] = new SegmentRange(entry.value().position(), entry.value().length());
            index++;
        }
        return client.connection().sync().segmentrange(volumeConfig.name(), segmentName, segmentRanges);
    }

    public void stop() {
        if (stopped) {
            return;
        }

        stopped = true;
        setActive(false);

        for (Segment segment : openSegments.values()) {
            try {
                segment.close();
            } catch (Exception e) {
                LOGGER.atError().
                        setMessage("Error while closing a segment, slotId = {}").
                        addArgument(ReplicationMetadata.stringifySlotId(slotId)).
                        setCause(e).
                        log();
            }
        }
    }

    /**
     * Executes a given Runnable task with a specified maximum number of attempts, waiting for the defined interval between
     * attempts. The execution loop will terminate if the task is successfully completed and the breakOnSuccess flag is true,
     * or if the maximum number of attempts is reached.
     *
     * @param maxAttempts    the maximum number of attempts to execute the given runnable before stopping.
     * @param interval       the duration to wait between consecutive attempts expressed as a Duration object.
     * @param breakOnSuccess if true, the method stops attempting the task after a successful execution.
     * @param runnable       the Runnable task to be executed.
     */
    private void runWithMaxAttempt_internal(int maxAttempts, Duration interval, boolean breakOnSuccess, Runnable runnable) {
        int attempts = 0;
        while (!isStopped()) {
            if (attempts >= maxAttempts) {
                // Failed, stop the replication
                ReplicationSlot replicationSlot = context.getFoundationDB().run(tr -> {
                    ReplicationSlot slot = ReplicationSlot.load(tr, config, slotId);
                    slot.setActive(false);
                    return slot;
                });
                LOGGER.warn("Replication with slot id {} has stopped, current ReplicationStage: {}",
                        ReplicationMetadata.stringifySlotId(slotId),
                        replicationSlot.getReplicationStage()
                );
                break;
            }

            try {
                // There is no connection at all. First, try to connect to the primary owner.
                if (!client.hasConnection()) {
                    client.tryConnect();
                }
                runnable.run();
                if (breakOnSuccess) {
                    break;
                }
                attempts = 0;
            } catch (CancellationException e) {
                // Watcher canceled, break the loop.
                break;
            } catch (Exception e) {
                attempts++;
                String id = ReplicationMetadata.stringifySlotId(slotId);
                LOGGER.atError().setMessage("Error while running replication, slotId = {}").addArgument(id).setCause(e).log();
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException ex) {
                    // TODO: Do we need this call?
                    Thread.currentThread().interrupt();
                    break;
                }
                // Retrying...
            }
        }
    }

    /**
     * Executes the specified runnable with a maximum number of attempts, waiting for the given interval
     * between each attempt.
     *
     * @param maxAttempts the maximum number of attempts to execute the runnable before stopping
     * @param interval    the duration to wait between consecutive attempts
     * @param runnable    the task to be executed
     */
    protected void runWithMaxAttempt(int maxAttempts, Duration interval, Runnable runnable) {
        runWithMaxAttempt_internal(maxAttempts, interval, true, runnable);
    }

    /**
     * Executes the specified runnable continuously with a maximum number of attempts, waiting for the given interval
     * between each attempt. The task does not stop on success and keeps running until stopped or the maximum attempts
     * are reached.
     *
     * @param maxAttempts the maximum number of attempts to execute the runnable before stopping
     * @param interval    the duration to wait between consecutive attempts
     * @param runnable    the task to be executed
     */
    protected void keepRunningWithMaxAttempt(int maxAttempts, Duration interval, Runnable runnable) {
        runWithMaxAttempt_internal(maxAttempts, interval, false, runnable);
    }

    protected boolean isStopped() {
        return stopped;
    }

    protected record IterationResult(Versionstamp latestKey, int processedKeys) {
    }
}
