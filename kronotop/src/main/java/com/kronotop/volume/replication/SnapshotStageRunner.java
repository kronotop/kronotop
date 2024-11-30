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
import com.kronotop.VersionstampUtils;
import com.kronotop.volume.NotEnoughSpaceException;
import com.kronotop.volume.VersionstampedKeySelector;
import com.kronotop.volume.segment.Segment;
import com.kronotop.volume.segment.SegmentConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;

public class SnapshotStageRunner extends ReplicationStageRunner implements StageRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotStageRunner.class);

    SnapshotStageRunner(Context context, ReplicationContext replicationContext) {
        super(context, replicationContext);
    }

    public String name() {
        return "Snapshot";
    }

    /**
     * Checks if the snapshot for the specified segment has been completed.
     *
     * @param tr        the transaction context
     * @param segmentId the identifier of the segment whose snapshot is being checked
     * @return true if the snapshot is completed, false otherwise
     */
    private boolean isSnapshotCompleted(Transaction tr, long segmentId) {
        ReplicationSlot slot = ReplicationSlot.load(tr, config, slotId);
        Snapshot snapshot = slot.getSnapshots().get(segmentId);
        return Arrays.equals(snapshot.getBegin(), snapshot.getEnd());
    }

    /**
     * Iterates over the log entries of a segment within a transaction context.
     * It retrieves and processes entries from the specified segment based on the
     * snapshot information stored in the replication slot.
     *
     * @param tr        the transaction context in which to perform the iteration.
     * @param segmentId the identifier of the segment to be iterated.
     * @return an IterationResult object containing the latest processed key and the number of processed keys.
     * @throws IOException             if an I/O error occurs during iteration.
     * @throws NotEnoughSpaceException if there is not enough space to process the segment.
     */
    private IterationResult iterateSegmentLogEntries(Transaction tr, long segmentId) throws IOException, NotEnoughSpaceException {
        ReplicationSlot replicationSlot = ReplicationSlot.load(tr, config, slotId);
        Snapshot snapshot = replicationSlot.getSnapshots().get(segmentId);

        Segment segment = openSegments.get(segmentId);
        if (segment == null) {
            SegmentConfig segmentConfig = new SegmentConfig(segmentId, volumeConfig.dataDir(), volumeConfig.segmentSize());
            segment = new Segment(segmentConfig);
            openSegments.put(segmentId, segment);
        }

        // [begin, end)
        VersionstampedKeySelector begin; // inclusive
        if (snapshot.getProcessedEntries() == 0) {
            begin = VersionstampedKeySelector.firstGreaterOrEqual(Versionstamp.fromBytes(snapshot.getBegin()));
        } else {
            begin = VersionstampedKeySelector.firstGreaterThan(Versionstamp.fromBytes(snapshot.getBegin()));
            // There is no difference between firstGreaterThan and firstGreaterOrEqual. firstGreaterThan still returns the
            // begin-key. I don't understand why but calling add(1) fixes the problem.
            begin = begin.add(1);
        }
        VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterThan(Versionstamp.fromBytes(snapshot.getEnd())); // exclusive

        IterationResult iterationResult = iterate(tr, segment, begin, end, MAXIMUM_BATCH_SIZE);
        if (iterationResult.processedKeys() == 0) {
            // Fetch the end key to fulfill the condition: [begin, end]
            begin = VersionstampedKeySelector.firstGreaterOrEqual(end.getKey());
            iterationResult = iterate(tr, segment, begin, null, 1);
        }

        return iterationResult;
    }

    /**
     * Continuously copies a segment from its original source until the entire segment is replicated or a stop condition is met.
     *
     * @param segmentId the identifier of the segment to be copied.
     */
    private void snapshotLoopOnSegment(long segmentId) {
        // Copy the segment from its original source
        while (true) {
            if (isStopped()) {
                // Replication has stopped.
                break;
            }

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                if (isSnapshotCompleted(tr, segmentId)) {
                    break;
                }
                IterationResult result = iterateSegmentLogEntries(tr, segmentId);
                if (result.processedKeys() > 0) {
                    // Copied some data from the source, update the ReplicationSlot.
                    ReplicationSlot replicationSlot = ReplicationSlot.compute(tr, config, slotId, (slot) -> {
                        // Update the snapshot metadata
                        Snapshot snapshot = slot.getSnapshots().get(segmentId);
                        snapshot.setBegin(result.latestKey().getBytes());
                        snapshot.setProcessedEntries(result.processedKeys() + snapshot.getProcessedEntries());
                        snapshot.setLastUpdate(Instant.now().toEpochMilli());

                        // Update the slot
                        slot.setLatestVersionstampedKey(result.latestKey().getBytes());
                    });
                    tr.commit().join();

                    // The end key fetched: [begin, end]
                    Snapshot snapshot = replicationSlot.getSnapshots().get(segmentId);
                    if (Arrays.equals(snapshot.getBegin(), snapshot.getEnd())) {
                        // Copied everything from the source member.
                        break;
                    }
                    continue;
                }
                break;
            } catch (IOException | NotEnoughSpaceException e) {
                LOGGER.atError().setMessage("An error has occurred while running {} stage, retrying, slotId = {}").
                        addArgument(name()).
                        addArgument(ReplicationMetadata.stringifySlotId(slotId)).
                        setCause(e).
                        log();
            }
        }
    }

    /**
     * Iterates through the snapshot segments recorded in the replication slot.
     * For each snapshot segment, it checks if replication has been stopped.
     * If the segment is fully replicated, it is skipped.
     * Otherwise, continue processing the segment via the snapshot loop.
     */
    private void iterateOverSnapshotSegments() {
        ReplicationSlot slot = loadReplicationSlot();
        for (Map.Entry<Long, Snapshot> entry : slot.getSnapshots().entrySet()) {
            if (isStopped()) {
                // Replication has stopped.
                break;
            }
            Snapshot snapshot = entry.getValue();
            if (Arrays.equals(snapshot.getBegin(), snapshot.getEnd())) {
                // This segment already copied from the source.
                continue;
            }
            snapshotLoopOnSegment(entry.getKey());
        }
    }

    /**
     * Checks the replication status by analyzing the snapshots in the replication slot.
     * It identifies whether all snapshots have completed and marks the slot accordingly.
     *
     * @return the replication slot with updated snapshot completion status.
     */
    private ReplicationSlot checkReplicationStatus() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationSlot replicationSlot = ReplicationSlot.compute(tr, config, slotId, (slot) -> {
                boolean completed = true;
                for (Map.Entry<Long, Snapshot> entry : slot.getSnapshots().entrySet()) {
                    Snapshot snapshot = entry.getValue();
                    if (!Arrays.equals(snapshot.getBegin(), snapshot.getEnd())) {
                        completed = false;
                        break;
                    }
                }
                if (completed) {
                    slot.completeReplicationStage(ReplicationStage.SNAPSHOT);
                }
            });
            tr.commit().join();
            return replicationSlot;
        }
    }

    private void report() {
        ReplicationSlot slot = checkReplicationStatus();
        if (slot.isReplicationStageCompleted(ReplicationStage.SNAPSHOT)) {
            long totalProcessedEntries = 0;
            for (Map.Entry<Long, Snapshot> entry : slot.getSnapshots().entrySet()) {
                totalProcessedEntries += entry.getValue().getProcessedEntries();
            }
            LOGGER.atInfo().setMessage("{} stage has completed, slotId = {}").
                    addArgument(name()).
                    addArgument(ReplicationMetadata.stringifySlotId(slotId)).
                    log();
            LOGGER.atInfo().setMessage("Number of processed keys during {} stage: {}, slotId = {}").
                    addArgument(name()).
                    addArgument(totalProcessedEntries).
                    addArgument(ReplicationMetadata.stringifySlotId(slotId)).
                    log();
        }
    }

    /**
     * Executes the snapshot stage of replication.
     * This method attempts to iterate over snapshot segments and report the progress,
     * retrying the operation with a defined maximum number of attempts and interval between retries.
     * <p>
     * It marks the replication process as active and uses the `runWithMaxAttempt` method to handle retries.
     * During each attempt, it calls `iterateOverSnapshotSegments` to process each segment, and `report`
     * to log the replication status. If an exception occurs during the attempt, an error is logged,
     * and the exception is rethrown.
     */
    @Override
    public void run() {
        setActive(true);

        // Try to re-connect for half an hour.
        runWithMaxAttempt(360, Duration.ofSeconds(5), () -> {
            try {
                iterateOverSnapshotSegments();
                report();
            } catch (Exception e) {
                LOGGER.atError().setMessage("{} stage has failed, slotId = {}").
                        addArgument(name()).
                        addArgument(ReplicationMetadata.stringifySlotId(slotId)).
                        setCause(e).
                        log();
                throw e;
            }
        });
    }
}