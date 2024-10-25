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
import com.kronotop.cluster.client.StatefulInternalConnection;
import com.kronotop.volume.NotEnoughSpaceException;
import com.kronotop.volume.VersionstampedKeySelector;
import com.kronotop.volume.segment.Segment;
import com.kronotop.volume.segment.SegmentConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;

public class SnapshotStageRunner extends ReplicationStageRunner implements StageRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotStageRunner.class);

    SnapshotStageRunner(Context context, ReplicationConfig config, StatefulInternalConnection<byte[], byte[]> connection) {
        super(context, config, connection);
    }

    public String name() {
        return "Snapshot";
    }

    private boolean isSnapshotCompleted(Transaction tr, long segmentId) {
        ReplicationSlot replicationSlot = ReplicationSlot.load(tr, config);
        Snapshot snapshot = replicationSlot.getSnapshots().get(segmentId);
        return snapshot.getProcessedEntries() == snapshot.getTotalEntries();
    }

    private IterationResult iterateSegmentLogEntries(Transaction tr, long segmentId) throws IOException, NotEnoughSpaceException {
        ReplicationSlot replicationSlot = ReplicationSlot.load(tr, config);
        Snapshot snapshot = replicationSlot.getSnapshots().get(segmentId);

        Segment segment = openSegments.get(segmentId);
        if (segment == null) {
            SegmentConfig segmentConfig = new SegmentConfig(segmentId, config.dataDir(), config.segmentSize());
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

    private void snapshotLoopOnSegment(long segmentId) {
        // Take a snapshot
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
                    ReplicationSlot replicationSlot = ReplicationSlot.compute(tr, config, (slot) -> {
                        Snapshot snapshot = slot.getSnapshots().get(segmentId);
                        snapshot.setBegin(result.latestKey().getBytes());
                        snapshot.setProcessedEntries(result.processedKeys() + snapshot.getProcessedEntries());
                        snapshot.setLastUpdate(Instant.now().toEpochMilli());
                    });
                    tr.commit().join();

                    // The end key fetched: [begin, end]
                    Snapshot snapshot = replicationSlot.getSnapshots().get(segmentId);
                    if (Arrays.equals(snapshot.getBegin(), snapshot.getEnd())) {
                        break;
                    }
                    continue;
                }
                break;
            } catch (IOException | NotEnoughSpaceException e) {
                LOGGER.atError().setMessage("An error has occurred while running {} stage, retrying, slotId = {}").
                        addArgument(name()).
                        addArgument(config.stringifySlotId()).
                        setCause(e).
                        log();
            }
        }
    }

    private void snapshotLoop() {
        ReplicationSlot replicationSlot;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            replicationSlot = ReplicationSlot.load(tr, config);
        }

        if (replicationSlot.getSnapshots().isEmpty()) {
            throw new IllegalStateException("No segment found to take a snapshot");
        }

        for (Map.Entry<Long, Snapshot> entry : replicationSlot.getSnapshots().entrySet()) {
            if (isStopped()) {
                // Replication has stopped.
                break;
            }

            Snapshot snapshot = entry.getValue();
            if (snapshot.getProcessedEntries() == snapshot.getTotalEntries()) {
                // Completed
                continue;
            }
            snapshotLoopOnSegment(entry.getKey());
        }
    }

    private void isSnapshotCompleted() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationSlot replicationSlot = ReplicationSlot.compute(tr, config, (slot) -> {
                boolean completed = true;
                for (Map.Entry<Long, Snapshot> entry : slot.getSnapshots().entrySet()) {
                    Snapshot snapshot = entry.getValue();
                    if (snapshot.getProcessedEntries() != snapshot.getTotalEntries()) {
                        completed = false;
                        break;
                    }
                }
                slot.setSnapshotCompleted(completed);
            });
            tr.commit().join();
            if (replicationSlot.isSnapshotCompleted()) {
                long totalProcessedEntries = 0;
                for (Map.Entry<Long, Snapshot> entry : replicationSlot.getSnapshots().entrySet()) {
                    totalProcessedEntries += entry.getValue().getProcessedEntries();
                }
                LOGGER.atInfo().setMessage("{} stage has completed, slotId = {}").
                        addArgument(name()).
                        addArgument(config.stringifySlotId()).
                        log();
                LOGGER.atInfo().setMessage("Number of processed keys during {} stage: {}, slotId = {}").
                        addArgument(name()).
                        addArgument(totalProcessedEntries).
                        addArgument(config.stringifySlotId()).
                        log();
            }
        }
    }

    @Override
    public void run() {
        try {
            snapshotLoop();
            isSnapshotCompleted();
        } catch (Exception e) {
            LOGGER.atError().setMessage("{} stage has failed, slotId = {}").
                    addArgument(name()).
                    addArgument(config.stringifySlotId()).
                    setCause(e).
                    log();
        }
    }
}