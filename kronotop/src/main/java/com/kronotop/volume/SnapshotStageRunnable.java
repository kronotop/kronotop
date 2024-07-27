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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.VersionstampUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

class SnapshotStageRunnable extends ReplicationRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotStageRunnable.class);

    SnapshotStageRunnable(Context context, ReplicationConfig config) {
        super(context, config);
    }

    private boolean isSnapshotCompleted(Transaction tr, long segmentId) {
        ReplicationJob replicationJob = ReplicationJob.load(tr, config);
        Snapshot snapshot = replicationJob.getSnapshots().get(segmentId);
        return snapshot.getProcessedEntries() == snapshot.getTotalEntries();
    }

    private IterationResult iterate(Transaction tr, Segment segment, VersionstampedKeySelector begin, VersionstampedKeySelector end, int limit) throws NotEnoughSpaceException, IOException {
        SegmentLogIterable iterable = new SegmentLogIterable(tr, config.subspace(), segment.getName(), begin, end, limit);
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

    private IterationResult iterateSegmentLogEntries(Transaction tr, long segmentId) throws IOException, NotEnoughSpaceException {
        ReplicationJob replicationJob = ReplicationJob.load(tr, config);
        Snapshot snapshot = replicationJob.getSnapshots().get(segmentId);

        Segment segment = openSegments.get(segmentId);
        if (segment == null) {
            SegmentConfig segmentConfig = new SegmentConfig(segmentId, config.rootPath(), config.segmentSize());
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
                    ReplicationJob replicationJob = ReplicationJob.compute(tr, config, (job) -> {
                        Snapshot snapshot = job.getSnapshots().get(segmentId);
                        snapshot.setBegin(result.latestKey().getBytes());
                        snapshot.setProcessedEntries(result.processedKeys() + snapshot.getProcessedEntries());
                        snapshot.setLastUpdate(Instant.now().toEpochMilli());
                    });
                    tr.commit().join();

                    // The end key fetched: [begin, end]
                    Snapshot snapshot = replicationJob.getSnapshots().get(segmentId);
                    if (Arrays.equals(snapshot.getBegin(), snapshot.getEnd())) {
                        break;
                    }
                    continue;
                }
                break;
            } catch (IOException | NotEnoughSpaceException e) {
                LOGGER.error("Error while fetching segment logs", e);
            }
        }
    }

    private void snapshotLoop() {
        ReplicationJob replicationJob;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            replicationJob = ReplicationJob.load(tr, config);
        }

        if (replicationJob.getSnapshots().isEmpty()) {
            throw new IllegalStateException("No segment found to take a snapshot");
        }

        for (Map.Entry<Long, Snapshot> entry : replicationJob.getSnapshots().entrySet()) {
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
            ReplicationJob replicationJob = ReplicationJob.compute(tr, config, (job) -> {
                boolean completed = true;
                for (Map.Entry<Long, Snapshot> entry : job.getSnapshots().entrySet()) {
                    Snapshot snapshot = entry.getValue();
                    if (snapshot.getProcessedEntries() != snapshot.getTotalEntries()) {
                        completed = false;
                        break;
                    }
                }
                job.setSnapshotCompleted(completed);
            });
            tr.commit().join();
            if (replicationJob.isSnapshotCompleted()) {
                long totalProcessedEntries = 0;
                for (Map.Entry<Long, Snapshot> entry : replicationJob.getSnapshots().entrySet()) {
                    totalProcessedEntries += entry.getValue().getProcessedEntries();
                }
                LOGGER.info("ReplicationJob: {}, snapshot stage has completed. Number of processed keys: {}", VersionstampUtils.base64Encode(config.jobId()), totalProcessedEntries);
            }
        }
    }

    @Override
    public void run() {
        if (isStopped()) {
            return;
        }
        if (isStarted()) {
            throw new IllegalStateException("Snapshot stage is already started");
        }

        setStarted(true);

        LOGGER.info("ReplicationJob: {}, Snapshot stage has started", VersionstampUtils.base64Encode(config.jobId()));
        try {
            semaphore.acquire();
            snapshotLoop();
            isSnapshotCompleted();
        } catch (Exception e) {
            LOGGER.error("ReplicationJob: {}, snapshot stage has failed", config.jobId(), e);
        } finally {
            semaphore.release();
        }
    }
}