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
import com.kronotop.cluster.Member;
import com.kronotop.cluster.client.InternalClient;
import com.kronotop.cluster.client.StatefulInternalConnection;
import com.kronotop.cluster.client.protocol.SegmentRange;
import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class Replication {
    private static final Logger LOGGER = LoggerFactory.getLogger(Replication.class);
    private static final int MAXIMUM_BATCH_SIZE = 100;
    private final Context context;
    private final ReplicationConfig config;
    private final RedisClient client;
    private final StatefulInternalConnection<byte[], byte[]> connection;
    private final HashMap<Long, Segment> openSegments = new HashMap<>();
    private final Semaphore semaphore = new Semaphore(1);
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicReference<Future<?>> snapshotFuture = new AtomicReference<>();
    private final AtomicReference<Future<?>> changeDataCaptureFuture = new AtomicReference<>();
    private volatile boolean started = false;
    private volatile boolean stopped = false;

    public Replication(Context context, ReplicationConfig config) {
        this.context = context;
        this.config = config;
        Member member = config.source().member();
        this.client = RedisClient.create(
                String.format("redis://%s:%d", member.getAddress().getHost(), member.getAddress().getPort())
        );
        this.connection = InternalClient.connect(client, ByteArrayCodec.INSTANCE);
    }

    public AtomicReference<Future<?>> getSnapshotFuture() {
        return snapshotFuture;
    }

    private boolean isSnapshotCompleted(Transaction tr) {
        ReplicationJob replicationJob = ReplicationJob.load(tr, config);
        return replicationJob.isSnapshotCompleted();
    }

    public void start() throws IOException {
        if (started) {
            throw new IllegalStateException("Replication is already started");
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            if (isSnapshotCompleted(tr)) {
                changeDataCaptureFuture.set(executor.submit(new ChangeDataCaptureRunner(this)));
            } else {
                snapshotFuture.set(executor.submit(new SnapshotStageRunner(this)));
            }
        }

        started = true;
        LOGGER.info("Replication job: {} started", config.jobId());
    }

    public void stop() throws IOException {
        if (!started) {
            throw new IllegalStateException("Replication is not started");
        }

        stopped = true;
        for (Segment segment : openSegments.values()) {
            segment.close();
        }
        try {
            executor.close();
            client.shutdown();
        } finally {
            started = false;
        }
        LOGGER.info("Replication job: {} stopped", config.jobId());
    }

    private void insertSegmentRange(Segment segment, List<SegmentLogEntry> entries, List<Object> dataRange) throws IOException, NotEnoughSpaceException {
        for (int i = 0; i < dataRange.size(); i++) {
            byte[] data = (byte[]) dataRange.get(i);
            SegmentLogEntry entry = entries.get(i);
            segment.insert(ByteBuffer.wrap(data), entry.value().position());
        }
        segment.flush(true);
    }

    private List<Object> fetchSegmentRange(String segmentName, List<SegmentLogEntry> entries) {
        SegmentRange[] segmentRanges = new SegmentRange[entries.size()];
        for (int i = 0; i < entries.size(); i++) {
            SegmentLogEntry entry = entries.get(i);
            segmentRanges[i] = new SegmentRange(entry.value().position(), entry.value().length());
        }
        return connection.sync().segmentRange(config.volumeName(), segmentName, segmentRanges);
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

    private record IterationResult(Versionstamp latestKey, int processedKeys) {
    }

    private static class SnapshotStageRunner implements Runnable {
        private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotStageRunner.class);
        private final Context context;
        private final ReplicationConfig config;
        private final Replication replication;

        public SnapshotStageRunner(Replication replication) {
            this.replication = replication;
            this.context = replication.context;
            this.config = replication.config;
        }

        private boolean isSnapshotCompleted(Transaction tr, long segmentId) {
            ReplicationJob replicationJob = ReplicationJob.load(tr, config);
            Snapshot snapshot = replicationJob.getSnapshots().get(segmentId);
            return snapshot.getProcessedEntries() == snapshot.getTotalEntries();
        }

        private void snapshotLoopOnSegment(long segmentId) {
            // Take a snapshot
            while (true) {
                if (replication.stopped) {
                    // Replication has stopped.
                    break;
                }

                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    if (isSnapshotCompleted(tr, segmentId)) {
                        break;
                    }
                    IterationResult result = replication.iterateSegmentLogEntries(tr, segmentId);
                    if (result.processedKeys > 0) {
                        ReplicationJob job = ReplicationJob.compute(tr, config.subspace(), context.getMember(), config.jobId(), (replicationJob) -> {
                            Snapshot snapshot = replicationJob.getSnapshots().get(segmentId);
                            snapshot.setBegin(result.latestKey.getBytes());
                            snapshot.setProcessedEntries(result.processedKeys + snapshot.getProcessedEntries());
                            snapshot.setLastUpdate(Instant.now().toEpochMilli());
                        });
                        tr.commit().join();

                        // The end key fetched: [begin, end]
                        Snapshot snapshot = job.getSnapshots().get(segmentId);
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
            for (Map.Entry<Long, Snapshot> entry : replicationJob.getSnapshots().entrySet()) {
                if (replication.stopped) {
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
                ReplicationJob replicationJob = ReplicationJob.compute(tr, config.subspace(), context.getMember(), config.jobId(), (job) -> {
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
                    LOGGER.info("ReplicationJob: {}, snapshot stage has completed. Number of processed keys: {}", config.jobId(), totalProcessedEntries);
                    replication.changeDataCaptureFuture.set(replication.executor.submit(new ChangeDataCaptureRunner(replication)));
                }
            }
        }

        @Override
        public void run() {
            if (replication.stopped) {
                return;
            }

            try {
                replication.semaphore.acquire();
                snapshotLoop();
                isSnapshotCompleted();
            } catch (Exception e) {
                LOGGER.error("ReplicationJob: {}, snapshot stage has failed", replication.config.jobId(), e);
            } finally {
                replication.semaphore.release();
            }
        }
    }

    private static class ChangeDataCaptureRunner implements Runnable {
        private static final Logger LOGGER = LoggerFactory.getLogger(ChangeDataCaptureRunner.class);
        private final Replication replication;
        private final Context context;
        private final ReplicationConfig config;

        public ChangeDataCaptureRunner(Replication replication) {
            this.replication = replication;
            this.context = replication.context;
            this.config = replication.config;
        }

        private void watchChanges() {
            try (Transaction tr = replication.context.getFoundationDB().createTransaction()) {

            }
        }

        private ReplicationJob startChangeDataCapture() {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                ReplicationJob replicationJob = ReplicationJob.compute(tr, config.subspace(), context.getMember(), config.jobId(), (job) -> {
                    Map.Entry<Long, Snapshot> entry = job.getSnapshots().lastEntry();
                    if (entry == null) {
                        throw new IllegalStateException("ReplicationJob: " + config.jobId() + " has no snapshot");
                    }
                    Snapshot snapshot = entry.getValue();
                    job.setLatestSegmentId(snapshot.getSegmentId());
                    job.setLatestVersionstampedKey(snapshot.getEnd());
                });
                tr.commit().join();
                return replicationJob;
            }
        }

        @Override
        public void run() {
            if (replication.stopped) {
                return;
            }

            Future<?> future = replication.getSnapshotFuture().get();
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Error while fetching segment logs", e);
            }

            try {
                replication.semaphore.acquire();
                startChangeDataCapture();
                watchChanges();
            } catch (Exception e) {
                LOGGER.error("ChangeDataCapture: {} has failed", replication.config.jobId(), e);
            } finally {
                replication.semaphore.release();
            }
        }
    }
}
