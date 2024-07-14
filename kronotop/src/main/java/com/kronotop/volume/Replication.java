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
import com.apple.foundationdb.directory.DirectorySubspace;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
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
    private volatile boolean isStarted = false;

    public Replication(Context context, ReplicationConfig config) {
        this.context = context;
        this.config = config;
        Member member = config.source().member();
        this.client = RedisClient.create(
                String.format("redis://%s:%d", member.getAddress().getHost(), member.getAddress().getPort())
        );
        this.connection = InternalClient.connect(client, ByteArrayCodec.INSTANCE);
    }

    public static String CreateReplicationJob(Transaction tr, DirectorySubspace subspace) {
        AtomicReference<String> jobId = new AtomicReference<>();
        ReplicationMetadata.compute(tr, subspace, (metadata) -> {
            VolumeMetadata volumeMetadata = VolumeMetadata.load(tr, subspace);
            long segmentId = volumeMetadata.getSegments().getFirst();

            String segmentName = Segment.generateName(segmentId);
            SegmentLogEntry firstEntry = new SegmentLogIterable(tr, subspace, segmentName, null, null, 1).iterator().next();
            SegmentLogEntry lastEntry = new SegmentLogIterable(tr, subspace, segmentName, null, null, 1, true).iterator().next();

            SegmentLog segmentLog = new SegmentLog(segmentName, subspace);
            int totalEntries = segmentLog.getCardinality(tr);
            ReplicationMetadata.Snapshot snapshot = new ReplicationMetadata.Snapshot(
                    segmentId,
                    totalEntries,
                    firstEntry.key().getBytes(),
                    lastEntry.key().getBytes()
            );
            jobId.set(metadata.setSnapshot(snapshot));
        });
        return jobId.get();
    }

    public AtomicReference<Future<?>> getSnapshotFuture() {
        return snapshotFuture;
    }

    public void start() throws IOException {
        if (isStarted) {
            throw new IllegalStateException("Replication is already started");
        }
        snapshotFuture.set(executor.submit(new SnapshotJob(this)));
        isStarted = true;
    }

    public void stop() throws IOException {
        if (!isStarted) {
            return;
        }

        for (Segment segment : openSegments.values()) {
            segment.close();
        }

        try {
            executor.close();
            client.shutdown();
        } finally {
            isStarted = false;
        }
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

    private static class SnapshotJob implements Runnable {
        private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotJob.class);
        private final Context context;
        private final ReplicationConfig config;
        private final Replication replication;

        public SnapshotJob(Replication replication) {
            this.replication = replication;
            this.context = replication.context;
            this.config = replication.config;
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

            List<Object> dataRanges = replication.fetchSegmentRange(segment.getName(), segmentLogEntries);
            replication.insertSegmentRange(segment, segmentLogEntries, dataRanges);
            return new IterationResult(segmentLogEntries.getLast().key(), segmentLogEntries.size());
        }

        private IterationResult iterateSegmentLogEntries(Transaction tr) throws IOException, NotEnoughSpaceException {
            ReplicationMetadata replicationMetadata = ReplicationMetadata.load(tr, config.subspace());
            ReplicationMetadata.Snapshot snapshot = replicationMetadata.getSnapshot(config.jobId());

            Segment segment = replication.openSegments.get(snapshot.getSegmentId());
            if (segment == null) {
                SegmentConfig segmentConfig = new SegmentConfig(snapshot.getSegmentId(), config.rootPath(), config.segmentSize());
                segment = new Segment(segmentConfig);
                replication.openSegments.put(snapshot.getSegmentId(), segment);
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

        private void snapshotLoop() {
            // Take a snapshot
            while (true) {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    IterationResult result = iterateSegmentLogEntries(tr);
                    if (result.processedKeys > 0) {
                        ReplicationMetadata replicationMetadata = ReplicationMetadata.compute(tr, config.subspace(), (metadata) -> {
                            ReplicationMetadata.Snapshot snapshot = metadata.getSnapshot(config.jobId());
                            snapshot.setBegin(result.latestKey.getBytes());
                            snapshot.setProcessedEntries(result.processedKeys + snapshot.getProcessedEntries());
                            snapshot.setLastUpdate(Instant.now().toEpochMilli());
                        });
                        tr.commit().join();

                        // The end key fetched: [begin, end]
                        ReplicationMetadata.Snapshot snapshot = replicationMetadata.getSnapshot(config.jobId());
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

        @Override
        public void run() {
            try {
                replication.semaphore.acquire();
                snapshotLoop();
            } catch (Exception e) {
                LOGGER.error("SnapshotJob: {} has failed", replication.config.jobId(), e);
            } finally {
                replication.semaphore.release();
            }
        }

        private record IterationResult(Versionstamp latestKey, int processedKeys) {
        }
    }
}
