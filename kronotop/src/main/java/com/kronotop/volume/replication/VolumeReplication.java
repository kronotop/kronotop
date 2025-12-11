/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume.replication;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.cluster.client.StatefulInternalConnection;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.Response;
import com.kronotop.volume.VolumeNames;
import io.github.resilience4j.retry.Retry;
import io.lettuce.core.ClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Implements volume replication from a primary node to a standby.
 *
 * <p>This class orchestrates the two-phase replication process: segment replication
 * (bulk data transfer) followed by change data capture (streaming incremental changes).
 * It maintains the replication state in FoundationDB to enable resumable transfers.</p>
 *
 * <p>The replication progresses through segments in order, tracking position within
 * each segment. Once all segments are replicated, it switches to CDC mode to stream
 * ongoing changes from the primary.</p>
 */
public class VolumeReplication implements ReplicationTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(VolumeReplication.class);

    private final Context context;
    private final ShardKind shardKind;
    private final int shardId;
    private final Path destination;
    private final String volume;
    private final long segmentSize;
    private final DirectorySubspace subspace;

    private final Retry transactionWithRetry;
    private final ReplicationClient client;
    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile ReplicationStage stage;
    private volatile boolean started;
    private volatile boolean shutdown;

    public VolumeReplication(Context context, ShardKind shardKind, int shardId, String destination) {
        this.context = context;
        this.shardKind = shardKind;
        this.shardId = shardId;
        this.transactionWithRetry = TransactionUtils.retry(10, Duration.ofMillis(100));

        try {
            this.destination = Files.createDirectories(Path.of(destination));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        this.segmentSize = getSegmentSize();
        this.volume = VolumeNames.format(shardKind, shardId);
        this.subspace = createOrOpenStandbySubspace();
        this.client = new ReplicationClient(context, shardKind, shardId);
    }

    /**
     * Returns the configured segment size based on shard kind.
     */
    private long getSegmentSize() {
        return switch (shardKind) {
            case REDIS -> context.getConfig().getLong("redis.volume.segment_size");
            case BUCKET -> context.getConfig().getLong("bucket.volume.segment_size");
        };
    }

    DirectorySubspace createOrOpenStandbySubspace() {
        KronotopDirectoryNode node = KronotopDirectory.kronotop().
                cluster(context.getClusterName()).
                metadata().
                volumes().
                bucket().
                volume(volume).
                standby(context.getMember().getId());

        return transactionWithRetry.executeSupplier(() -> TransactionUtils.executeThenCommit(context,
                tr -> DirectoryLayer.getDefault().createOrOpen(tr, node.toList()).join()
        ));
    }

    /**
     * Returns the configured chunk size for segment transfers based on shard kind.
     */
    private long getChunkSize() {
        return switch (shardKind) {
            case BUCKET -> context.getConfig().getLong("bucket.volume.segment_replication_chunk_size");
            case REDIS -> context.getConfig().getLong("redis.volume.segment_replication_chunk_size");
        };
    }

    /**
     * Finds the next segment ID after the given one, or null if none exists.
     */
    private Long nextSegmentId(long segmentId) {
        List<Long> segmentIds = client.conn().sync().listSegments(volume);
        int idx = Collections.binarySearch(segmentIds, segmentId);
        if (idx >= 0) {
            idx++;
        } else {
            idx = -(idx + 1);
        }
        return idx < segmentIds.size() ? segmentIds.get(idx) : null;
    }

    /**
     * Determines the next replication step based on current cursor state and segment status.
     */
    private ReplicationStep resolveNextSegment() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationCursor cursor = ReplicationState.readCursor(tr, subspace);
            ReplicationStatus status = ReplicationState.readStatus(tr, subspace, cursor.segmentId());
            Stage stage = ReplicationState.readStage(tr, subspace, cursor.segmentId());

            // ReplicationStep -> segmentId, position, stop, startCDC
            return switch (status) {
                case WAITING, RUNNING -> new ReplicationStep(
                        cursor.segmentId(),
                        cursor.position(),
                        false,
                        stage == Stage.CHANGE_DATA_CAPTURE
                );
                case DONE -> {
                    Long next = nextSegmentId(cursor.segmentId());
                    if (next == null) {
                        yield new ReplicationStep(cursor.segmentId(), 0, false, true); // Start CDC
                    }
                    yield new ReplicationStep(next, 0, false, false);
                }
                case FAILED, STOPPED -> new ReplicationStep(cursor.segmentId(), 0, true, false);
            };
        }
    }

    /**
     * Creates a segment replication stage for bulk data transfer.
     */
    private ReplicationStage initializeSegmentReplicationStage(ReplicationStep step, long chunkSize) {
        ReplicationSession session = new ReplicationSession(
                volume,
                shardKind,
                shardId,
                destination,
                step.segmentId(),
                step.position(),
                chunkSize,
                segmentSize
        );
        return new SegmentReplication(context, subspace, client, session);
    }

    /**
     * Creates a CDC stage for streaming incremental changes.
     */
    private ReplicationStage initializeChangeDataCaptureStage(ReplicationStep step, long chunkSize) {
        ReplicationSession session = new ReplicationSession(
                volume,
                shardKind,
                shardId,
                destination,
                step.segmentId(),
                step.position(),
                chunkSize,
                segmentSize
        );
        return new ChangeDataCapture(context, subspace, client, session);
    }

    @Override
    public void reconnect() {
        client.reconnect();
        stage.reconnect();
    }

    /**
     * Main replication loop that connects to primary and processes segments until shutdown.
     */
    private void startInternal() {
        client.connect(ClientOptions.create());

        StatefulInternalConnection<byte[], byte[]> connection = client.conn();
        if (!connection.sync().ping().equals(Response.PONG)) {
            throw new KronotopException("Replication client health check has failed");
        }

        LOGGER.debug("Ready to start replication on ShardKind={}, ShardId={}", shardKind, shardId);

        long chunkSize = getChunkSize();

        while (!shutdown) {
            ReplicationStep step = resolveNextSegment();
            if (step.stop()) {
                break;
            }

            if (step.startCDC()) {
                stage = initializeChangeDataCaptureStage(step, chunkSize);
            } else {
                stage = initializeSegmentReplicationStage(step, chunkSize);
            }

            try {
                stage.start();
            } finally {
                stage.close();
                stage = null;
            }
        }
    }

    @Override
    public void start() {
        if (shutdown) {
            throw new IllegalStateException("VolumeReplication cannot be restarted");
        }
        started = true;
        try {
            startInternal();
        } finally {
            latch.countDown();
        }
    }

    @Override
    public void shutdown() {
        shutdown = true;
        try {
            client.shutdown(); // this will kill the connections and completable futures.
            if (stage != null) stage.close();
            if (started) {
                if (!latch.await(10, TimeUnit.SECONDS)) {
                    LOGGER.warn(
                            "Graceful shutdown period exceeded for Volume: {}. Forcing close.", volume
                    );
                }
            }
        } catch (InterruptedException exp) {
            Thread.currentThread().interrupt();
            throw new KronotopException("Operation was interrupted while waiting", exp);
        }
    }

    record ReplicationStep(long segmentId, long position, boolean stop, boolean startCDC) {
    }
}
