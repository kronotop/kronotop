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

package com.kronotop.volume.segrep;

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
import com.kronotop.volume.VolumeConfigGenerator;
import io.github.resilience4j.retry.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Throwables.getRootCause;

public class VolumeReplication implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(VolumeReplication.class);

    private final Context context;
    private final ExecutorService executor;
    private final ShardKind shardKind;
    private final int shardId;
    private final Path destination;
    private final String volume;
    private final long segmentSize;
    private final DirectorySubspace subspace;

    private final Retry transactionWithRetry;
    private final ReplicationClient client;
    private volatile boolean shutdown;

    public VolumeReplication(Context context, ShardKind shardKind, int shardId, String destination) {
        this.context = context;
        this.shardKind = shardKind;
        this.shardId = shardId;
        this.executor = Executors.newSingleThreadExecutor();
        this.transactionWithRetry = TransactionUtils.retry(10, Duration.ofMillis(100));

        try {
            this.destination = Files.createDirectories(Path.of(destination));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        this.segmentSize = getSegmentSize();
        this.volume = VolumeConfigGenerator.volumeName(shardKind, shardId);
        this.subspace = openStandbySubspace();
        this.client = new ReplicationClient(context, shardKind, shardId);
    }

    private long getSegmentSize() {
        return switch (shardKind) {
            case REDIS -> context.getConfig().getLong("redis.volume_syncer.segment_size");
            case BUCKET -> context.getConfig().getLong("bucket.volume.segment_size");
        };
    }

    private DirectorySubspace openStandbySubspace() {
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

    private long getChunkSize() {
        return switch (shardKind) {
            case BUCKET -> context.getConfig().getLong("bucket.volume.segment_replication_chunk_size");
            case REDIS -> context.getConfig().getLong("redis.volume_syncer.segment_replication_chunk_size");
        };
    }

    private Long nextSegmentId(long segmentId) {
        List<Long> segmentIds = client.connection().sync().listSegments(volume);
        int idx = Collections.binarySearch(segmentIds, segmentId);
        if (idx >= 0) {
            idx++;
        } else {
            idx = -(idx + 1);
        }
        return idx < segmentIds.size() ? segmentIds.get(idx) : null;
    }

    private boolean isLastSegment(long segmentId) {
        List<Long> segmentIds = client.connection().sync().listSegments(volume);
        if (segmentIds.isEmpty()) {
            return true;
        }
        long lastSegmentId = segmentIds.getLast();
        return lastSegmentId == segmentId;
    }

    private void setSegmentReplicationFailed(long segmentId, Throwable throwable) {
        Throwable root = getRootCause(throwable);
        transactionWithRetry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                SegmentReplicationState.setErrorMessage(tr, subspace, segmentId, root.getMessage());
                tr.commit().join();
            }
        });
    }

    @Override
    public void run() {
        client.connect();
        StatefulInternalConnection<byte[], byte[]> connection = client.connection();
        if (!connection.sync().ping().equals(Response.PONG)) {
            throw new KronotopException("Replication client health check has failed");
        }
        LOGGER.debug("Ready to start replication");

        long chunkSize = getChunkSize();

        while (true) {
            ReplicationCursor cursor = TransactionUtils.execute(context, tr -> SegmentReplicationState.readCursor(tr, subspace));
            // Guard
            if (cursor.position() > segmentSize) {
                throw new IllegalStateException("cursor.position exceeds segmentSize");
            }

            Long segmentId;
            long position = 0;
            if (cursor.position() < segmentSize) {
                // continue from where we left off
                position = cursor.position();
                segmentId = cursor.segmentId();
            } else {
                segmentId = nextSegmentId(cursor.segmentId());
            }
            if (segmentId == null) {
                // Consumed the last segment. Quit.
                break;
            }

            ReplicationSession session = new ReplicationSession(
                    volume,
                    destination,
                    segmentId,
                    position,
                    chunkSize,
                    segmentSize
            );
            SegmentReplication replication = new SegmentReplication(context, subspace, client, session);
            try {
                replication.start();
            } catch (Exception exp) {
                setSegmentReplicationFailed(segmentId, exp);
                return;
            }

            if (isLastSegment(segmentId)) {
                // TODO: Implement CDC
                break;
            }
        }
    }

    public void shutdown() {
        shutdown = true;
        executor.shutdownNow();
        client.shutdown();

        try {
            if (!executor.awaitTermination(6, TimeUnit.SECONDS)) {
                LOGGER.debug("Replication executor did not terminate within timeout");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
