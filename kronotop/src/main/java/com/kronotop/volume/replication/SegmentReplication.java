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

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.cluster.client.protocol.SegmentRange;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Throwables.getRootCause;

/**
 * Handles bulk data transfer of a single segment from primary to standby.
 *
 * <p>This stage fetches segment data in chunks up to the current tail pointer,
 * writes them to local storage, and tracks progress. Once the segment is fully
 * replicated up to the tail pointer, it marks the segment as DONE.</p>
 */
public class SegmentReplication extends AbstractReplication implements ReplicationStage {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentReplication.class);

    private final CountDownLatch latch = new CountDownLatch(1);

    public SegmentReplication(Context context, DirectorySubspace subspace, ReplicationClient client, ReplicationSession session) {
        super(context, subspace, client, session);
    }

    /**
     * Fetches and writes segment chunks in a loop until the tail pointer is reached.
     */
    private void startInternal() {
        long position = session.position();
        long length;

        try {
            RedisFuture<List<Long>> cursorFuture = client.conn().async().segmentTailPointer(session.volume(), session.segmentId());
            List<Long> cursor = cursorFuture.get();

            long limitPosition = cursor.get(0);
            long sequenceNumber = cursor.get(1);
            setTailPointer(sequenceNumber, limitPosition);

            while (position < limitPosition) {
                if (shutdown) break;

                length = Math.min(session.chunkSize(), session.segmentSize() - position);

                SegmentRange range = new SegmentRange(position, length);
                List<Object> chunks = client.conn().sync()
                        .segmentrange(session.volume(), session.segmentId(), List.of(range));
                if (chunks.isEmpty()) {
                    throw new KronotopException("Segment range returned no chunks");
                }
                for (Object chunk : chunks) {
                    if (shutdown) break;
                    if (!(chunk instanceof byte[] data)) {
                        throw new KronotopException("SEGMENTRANGE returned an invalid chunk type");
                    }
                    if (data.length != length) {
                        throw new KronotopException(
                                "SEGMENTRANGE returned " + data.length + " bytes, expected " + length
                        );
                    }
                    position = writeChunks(data, position);
                    file.getFD().sync();
                    // During active-segment replication, batch writes may overshoot the limitPosition.
                    // To avoid advancing beyond the allowed range, clamp to the lower of (position, limitPosition).
                    // The main loop will exit once this boundary is reached.
                    setPosition(Math.min(position, limitPosition));
                }
            }
            if (position >= limitPosition) {
                setReplicationStatus(session.segmentId(), ReplicationStatus.DONE);
            }
        } catch (ExecutionException exp) {
            throw new KronotopException(exp.getCause());
        } catch (InterruptedException exp) {
            Thread.currentThread().interrupt();
            throw new KronotopException("Operation was interrupted while waiting", exp);
        } catch (IOException exp) {
            throw new KronotopException(exp);
        } finally {
            latch.countDown();
        }
    }

    @Override
    public Stage stage() {
        return Stage.SEGMENT_REPLICATION;
    }

    @Override
    public void reconnect() {
        // Placeholder
    }

    @Override
    public void start() {
        started = true;
        try {
            markReplicationStageRunning(session.segmentId(), Stage.SEGMENT_REPLICATION);
            startInternal();
        } catch (Exception exp) {
            if (shutdown) {
                Throwable root = getRootCause(exp);
                if (root instanceof RedisException) {
                    // Expected: connection closed during shutdown
                    return;
                }
            }
            if (exp instanceof ReplicationClientShutdownException ||
                    exp instanceof ReplicationClientNotConnectedException) {
                return;
            }
            markReplicationStageFailed(session.segmentId(), exp);
            LOGGER.error("{} stage has failed on SegmentId: {} ", Stage.SEGMENT_REPLICATION, session.segmentId(), exp);
            throw exp;
        }
    }

    @Override
    public void close() {
        shutdown = true;

        try {
            if (started) {
                if (!latch.await(10, TimeUnit.SECONDS)) {
                    LOGGER.warn(
                            "Graceful shutdown period exceeded for Volume: {} Segment: {}. Forcing close.",
                            session.volume(),
                            session.segmentId()
                    );
                }
            }

            if (file.getChannel().isOpen()) {
                file.getFD().sync();
                file.close();
            }
        } catch (InterruptedException exp) {
            Thread.currentThread().interrupt();
            throw new KronotopException("Operation was interrupted while waiting", exp);
        } catch (IOException exp) {
            throw new KronotopException(
                    String.format("Failed to close file for Volume=%s Segment=%d", session.volume(), session.segmentId()),
                    exp
            );
        }
    }
}
