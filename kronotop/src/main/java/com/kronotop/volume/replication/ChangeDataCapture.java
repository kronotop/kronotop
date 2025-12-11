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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.cluster.client.protocol.ChangeLogCoordinateResponse;
import com.kronotop.cluster.client.protocol.ChangeLogEntryResponse;
import com.kronotop.cluster.client.protocol.SegmentRange;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.TimeoutOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Throwables.getRootCause;
import static com.kronotop.volume.ParentOperationKind.LIFECYCLE;

/**
 * Streams incremental changes from the primary's changelog to the standby.
 *
 * <p>After segment replication completes, CDC takes over to continuously watch for
 * new changelog entries and apply them. It handles segment rollovers when the primary
 * creates new segments and maintains sequence number tracking for resumability.</p>
 */
public class ChangeDataCapture extends AbstractReplication implements ReplicationStage {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeDataCapture.class);
    private final ReplicationClient clientWithoutTimeout;
    private volatile RedisFuture<Long> watcher;

    public ChangeDataCapture(Context context, DirectorySubspace subspace, ReplicationClient client, ReplicationSession session) {
        super(context, subspace, client, session);
        this.clientWithoutTimeout = new ReplicationClient(context, session.shardKind(), session.shardId());

        ClientOptions options = ClientOptions.builder().timeoutOptions(TimeoutOptions.create()).build();
        this.clientWithoutTimeout.connect(options);
    }

    @Override
    public Stage stage() {
        return Stage.CHANGE_DATA_CAPTURE;
    }

    /**
     * Persists current replication progress to FoundationDB.
     */
    private void persistProgress(long segmentId, long sequenceNumber, long position) {
        transactionWithRetry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                ReplicationState.setSequenceNumber(tr, subspace, segmentId, sequenceNumber);
                ReplicationState.setPosition(tr, subspace, segmentId, position);
                tr.commit().join();
            }
        });
    }

    /**
     * Fetches segment ranges from the primary and writes them to local storage.
     */
    private void pullAndPersistRanges(long segmentId, List<SegmentRange> ranges) throws IOException, ExecutionException, InterruptedException {
        RedisFuture<List<Object>> future = client.conn().async().segmentrange(session.volume(), segmentId, ranges);
        List<Object> chunks = future.get();
        for (int i = 0; i < chunks.size(); i++) {
            Object chunk = chunks.get(i);
            if (shutdown) break;
            if (!(chunk instanceof byte[] data)) {
                throw new KronotopException("SEGMENTRANGE returned an invalid chunk type");
            }
            SegmentRange range = ranges.get(i);
            if (data.length != range.length()) {
                throw new KronotopException(
                        "SEGMENTRANGE returned " + data.length + " bytes, expected " + range.length()
                );
            }
            writeChunks(data, range.position());
        }
    }

    /**
     * Processes changelog entries in batches, handling segment rollovers as needed.
     */
    private void processChanges(Checkpoint checkpoint, long latestSequenceNumber, List<ChangeLogEntryResponse> changes)
            throws IOException, ExecutionException, InterruptedException {
        int index = 0;
        while (index < changes.size()) {
            if (shutdown) break;

            // Initial state of the batch processing
            long batchLength = 0;
            long sequenceNumber = 0;
            long nextPosition = 0;
            long nextSegmentId = -1;

            List<SegmentRange> ranges = new ArrayList<>();
            for (int i = index; i < changes.size(); i++) {
                if (shutdown) break;
                ChangeLogEntryResponse response = changes.get(i);
                ChangeLogCoordinateResponse coordinate = response.after();
                if (checkpoint.segmentId() != coordinate.segmentId()) {
                    nextSegmentId = coordinate.segmentId();
                    break;
                }
                if (batchLength + coordinate.length() >= session.chunkSize()) {
                    break;
                }
                index++;
                SegmentRange range = new SegmentRange(coordinate.position(), coordinate.length());
                ranges.add(range);
                batchLength += coordinate.length();
                sequenceNumber = coordinate.sequenceNumber();
                nextPosition = coordinate.position() + coordinate.length();
            }

            if (shutdown) break;

            if (!ranges.isEmpty()) {
                pullAndPersistRanges(checkpoint.segmentId(), ranges);
                file.getFD().sync();
                persistProgress(checkpoint.segmentId(), sequenceNumber, nextPosition);
                checkpoint.setSequenceNumber(sequenceNumber);
                if (nextSegmentId > 0) {
                    LOGGER.debug(
                            "Segment rollover: volume={}, oldSegmentId={}, newSegmentId={}, rolloverPosition={}",
                            session.volume(),
                            checkpoint.segmentId(),
                            nextSegmentId,
                            nextPosition
                    );
                    file.close();
                    file = createOrOpenSegmentFile(nextSegmentId);
                    checkpoint.setSegmentId(nextSegmentId);
                }
            } else {
                if (nextSegmentId > 0) {
                    // segment only advanced, no ranges
                    checkpoint.setSegmentId(nextSegmentId);
                    checkpoint.setSequenceNumber(latestSequenceNumber);
                }
            }
        }
    }

    /**
     * Main CDC loop that watches for changelog updates and applies them.
     */
    private void startInternal(Cursor cursor) {
        Checkpoint checkpoint = new Checkpoint();
        checkpoint.setSegmentId(session.segmentId());
        checkpoint.setSequenceNumber(cursor.sequenceNumber());

        while (!shutdown) {
            try {
                synchronized (this) {
                    if (shutdown) break;
                    watcher = clientWithoutTimeout.conn().async().changelogWatch(session.volume(), checkpoint.sequenceNumber());
                }
                long latestSequenceNumber = watcher.get();

                String start = String.format("(%d", checkpoint.sequenceNumber());
                String end = String.format("%d]", latestSequenceNumber);

                RedisFuture<List<ChangeLogEntryResponse>> iterator = client.conn().
                        async().changelogRange(session.volume(), LIFECYCLE.toString(), start, end, null);
                List<ChangeLogEntryResponse> changes = iterator.get();
                if (changes.isEmpty()) {
                    checkpoint.setSequenceNumber(latestSequenceNumber);
                    continue;
                }
                processChanges(checkpoint, latestSequenceNumber, changes);
            } catch (CancellationException exp) {
                LOGGER.debug(
                        "CDC loop interrupted: task cancelled (future cancel or shutdown). segmentId={} volume={}",
                        checkpoint.segmentId(), session.volume()
                );
                break;
            } catch (ExecutionException exp) {
                throw new KronotopException(exp.getCause());
            } catch (InterruptedException exp) {
                Thread.currentThread().interrupt();
                throw new KronotopException("Operation was interrupted while waiting", exp);
            } catch (IOException exp) {
                throw new KronotopException(exp);
            }
        }
    }

    /**
     * Determines the starting cursor position based on persisted replication state.
     */
    private Cursor locateCursor() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Stage currentStage = ReplicationState.readStage(tr, subspace, session.segmentId());
            long sequenceNumber;
            if (currentStage == Stage.SEGMENT_REPLICATION) {
                List<Long> tailPointer = ReplicationState.readTailPointer(tr, subspace, session.segmentId());
                sequenceNumber = tailPointer.getFirst();
            } else if (currentStage == Stage.CHANGE_DATA_CAPTURE) {
                sequenceNumber = ReplicationState.readSequenceNumber(tr, subspace, session.segmentId());
            } else {
                throw new IllegalArgumentException("Unknown replication stage: " + currentStage);
            }
            long position = ReplicationState.readPosition(tr, subspace, session.segmentId());

            // A sequence number of -1 on a non-active segment indicates that no CDC
            // offset exists for this segment. This is an inconsistent state for CDC
            // and requires falling back to full SegmentReplication.
            if (sequenceNumber < 0) {
                List<Long> segments = client.conn().sync().listSegments(session.volume());
                if (segments.isEmpty() && session.segmentId() == 0) {
                    // brand new volume, no data
                    sequenceNumber = 0;
                } else if (Objects.equals(segments.getLast(), session.segmentId())) {
                    // Active segment
                    sequenceNumber = 0;
                } else {
                    throw new IllegalStateException(
                            "CDC cannot start: sequence number is -1 for a non-active segment; fallback to SegmentReplication required."
                    );
                }
            }
            return new Cursor(sequenceNumber, position);
        }
    }

    @Override
    public void reconnect() {
        clientWithoutTimeout.reconnect();
    }

    @Override
    public void start() {
        started = true;
        try {
            Cursor cursor = locateCursor();
            markReplicationStageRunning(session.segmentId(), Stage.CHANGE_DATA_CAPTURE, cursor.sequenceNumber());
            startInternal(cursor);
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
            LOGGER.error("{} stage has failed on SegmentId: {} ", Stage.CHANGE_DATA_CAPTURE, session.segmentId(), exp);
            throw exp;
        } finally {
            latch.countDown();
        }
    }

    @Override
    public void close() {
        try {
            synchronized (this) {
                shutdown = true;
                clientWithoutTimeout.shutdown();
                if (watcher != null) {
                    watcher.cancel(true);
                }
            }

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
        } catch (IOException exp) {
            LOGGER.error("Failed to close Segment file", exp);
        } catch (InterruptedException exp) {
            Thread.currentThread().interrupt();
            throw new KronotopException("Operation was interrupted while waiting", exp);
        }
    }

    record Cursor(long sequenceNumber, long nextPosition) {
    }

    /**
     * Mutable tracking state for current segment and sequence number during CDC processing.
     */
    private static final class Checkpoint {
        private long segmentId;
        private long sequenceNumber;

        public void setSegmentId(long segmentId) {
            this.segmentId = segmentId;
        }

        public long segmentId() {
            return segmentId;
        }

        public void setSequenceNumber(long sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
        }

        public long sequenceNumber() {
            return sequenceNumber;
        }
    }
}
