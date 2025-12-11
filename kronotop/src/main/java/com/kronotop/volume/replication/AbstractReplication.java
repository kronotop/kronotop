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
import com.kronotop.internal.TransactionUtils;
import com.kronotop.volume.segment.Segment;
import io.github.resilience4j.retry.Retry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Throwables.getRootCause;

/**
 * Base class for replication stages providing common state management and file operations.
 *
 * <p>Provides shared functionality for segment file handling, replication state persistence
 * to FoundationDB, and progress tracking used by both SegmentReplication and ChangeDataCapture.</p>
 */
public abstract class AbstractReplication {
    protected final Context context;
    protected final DirectorySubspace subspace;
    protected final ReplicationClient client;
    protected final ReplicationSession session;
    protected final Retry transactionWithRetry;
    protected final CountDownLatch latch = new CountDownLatch(1);
    protected volatile RandomAccessFile file;
    protected volatile boolean started;
    protected volatile boolean shutdown;

    protected AbstractReplication(Context context, DirectorySubspace subspace, ReplicationClient client, ReplicationSession session) {
        this.context = context;
        this.subspace = subspace;
        this.client = client;
        this.session = session;
        this.transactionWithRetry = TransactionUtils.retry(10, Duration.ofMillis(100));
        try {
            this.file = createOrOpenSegmentFile(session.segmentId());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Marks a replication stage as running and clears any previous error.
     */
    protected void markReplicationStageRunning(long segmentId, Stage stage) {
        markReplicationStageRunning(segmentId, stage, null);
    }

    /**
     * Marks a replication stage as running, clears any previous error, and optionally sets the sequence number.
     */
    protected void markReplicationStageRunning(long segmentId, Stage stage, Long sequenceNumber) {
        transactionWithRetry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                ReplicationState.setStatus(tr, subspace, segmentId, ReplicationStatus.RUNNING);
                ReplicationState.setStage(tr, subspace, segmentId, stage);
                ReplicationState.clearErrorMessage(tr, subspace, segmentId);
                if (sequenceNumber != null) {
                    ReplicationState.setSequenceNumber(tr, subspace, segmentId, sequenceNumber);
                }
                tr.commit().join();
            }
        });
    }

    /**
     * Updates the replication status for a segment.
     */
    protected void setReplicationStatus(long segmentId, ReplicationStatus status) {
        transactionWithRetry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                ReplicationState.setStatus(tr, subspace, segmentId, status);
                tr.commit().join();
            }
        });
    }

    /**
     * Marks a replication stage as failed and records the error message.
     */
    protected void markReplicationStageFailed(long segmentId, Throwable throwable) {
        Throwable root = getRootCause(throwable);
        transactionWithRetry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                ReplicationState.setStatus(tr, subspace, segmentId, ReplicationStatus.FAILED);
                ReplicationState.setErrorMessage(tr, subspace, segmentId, root.getMessage());
                tr.commit().join();
            }
        });
    }

    /**
     * Persists the current replication position for the session's segment.
     */
    protected void setPosition(final long position) {
        transactionWithRetry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                ReplicationState.setPosition(tr, subspace, session.segmentId(), position);
                tr.commit().join();
            }
        });
    }

    /**
     * Records the tail pointer (sequence number and position) for segment replication.
     */
    protected void setTailPointer(long sequenceNumber, long pointer) {
        transactionWithRetry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                ReplicationState.setTailPointer(tr, subspace, session.segmentId(), sequenceNumber, pointer);
                tr.commit().join();
            }
        });
    }

    /**
     * Creates or opens a segment file, extending it to the configured segment size if needed.
     */
    protected RandomAccessFile createOrOpenSegmentFile(long segmentId) throws IOException {
        Path segmenFilePath = Segment.getSegmentFilePath(session.destination().toAbsolutePath().toString(), segmentId);
        Files.createDirectories(segmenFilePath.getParent());
        try {
            RandomAccessFile file = new RandomAccessFile(segmenFilePath.toFile(), "rw");
            if (file.length() < session.segmentSize()) {
                // Do not truncate the file, only extend it.
                file.setLength(session.segmentSize());
            }
            return file;
        } catch (FileNotFoundException e) {
            // This should not be possible.
            throw new KronotopException(e);
        }
    }

    /**
     * Writes a chunk to the segment file at the specified position and returns the new position.
     */
    protected long writeChunks(byte[] chunk, long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(chunk);
        long writePosition = position;

        while (buffer.hasRemaining()) {
            int nr = file.getChannel().write(buffer, writePosition);
            if (nr == 0) {
                Thread.onSpinWait();
                continue;
            }
            writePosition += nr;
        }

        return writePosition;
    }
}
