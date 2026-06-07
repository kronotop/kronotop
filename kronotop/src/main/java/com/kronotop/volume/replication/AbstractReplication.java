/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.volume.segment.SegmentUtil;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

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
    protected final long reconnectBackoffNanos;
    protected final CountDownLatch latch = new CountDownLatch(1);
    protected final Object fileLock = new Object();
    protected volatile RandomAccessFile file;
    protected volatile boolean started;
    protected volatile boolean shutdown;
    private boolean fileClosed = false; // guarded by fileLock

    protected AbstractReplication(Context context, DirectorySubspace subspace, ReplicationClient client, ReplicationSession session) {
        this.context = context;
        this.subspace = subspace;
        this.client = client;
        this.session = session;
        this.transactionWithRetry = TransactionUtil.retry(10, Duration.ofMillis(100));
        this.reconnectBackoffNanos =
                TimeUnit.MILLISECONDS.toNanos(context.getConfig().getLong("volume.replication.reconnect_backoff"));
        try {
            this.file = createOrOpenSegmentFile(session.segmentId());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Parks for the configured backoff before the replication loop re-dispatches a stage that
     * aborted because the client was not connected to the primary. Without this, the loop would
     * busy-spin while the client is mid-reconnect. Returns immediately if shutdown is in progress.
     */
    protected void parkBeforeReconnectRetry() {
        if (shutdown) {
            return;
        }
        LockSupport.parkNanos(reconnectBackoffNanos);
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
        String message = root.getMessage() != null ? root.getMessage() : root.getClass().getName();
        transactionWithRetry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                ReplicationState.setStatus(tr, subspace, segmentId, ReplicationStatus.FAILED);
                ReplicationState.setErrorMessage(tr, subspace, segmentId, message);
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
        Path segmenFilePath = SegmentUtil.getFilePath(session.destination().toAbsolutePath().toString(), segmentId);
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
     * Syncs and closes the segment file at most once.
     *
     * <p>Callers may invoke this concurrently (worker loop teardown and the shutdown thread both
     * close the stage). The {@code fileLock} serializes the sync-then-close so a closed file
     * descriptor is never synced, and the idempotency flag makes repeated calls a no-op.</p>
     */
    protected void syncAndCloseFile() throws IOException {
        synchronized (fileLock) {
            if (fileClosed) {
                return;
            }
            fileClosed = true;
            if (file.getChannel().isOpen()) {
                file.getFD().sync();
                file.close();
            }
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
