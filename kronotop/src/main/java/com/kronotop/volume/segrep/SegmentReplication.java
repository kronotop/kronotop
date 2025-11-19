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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.cluster.client.protocol.SegmentRange;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.volume.segment.Segment;
import io.github.resilience4j.retry.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Throwables.getRootCause;

public class SegmentReplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentReplication.class);
    private final Context context;
    private final DirectorySubspace subspace;
    private final RandomAccessFile file;
    private final ReplicationClient client;
    private final ReplicationSession session;
    private final Retry transactionWithRetry;

    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile boolean shutdown;

    public SegmentReplication(Context context, DirectorySubspace subspace, ReplicationClient client, ReplicationSession session) {
        this.context = context;
        this.subspace = subspace;
        this.client = client;
        this.session = session;
        this.transactionWithRetry = TransactionUtils.retry(10, Duration.ofMillis(100));
        try {
            this.file = createOrOpenSegmentFile();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void updateStatus(long segmentId, SegmentReplicationStatus status) {
        transactionWithRetry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                SegmentReplicationState.setStatus(tr, subspace, segmentId, status);
                tr.commit().join();
            }
        });
    }

    private void markFailed(long segmentId, Throwable throwable) {
        Throwable root = getRootCause(throwable);
        transactionWithRetry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                SegmentReplicationState.setStatus(tr, subspace, segmentId, SegmentReplicationStatus.FAILED);
                SegmentReplicationState.setErrorMessage(tr, subspace, segmentId, root.getMessage());
                tr.commit().join();
            }
        });
    }

    private RandomAccessFile createOrOpenSegmentFile() throws IOException {
        Path segmenFilePath = Segment.getSegmentFilePath(session.destination().toAbsolutePath().toString(), session.segmentId());
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

    private long writeChunks(byte[] chunk, long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(chunk);
        long writePosition = position;

        while (buffer.hasRemaining()) {
            int nr = file.getChannel().write(buffer, writePosition);
            if (nr == 0) {
                Thread.onSpinWait();
                continue;
            }
            writePosition += nr;

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("{} bytes written to segment {}", nr, session.segmentId());
            }
        }

        return writePosition;
    }

    private void setPosition(final long position) {
        transactionWithRetry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                SegmentReplicationState.setPosition(tr, subspace, session.segmentId(), position);
                tr.commit().join();
            }
        });
    }

    private boolean isLastSegment(long segmentId) {
        List<Long> segmentIds = client.connection().sync().listSegments(session.volume());
        if (segmentIds.isEmpty()) {
            return true;
        }
        long lastSegmentId = segmentIds.getLast();
        return lastSegmentId == segmentId;
    }

    private void startInternal() {
        long limitPosition = session.segmentSize();
        long position = session.position();
        long length;

        boolean isLastSegment = isLastSegment(session.segmentId());
        if (isLastSegment) {
            limitPosition = client.connection().sync().findPosition(session.volume(), session.segmentId());
        }

        try {
            while (position < limitPosition) {
                if (shutdown) break;

                length = Math.min(session.chunkSize(), session.segmentSize() - position);

                SegmentRange range = new SegmentRange(position, length);
                List<Object> chunks = client.connection().sync()
                        .segmentrange(session.volume(), session.segmentId(), range);
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
                    if (isLastSegment) {
                        setPosition(Math.min(position, limitPosition));
                    } else {
                        setPosition(position);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            latch.countDown();
        }
    }

    public void start() {
        try {
            updateStatus(session.segmentId(), SegmentReplicationStatus.RUNNING);
            startInternal();
            if (!isLastSegment(session.segmentId())) {
                updateStatus(session.segmentId(), SegmentReplicationStatus.DONE);
            }
        } catch (Exception exp) {
            markFailed(session.segmentId(), exp);
        }
    }

    public void shutdown() throws InterruptedException, IOException {
        shutdown = true;
        latch.await();
        file.getFD().sync();
        file.close();
    }
}
