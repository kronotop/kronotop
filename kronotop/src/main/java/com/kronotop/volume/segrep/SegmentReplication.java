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

public class SegmentReplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentReplication.class);
    private static final long CHUNK_SIZE = 1 << 24;
    private final Context context;
    private final DirectorySubspace subspace;
    private final RandomAccessFile file;
    private final ReplicationClient client;
    private final ReplicationSession session;
    private final Retry transactionWithRetry;

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

    private RandomAccessFile createOrOpenSegmentFile() throws IOException {
        Path parent = Files.createDirectories(session.destination());
        String fileName = Segment.generateFileName(session.segmentId());
        Path segmentFilePath = parent.resolve(fileName);
        try {
            RandomAccessFile file = new RandomAccessFile(segmentFilePath.toFile(), "rw");
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

    public void run() {
        long position = session.position();
        long length = CHUNK_SIZE;

        try {
            do {
                if (position + CHUNK_SIZE > session.segmentSize()) {
                    length = session.segmentSize() - position;
                }
                SegmentRange range = new SegmentRange(position, length);
                List<Object> chunks = client.connection().sync().segmentrange(session.volume(), session.segmentId(), range);
                for (Object chunk : chunks) {
                    ByteBuffer buffer = ByteBuffer.wrap((byte[]) chunk);
                    int nr = file.getChannel().write(buffer, position);
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("{} bytes has been written to segment {}", nr, session.segmentId());
                    }
                }
                file.getFD().sync();
                position += length;
                final long finalPosition = position;
                transactionWithRetry.executeRunnable(() -> {
                    TransactionUtils.executeThenCommit(context, tr -> {
                        SegmentReplicationState.setPosition(tr, subspace, session.segmentId(), finalPosition);
                        return null;
                    });
                });
            } while (position < session.segmentSize() && !shutdown);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void shutdown() {
        shutdown = true;
    }
}
