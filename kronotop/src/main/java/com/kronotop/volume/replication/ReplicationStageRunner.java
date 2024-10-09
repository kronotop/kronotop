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

package com.kronotop.volume.replication;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.cluster.client.StatefulInternalConnection;
import com.kronotop.cluster.client.protocol.SegmentRange;
import com.kronotop.volume.NotEnoughSpaceException;
import com.kronotop.volume.VersionstampedKeySelector;
import com.kronotop.volume.segment.Segment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ReplicationStageRunner {
    protected static final int MAXIMUM_BATCH_SIZE = 100;
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationStageRunner.class);
    protected final Context context;
    protected final ReplicationConfig config;
    protected final StatefulInternalConnection<byte[], byte[]> connection;
    protected final HashMap<Long, Segment> openSegments = new HashMap<>();
    private volatile boolean stopped = false;

    public ReplicationStageRunner(Context context, ReplicationConfig config, StatefulInternalConnection<byte[], byte[]> connection) {
        this.context = context;
        this.config = config;
        this.connection = connection;
    }

    protected IterationResult iterate(Transaction tr, Segment segment, VersionstampedKeySelector begin, VersionstampedKeySelector end, int limit) throws NotEnoughSpaceException, IOException {
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

    protected void insertSegmentRange(Segment segment, List<SegmentLogEntry> entries, List<Object> dataRange) throws IOException, NotEnoughSpaceException {
        for (int i = 0; i < dataRange.size(); i++) {
            byte[] data = (byte[]) dataRange.get(i);
            SegmentLogEntry entry = entries.get(i);
            segment.insert(ByteBuffer.wrap(data), entry.value().position());
        }
        segment.flush(true);
    }

    protected List<Object> fetchSegmentRange(String segmentName, List<SegmentLogEntry> entries) {
        SegmentRange[] segmentRanges = new SegmentRange[entries.size()];
        for (int i = 0; i < entries.size(); i++) {
            SegmentLogEntry entry = entries.get(i);
            segmentRanges[i] = new SegmentRange(entry.value().position(), entry.value().length());
        }
        return connection.sync().segmentRange(config.volumeName(), segmentName, segmentRanges);
    }

    public void stop() {
        if (stopped) {
            return;
        }

        stopped = true;

        for (Segment segment : openSegments.values()) {
            try {
                segment.close();
            } catch (Exception e) {
                LOGGER.atError().
                        setMessage("Error while closing a segment, jobId = {}").
                        addArgument(config.stringifyJobId()).
                        setCause(e).
                        log();
            }
        }
    }

    protected boolean isStopped() {
        return stopped;
    }

    protected record IterationResult(Versionstamp latestKey, int processedKeys) {
    }
}
