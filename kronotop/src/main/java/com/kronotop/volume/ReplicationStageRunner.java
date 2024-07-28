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

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.client.InternalClient;
import com.kronotop.cluster.client.StatefulInternalConnection;
import com.kronotop.cluster.client.protocol.SegmentRange;
import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Semaphore;

public class ReplicationStageRunner {
    protected static final int MAXIMUM_BATCH_SIZE = 100;
    protected final Context context;
    protected final ReplicationConfig config;
    protected final Semaphore semaphore = new Semaphore(1);
    protected final RedisClient client;
    protected final StatefulInternalConnection<byte[], byte[]> connection;
    protected final HashMap<Long, Segment> openSegments = new HashMap<>();
    private volatile boolean started = false;
    private volatile boolean stopped = false;

    public ReplicationStageRunner(Context context, ReplicationConfig config) {
        this.context = context;
        this.config = config;

        Member member = config.source().member();
        this.client = RedisClient.create(
                String.format("redis://%s:%d", member.getAddress().getHost(), member.getAddress().getPort())
        );
        this.connection = InternalClient.connect(client, ByteArrayCodec.INSTANCE);
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
        com.kronotop.cluster.client.protocol.SegmentRange[] segmentRanges = new com.kronotop.cluster.client.protocol.SegmentRange[entries.size()];
        for (int i = 0; i < entries.size(); i++) {
            SegmentLogEntry entry = entries.get(i);
            segmentRanges[i] = new SegmentRange(entry.value().position(), entry.value().length());
        }
        return connection.sync().segmentRange(config.volumeName(), segmentName, segmentRanges);
    }

    public void setStarted(boolean started) {
        this.started = started;
    }

    public boolean isStarted() {
        return started;
    }

    public void stop() {
        if (!started) {
            throw new IllegalStateException("Replication is not started");
        }

        stopped = true;

        for (Segment segment : openSegments.values()) {
            try {
                segment.close();
            } catch (IOException e) {
                // TODO: Log this properly
                e.printStackTrace();
            }
        }

        try {
            client.shutdown();
        } finally {
            setStarted(false);
        }
    }

    protected boolean isStopped() {
        return stopped;
    }

    protected record IterationResult(Versionstamp latestKey, int processedKeys) {
    }
}
