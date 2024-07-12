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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.client.InternalClient;
import com.kronotop.cluster.client.StatefulInternalConnection;
import com.kronotop.cluster.client.protocol.SegmentRange;
import io.lettuce.core.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class Replication {
    private static final Logger LOGGER = LoggerFactory.getLogger(Replication.class);
    private final Context context;
    private final ReplicationConfig config;
    private final RedisClient client;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private volatile boolean isStarted = false;

    public Replication(Context context, ReplicationConfig config) {
        this.context = context;
        this.config = config;
        Member member = config.source().member();
        this.client = RedisClient.create(
                String.format("redis://%s:%d", member.getAddress().getHost(), member.getAddress().getPort())
        );
    }

    public static String CreateReplicationJob(Transaction tr, DirectorySubspace subspace) {
        AtomicReference<String> jobId = new AtomicReference<>();
        ReplicationMetadata.compute(tr, subspace, (metadata) -> {
            VolumeMetadata volumeMetadata = VolumeMetadata.load(tr, subspace);
            long segmentId = volumeMetadata.getSegments().getFirst();

            String segmentName = Segment.generateName(segmentId);
            SegmentLogEntry firstEntry = new SegmentLogIterable(tr, subspace, segmentName, null, null, 1).iterator().next();
            SegmentLogEntry lastEntry = new SegmentLogIterable(tr, subspace, segmentName, null, null, 1, true).iterator().next();

            ReplicationMetadata.Snapshot snapshot = new ReplicationMetadata.Snapshot(segmentId, firstEntry.key().getBytes(), lastEntry.key().getBytes());
            jobId.set(metadata.setSnapshot(snapshot));
        });
        return jobId.get();
    }

    public void start() {
        if (isStarted) {
            throw new IllegalStateException("Replication is already started");
        }

        StatefulInternalConnection<String, String> connection = InternalClient.connect(client);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationMetadata replicationMetadata = ReplicationMetadata.load(tr, config.subspace());

            ReplicationMetadata.Snapshot snapshot = replicationMetadata.getSnapshot(config.jobId());
            if (snapshot.getBegin() == snapshot.getEnd()) {
                // Start the other thread
                return;
            }

            // [begin, end)
            VersionstampedKeySelector begin = VersionstampedKeySelector.firstGreaterOrEqual(Versionstamp.fromBytes(snapshot.getBegin()));
            VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterThan(Versionstamp.fromBytes(snapshot.getEnd()));

            List<SegmentRange> ranges = new ArrayList<>();
            String segmentName = Segment.generateName(snapshot.getSegmentId());
            SegmentLogIterable iterable = new SegmentLogIterable(tr, config.subspace(), segmentName, begin, end);
            for (SegmentLogEntry entry : iterable) {
                ranges.add(new SegmentRange(entry.value().position(), entry.value().length()));
            }
            SegmentRange[] r = new SegmentRange[ranges.size()];
            ranges.toArray(r);
            List<Object> items = connection.sync().segmentRange(config.volumeName(), segmentName, r);
            System.out.println(items);
        }
    }

    public void stop() {
        if (!isStarted) {
            return;
        }

        try {
            executor.close();
            client.shutdown();
        } finally {
            isStarted = false;
        }
    }
}
