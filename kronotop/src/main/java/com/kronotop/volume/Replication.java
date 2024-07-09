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
import com.kronotop.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Replication {
    private static final Logger LOGGER = LoggerFactory.getLogger(Replication.class);
    private final Context context;
    private final Volume volume;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private volatile boolean isClosed = false;

    public Replication(Context context, Volume volume) {
        this.context = context;
        this.volume = volume;
    }

    private void checkStandbyOwnership() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadata metadata = VolumeMetadata.load(tr, volume.getConfig().subspace());
            Host owner = metadata.getOwner();
            if (owner.member().getId().equals(context.getMember().getId())) {
                isClosed = true;
                throw new IllegalStateException("Volume owner cannot run volume replication");
            }

            for (Host host : metadata.getStandbyHosts()) {
                if (host.member().getId().equals(context.getMember().getId())) {
                    return;
                }
            }
            isClosed = true;
            throw new IllegalStateException("This member is not listed as a standby");
        }
    }

    public void start() {
        checkStandbyOwnership();
        try(Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationMetadata replicationMetadata = ReplicationMetadata.compute(tr, volume.getConfig().subspace(), (metadata) -> {
                ReplicationMetadata.Snapshot snapshot = metadata.getSnapshot();
                if (snapshot == null) {
                    VolumeMetadata volumeMetadata = VolumeMetadata.load(tr, volume.getConfig().subspace());
                    long firstSegmentId = volumeMetadata.getSegments().getFirst();

                    SegmentLogEntry firstEntry = new SegmentLogIterable(tr, volume.getConfig().subspace(), Segment.generateName(firstSegmentId), null, null, 1).iterator().next();
                    SegmentLogEntry lastEntry = new SegmentLogIterable(tr, volume.getConfig().subspace(), Segment.generateName(firstSegmentId), null, null, 1, true).iterator().next();

                    snapshot = new ReplicationMetadata.Snapshot(firstEntry.key().getBytes(), lastEntry.key().getBytes());
                    metadata.setSnapshot(snapshot);
                }
            });

            ReplicationMetadata.Snapshot snapshot = replicationMetadata.getSnapshot();
            if (snapshot.getBegin() == snapshot.getEnd()) {
                // Start the other thread
                return;
            }

            // Run the snapshot fetcher
            //new SegmentLogIterable(tr, volume.getConfig(), snapshot.getBegin(), snapshot.getEnd());
        }
        executor.submit(() -> {});
    }

    public void stop() {
        if (isClosed) {
            return;
        }

        isClosed = true;
        executor.close();
    }
}
