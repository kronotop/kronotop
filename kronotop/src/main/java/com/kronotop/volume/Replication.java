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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Replication {
    private static final Logger LOGGER = LoggerFactory.getLogger(Replication.class);
    private final Context context;
    private final Volume volume;
    private final DirectorySubspace subspace;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private volatile boolean isClosed = false;

    public Replication(Context context, Volume volume) {
        this.context = context;
        this.volume = volume;
        this.subspace = volume.getConfig().subspace();
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

    ReplicationMetadata getReplicationMetadata(Transaction tr) {
        return ReplicationMetadata.compute(tr, subspace, (metadata) -> {
            ReplicationMetadata.Snapshot snapshot = metadata.getSnapshot();
            if (snapshot == null) {
                VolumeMetadata volumeMetadata = VolumeMetadata.load(tr, subspace);
                long segmentId = volumeMetadata.getSegments().getFirst();

                String segmentName = Segment.generateName(segmentId);
                SegmentLogEntry firstEntry = new SegmentLogIterable(tr, subspace, segmentName, null, null, 1).iterator().next();
                SegmentLogEntry lastEntry = new SegmentLogIterable(tr, subspace, segmentName, null, null, 1, true).iterator().next();

                snapshot = new ReplicationMetadata.Snapshot(segmentId, firstEntry.key().getBytes(), lastEntry.key().getBytes());
                System.out.println(snapshot);
                metadata.setSnapshot(snapshot);
            }
        });
    }

    public void start() {
        //checkStandbyOwnership();

        try(Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationMetadata replicationMetadata = getReplicationMetadata(tr);

            ReplicationMetadata.Snapshot snapshot = replicationMetadata.getSnapshot();
            if (snapshot.getBegin() == snapshot.getEnd()) {
                // Start the other thread
                return;
            }

            // [begin, end)
            VersionstampedKeySelector begin = VersionstampedKeySelector.firstGreaterOrEqual(Versionstamp.fromBytes(snapshot.getBegin()));
            VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterOrEqual(Versionstamp.fromBytes(snapshot.getEnd()));

            String segmentName = Segment.generateName(snapshot.getSegmentId());
            SegmentLogIterable iterable = new SegmentLogIterable(tr, subspace, segmentName, begin, end);
            for (SegmentLogEntry entry : iterable) {
                System.out.println(entry);
            }
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
