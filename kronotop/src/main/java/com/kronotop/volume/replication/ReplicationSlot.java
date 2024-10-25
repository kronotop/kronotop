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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.JSONUtils;
import com.kronotop.cluster.Member;
import com.kronotop.volume.VolumeMetadata;
import com.kronotop.volume.segment.Segment;

import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.kronotop.volume.Subspaces.SEGMENT_REPLICATION_SLOT_SUBSPACE;

public class ReplicationSlot {

    private final TreeMap<Long, Snapshot> snapshots = new TreeMap<>();

    private boolean snapshotCompleted;

    private long latestSegmentId;

    private byte[] latestVersionstampedKey;

    public static Versionstamp newSlot(Database database, DirectorySubspace subspace, Member standbyMember) {
        // A replication slot can only be started on a standby server, the primary owner only responds to SEGMENTRANGE requests
        // It doesn't have any idea about the standby servers and the current replication status.
        CompletableFuture<byte[]> future;
        try (Transaction tr = database.createTransaction()) {
            ReplicationSlot replicationSlot = new ReplicationSlot();
            VolumeMetadata volumeMetadata = VolumeMetadata.load(tr, subspace);

            for (Long segmentId : volumeMetadata.getSegments()) {
                String segmentName = Segment.generateName(segmentId);

                SegmentLogEntry firstEntry = new SegmentLogIterable(tr, subspace, segmentName, null, null, 1).iterator().next();
                SegmentLogEntry lastEntry = new SegmentLogIterable(tr, subspace, segmentName, null, null, 1, true).iterator().next();

                SegmentLog segmentLog = new SegmentLog(segmentName, subspace);
                int totalEntries = segmentLog.getCardinality(tr);
                Snapshot snapshot = new Snapshot(
                        segmentId,
                        totalEntries,
                        firstEntry.key().getBytes(),
                        lastEntry.key().getBytes()
                );
                replicationSlot.getSnapshots().put(segmentId, snapshot);
            }

            byte[] key = subspace.packWithVersionstamp(Tuple.from(SEGMENT_REPLICATION_SLOT_SUBSPACE, standbyMember.getId(), Versionstamp.incomplete()));
            tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, key, JSONUtils.writeValueAsBytes(replicationSlot));
            future = tr.getVersionstamp();
            tr.commit().join();
        }

        byte[] trVersion = future.join();
        return Versionstamp.complete(trVersion);
    }

    public static ReplicationSlot load(Transaction tr, ReplicationConfig config) {
        Tuple tuple = Tuple.from(SEGMENT_REPLICATION_SLOT_SUBSPACE, config.standby().member().getId(), config.slotId());
        byte[] packedKey = config.subspace().pack(tuple);
        byte[] value = tr.get(packedKey).join();
        if (value == null) {
            throw new ReplicationNotFoundException();
        }
        return JSONUtils.readValue(value, ReplicationSlot.class);
    }

    public static ReplicationSlot compute(Transaction tr, ReplicationConfig config, Consumer<ReplicationSlot> remappingFunction) {
        Tuple tuple = Tuple.from(SEGMENT_REPLICATION_SLOT_SUBSPACE, config.standby().member().getId(), config.slotId());
        byte[] packedKey = config.subspace().pack(tuple);
        byte[] value = tr.get(packedKey).join();
        if (value == null) {
            throw new ReplicationNotFoundException();
        }
        ReplicationSlot replicationSlot = JSONUtils.readValue(value, ReplicationSlot.class);
        remappingFunction.accept(replicationSlot);
        tr.set(packedKey, JSONUtils.writeValueAsBytes(replicationSlot));
        return replicationSlot;
    }

    public TreeMap<Long, Snapshot> getSnapshots() {
        return snapshots;
    }

    public boolean isSnapshotCompleted() {
        return snapshotCompleted;
    }

    public void setSnapshotCompleted(boolean snapshotCompleted) {
        this.snapshotCompleted = snapshotCompleted;
    }

    public byte[] getLatestVersionstampedKey() {
        return latestVersionstampedKey;
    }

    public void setLatestVersionstampedKey(byte[] latestVersionstampedKey) {
        this.latestVersionstampedKey = latestVersionstampedKey;
    }

    public long getLatestSegmentId() {
        return latestSegmentId;
    }

    public void setLatestSegmentId(long latestSegmentId) {
        this.latestSegmentId = latestSegmentId;
    }
}

