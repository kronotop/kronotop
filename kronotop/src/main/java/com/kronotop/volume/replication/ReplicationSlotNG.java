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
import com.kronotop.volume.VolumeConfig;
import com.kronotop.volume.VolumeMetadata;
import com.kronotop.volume.segment.Segment;

import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.kronotop.volume.Subspaces.SEGMENT_REPLICATION_SLOT_SUBSPACE;

/**
 * Class representing a replication slot used for database replication.
 * A replication slot is responsible for tracking the replication state and snapshots
 * of a standby server.
 */
public class ReplicationSlotNG {
    private final TreeMap<Long, Snapshot> snapshots = new TreeMap<>();
    private final ReplicationConfigNG config;
    private ReplicationStage replicationStage;
    private long latestSegmentId;
    private byte[] latestVersionstampedKey;

    private ReplicationSlotNG(ReplicationConfigNG config) {
        this.config = config;
    }

    public static Versionstamp newSlot(Database database, ReplicationConfigNG config) {
        // A replication slot can only be started on a standby server, the primary owner only responds to SEGMENTRANGE requests
        // It doesn't have any idea about the standby servers and the current replication status.
        CompletableFuture<byte[]> future;
        VolumeConfig volumeConfig = config.volumeConfig();
        DirectorySubspace subspace = config.volumeConfig().subspace();
        try (Transaction tr = database.createTransaction()) {
            ReplicationSlotNG slot = new ReplicationSlotNG(config);
            VolumeMetadata volumeMetadata = VolumeMetadata.load(tr, subspace);

            for (Long segmentId : volumeMetadata.getSegments()) {
                String segmentName = Segment.generateName(segmentId);

                SegmentLogEntry firstEntry = new SegmentLogIterable(
                        tr,
                        subspace,
                        segmentName,
                        null,
                        null, 1
                ).iterator().next();
                SegmentLogEntry lastEntry = new SegmentLogIterable(
                        tr,
                        subspace,
                        segmentName,
                        null,
                        null,
                        1, true
                ).iterator().next();

                SegmentLog segmentLog = new SegmentLog(segmentName, subspace);
                int totalEntries = segmentLog.getCardinality(tr);
                Snapshot snapshot = new Snapshot(
                        segmentId,
                        totalEntries,
                        firstEntry.key().getBytes(),
                        lastEntry.key().getBytes()
                );
                slot.snapshots().put(segmentId, snapshot);
            }

            Tuple tuple = Tuple.from(SEGMENT_REPLICATION_SLOT_SUBSPACE, config.shardKind(), config.shardId(), config.standbyMemberId(), Versionstamp.incomplete());
            tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, subspace.packWithVersionstamp(tuple), JSONUtils.writeValueAsBytes(slot));
            future = tr.getVersionstamp();
            tr.commit().join();
        }

        byte[] trVersion = future.join();
        return Versionstamp.complete(trVersion);
    }

    public static ReplicationSlot load(Transaction tr, ReplicationConfigNG config) {
        Tuple tuple = Tuple.from(SEGMENT_REPLICATION_SLOT_SUBSPACE, config.shardKind(), config.shardId(), config.standbyMemberId(), config.slotId());
        byte[] packedKey = config.volumeConfig().subspace().pack(tuple);
        byte[] value = tr.get(packedKey).join();
        if (value == null) {
            throw new ReplicationNotFoundException();
        }
        return JSONUtils.readValue(value, ReplicationSlot.class);
    }

    public static ReplicationSlot compute(Transaction tr, ReplicationConfigNG config, Consumer<ReplicationSlot> remappingFunction) {
        Tuple tuple = Tuple.from(SEGMENT_REPLICATION_SLOT_SUBSPACE, config.shardKind(), config.shardId(), config.standbyMemberId(), config.slotId());
        byte[] packedKey = config.volumeConfig().subspace().pack(tuple);
        byte[] value = tr.get(packedKey).join();
        if (value == null) {
            throw new ReplicationNotFoundException();
        }
        ReplicationSlot replicationSlot = JSONUtils.readValue(value, ReplicationSlot.class);
        remappingFunction.accept(replicationSlot);
        tr.set(packedKey, JSONUtils.writeValueAsBytes(replicationSlot));
        return replicationSlot;
    }

    public ReplicationStage replicationStage() {
        return replicationStage;
    }

    public void setReplicationStage(ReplicationStage replicationStage) {
        this.replicationStage = replicationStage;
    }

    public TreeMap<Long, Snapshot> snapshots() {
        return snapshots;
    }

    public byte[] latestVersionstampedKey() {
        return latestVersionstampedKey;
    }

    public void setLatestVersionstampedKey(byte[] latestVersionstampedKey) {
        this.latestVersionstampedKey = latestVersionstampedKey;
    }

    public long latestSegmentId() {
        return latestSegmentId;
    }

    public void setLatestSegmentId(long latestSegmentId) {
        this.latestSegmentId = latestSegmentId;
    }
}

