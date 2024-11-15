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
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.JSONUtils;
import com.kronotop.volume.VolumeMetadata;
import com.kronotop.volume.segment.Segment;

import java.util.TreeMap;
import java.util.function.Consumer;

import static com.kronotop.volume.Subspaces.SEGMENT_REPLICATION_SLOT_SUBSPACE;

/**
 * Class representing a replication slot used for database replication.
 * A replication slot is responsible for tracking the replication state and snapshots
 * of a standby server.
 */
public class ReplicationSlotNG {
    private final TreeMap<Long, Snapshot> snapshots = new TreeMap<>();
    private ReplicationStage replicationStage;
    private long latestSegmentId;
    private byte[] latestVersionstampedKey;

    private static byte[] slotKey(ReplicationConfigNG config) {
        Tuple tuple = Tuple.from(SEGMENT_REPLICATION_SLOT_SUBSPACE, config.shardKind().name(), config.shardId(), config.memberId());
        return config.subspace().pack(tuple);
    }

    private static Snapshot newSegmentSnapshot(Transaction tr, ReplicationConfigNG config, long segmentId) {
        String segmentName = Segment.generateName(segmentId);
        SegmentLogEntry firstEntry = new SegmentLogIterable(
                tr,
                config.subspace(),
                segmentName,
                null,
                null, 1
        ).iterator().next();
        SegmentLogEntry lastEntry = new SegmentLogIterable(
                tr,
                config.subspace(),
                segmentName,
                null,
                null,
                1, true
        ).iterator().next();

        SegmentLog segmentLog = new SegmentLog(segmentName, config.subspace());
        int totalEntries = segmentLog.getCardinality(tr);
        return new Snapshot(
                segmentId,
                totalEntries,
                firstEntry.key().getBytes(),
                lastEntry.key().getBytes()
        );
    }

    public static void newSlot(Database database, ReplicationConfigNG config) {
        ReplicationSlotNG slot = new ReplicationSlotNG();

        try (Transaction tr = database.createTransaction()) {
            VolumeMetadata volumeMetadata = VolumeMetadata.load(tr, config.subspace());
            for (Long segmentId : volumeMetadata.getSegments()) {
                Snapshot snapshot = newSegmentSnapshot(tr, config, segmentId);
                slot.getSnapshots().put(segmentId, snapshot);
            }

            byte[] value = JSONUtils.writeValueAsBytes(slot);
            tr.set(slotKey(config), value);
            tr.commit().join();
        }
    }

    public static ReplicationSlotNG load(Transaction tr, ReplicationConfigNG config) {
        byte[] value = tr.get(slotKey(config)).join();
        if (value == null) {
            throw new ReplicationSlotNotFoundException();
        }
        return JSONUtils.readValue(value, ReplicationSlotNG.class);
    }

    public static ReplicationSlotNG compute(Transaction tr, ReplicationConfigNG config, Consumer<ReplicationSlotNG> remappingFunction) {
        byte[] key = slotKey(config);
        byte[] value = tr.get(key).join();
        if (value == null) {
            throw new ReplicationSlotNotFoundException();
        }
        ReplicationSlotNG replicationSlot = JSONUtils.readValue(value, ReplicationSlotNG.class);
        remappingFunction.accept(replicationSlot);
        tr.set(key, JSONUtils.writeValueAsBytes(replicationSlot));
        return replicationSlot;
    }

    public ReplicationStage getReplicationStage() {
        return replicationStage;
    }

    public void setReplicationStage(ReplicationStage replicationStage) {
        this.replicationStage = replicationStage;
    }

    public TreeMap<Long, Snapshot> getSnapshots() {
        return snapshots;
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

