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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.kronotop.JSONUtils;
import com.kronotop.volume.VolumeMetadata;
import com.kronotop.volume.segment.Segment;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

import static com.kronotop.volume.Subspaces.REPLICATION_SLOT_SUBSPACE;

/**
 * Class representing a replication slot used for database replication.
 * A replication slot is responsible for tracking the replication state and snapshots
 * of a standby server.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ReplicationSlot {
    private final TreeMap<Long, Snapshot> snapshots = new TreeMap<>();
    private final Set<ReplicationStage> completedStages = new HashSet<>();
    private long latestSegmentId;
    private byte[] latestVersionstampedKey;
    private ReplicationStage replicationStage;
    private boolean active;

    private static byte[] slotKey(ReplicationConfig config) {
        Tuple tuple = Tuple.from(
                REPLICATION_SLOT_SUBSPACE,
                config.shardKind().name(),
                config.shardId(),
                Versionstamp.incomplete()
        );
        return config.volumeConfig().subspace().packWithVersionstamp(tuple);
    }

    private static byte[] slotKey(ReplicationConfig config, Versionstamp slotId) {
        Tuple tuple = Tuple.from(
                REPLICATION_SLOT_SUBSPACE,
                config.shardKind().name(),
                config.shardId(),
                slotId
        );
        return config.volumeConfig().subspace().pack(tuple);
    }

    private static Snapshot newSegmentSnapshot(Transaction tr, ReplicationConfig config, long segmentId) {
        String segmentName = Segment.generateName(segmentId);
        SegmentLogEntry firstEntry = new SegmentLogIterable(
                tr,
                config.volumeConfig().subspace(),
                segmentName,
                null,
                null, 1
        ).iterator().next();
        SegmentLogEntry lastEntry = new SegmentLogIterable(
                tr,
                config.volumeConfig().subspace(),
                segmentName,
                null,
                null,
                1, true
        ).iterator().next();

        SegmentLog segmentLog = new SegmentLog(segmentName, config.volumeConfig().subspace());
        int totalEntries = segmentLog.getCardinality(tr);
        return new Snapshot(
                segmentId,
                totalEntries,
                firstEntry.key().getBytes(),
                lastEntry.key().getBytes()
        );
    }

    public static void newSlot(Transaction tr, ReplicationConfig config) {
        ReplicationSlot slot = new ReplicationSlot();

        VolumeMetadata volumeMetadata = VolumeMetadata.load(tr, config.volumeConfig().subspace());
        for (Long segmentId : volumeMetadata.getSegments()) {
            Snapshot snapshot = newSegmentSnapshot(tr, config, segmentId);
            slot.getSnapshots().put(segmentId, snapshot);
        }

        byte[] key = slotKey(config);
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, key, JSONUtils.writeValueAsBytes(slot));
    }

    public static ReplicationSlot load(Transaction tr, ReplicationConfig config, Versionstamp slotId) {
        byte[] key = slotKey(config, slotId);
        byte[] value = tr.get(key).join();
        if (value == null) {
            throw new ReplicationSlotNotFoundException();
        }
        return JSONUtils.readValue(value, ReplicationSlot.class);
    }

    public static ReplicationSlot compute(Transaction tr, ReplicationConfig config, Versionstamp slotId, Consumer<ReplicationSlot> remappingFunction) {
        byte[] key = slotKey(config, slotId);
        byte[] value = tr.get(key).join();
        if (value == null) {
            throw new ReplicationSlotNotFoundException();
        }
        ReplicationSlot replicationSlot = JSONUtils.readValue(value, ReplicationSlot.class);
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

    public Set<ReplicationStage> getCompletedStages() {
        // Keep this for JSON encode/decode.
        return completedStages;
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

    public boolean isReplicationStageCompleted(ReplicationStage stage) {
        return completedStages.contains(stage);
    }

    public void completeReplicationStage(ReplicationStage stage) {
        completedStages.add(stage);
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }
}

