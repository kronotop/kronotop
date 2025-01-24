/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.volume.handlers;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.JSONUtils;
import com.kronotop.VersionstampUtils;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import com.kronotop.volume.VolumeConfigGenerator;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.replication.ReplicationMetadata;
import com.kronotop.volume.replication.ReplicationSlot;
import com.kronotop.volume.replication.ReplicationStage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.kronotop.volume.Subspaces.REPLICATION_SLOT_SUBSPACE;

public class ReplicationsSubcommand extends BaseHandler implements SubcommandHandler {

    public ReplicationsSubcommand(VolumeService service) {
        super(service);
    }

    /**
     * Converts a given replication slot into a Map of RedisMessage key-value pairs.
     *
     * @param shardKind The type of shard. Must be of type {@link ShardKind}.
     * @param shardId   The ID of the shard.
     * @param slot      The replication slot to be converted into a Map.
     * @return A Map containing RedisMessage key-value pairs representing the attributes of the replication slot.
     */
    protected Map<RedisMessage, RedisMessage> replicationSlotToMap(ShardKind shardKind, int shardId, ReplicationSlot slot) {
        Map<RedisMessage, RedisMessage> current = new LinkedHashMap<>();

        current.put(
                new SimpleStringRedisMessage("shard_kind"),
                new SimpleStringRedisMessage(shardKind.name())
        );

        current.put(
                new SimpleStringRedisMessage("shard_id"),
                new IntegerRedisMessage(shardId)
        );

        current.put(
                new SimpleStringRedisMessage("active"),
                slot.isActive() ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE
        );

        current.put(
                new SimpleStringRedisMessage("stale"),
                slot.isStale() ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE
        );

        String replicationStage = "";
        if (slot.getReplicationStage() != null) {
            replicationStage = slot.getReplicationStage().name();
        }
        current.put(
                new SimpleStringRedisMessage("replication_stage"),
                new SimpleStringRedisMessage(replicationStage)
        );
        List<RedisMessage> completedStages = new ArrayList<>();
        for (ReplicationStage stage : slot.getCompletedStages()) {
            completedStages.add(new SimpleStringRedisMessage(stage.name()));
        }
        current.put(new SimpleStringRedisMessage("completed_stages"), new ArrayRedisMessage(completedStages));

        current.put(
                new SimpleStringRedisMessage("latest_segment_id"),
                new IntegerRedisMessage(slot.getLatestSegmentId())
        );

        String receivedVersionstampedKey = "";
        if (slot.getReceivedVersionstampedKey() != null) {
            receivedVersionstampedKey = VersionstampUtils.base64Encode(
                    Versionstamp.fromBytes(slot.getReceivedVersionstampedKey())
            );
        }
        current.put(
                new SimpleStringRedisMessage("received_versionstamped_key"),
                new SimpleStringRedisMessage(receivedVersionstampedKey)
        );

        VolumeConfigGenerator volumeConfigGenerator = new VolumeConfigGenerator(context, shardKind, shardId);
        DirectorySubspace volumeSubspace = volumeConfigGenerator.openVolumeSubspace();
        Versionstamp latestVersionstampedKey = ReplicationMetadata.findLatestVersionstampedKey(context, volumeSubspace);
        String latestVersionstampedKeyStr = "";
        if (latestVersionstampedKey != null) {
            latestVersionstampedKeyStr = VersionstampUtils.base64Encode(latestVersionstampedKey);
        }
        current.put(
                new SimpleStringRedisMessage("latest_versionstamped_key"),
                new SimpleStringRedisMessage(latestVersionstampedKeyStr)
        );
        return current;
    }

    protected void iterateReplicationSlots(
            Transaction tr,
            Map<RedisMessage, RedisMessage> result,
            ShardKind shardKind,
            int shards
    ) {
        for (int shardId = 0; shardId < shards; shardId++) {
            VolumeConfigGenerator generator = new VolumeConfigGenerator(context, shardKind, shardId);
            DirectorySubspace volumeSubspace = generator.createOrOpenVolumeSubspace();

            Tuple tuple = Tuple.from(
                    REPLICATION_SLOT_SUBSPACE,
                    shardKind.name(),
                    shardId
            );
            Range range = Range.startsWith(volumeSubspace.pack(tuple));
            AsyncIterable<KeyValue> iterable = tr.getRange(range);
            for (KeyValue keyValue : iterable) {
                Tuple unpackedKey = volumeSubspace.unpack(keyValue.getKey());
                Versionstamp slotId = (Versionstamp) unpackedKey.get(3);
                ReplicationSlot slot = JSONUtils.readValue(keyValue.getValue(), ReplicationSlot.class);
                Map<RedisMessage, RedisMessage> current = replicationSlotToMap(shardKind, shardId, slot);
                result.put(
                        new SimpleStringRedisMessage(VersionstampUtils.base64Encode(slotId)),
                        new MapRedisMessage(current)
                );
            }
        }
    }

    @Override
    public void execute(Request request, Response response) {
        Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (ShardKind shardKind : ShardKind.values()) {
                if (shardKind.equals(ShardKind.REDIS)) {
                    int shards = context.getConfig().getInt("redis.shards");
                    iterateReplicationSlots(tr, result, shardKind, shards);
                } else if (shardKind.equals(ShardKind.BUCKET)) {
                    // TODO: BUCKET-IMPLEMENTATION
                } else {
                    throw new IllegalArgumentException("Unknown shard kind: " + shardKind);
                }
            }
        }
        response.writeMap(result);
    }
}
