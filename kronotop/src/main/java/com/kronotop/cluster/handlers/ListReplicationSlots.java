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

package com.kronotop.cluster.handlers;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.JSONUtils;
import com.kronotop.VersionstampUtils;
import com.kronotop.cluster.MembershipService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.VolumeConfigGenerator;
import com.kronotop.volume.replication.ReplicationSlot;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.kronotop.volume.Subspaces.REPLICATION_SLOT_SUBSPACE;

public class ListReplicationSlots extends BaseKrAdminSubcommandHandler implements SubcommandHandler {

    public ListReplicationSlots(MembershipService membership) {
        super(membership);
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
                } else {
                    throw new IllegalArgumentException("Unknown shard kind: " + shardKind);
                }
            }
        }
        response.writeMap(result);
    }
}
