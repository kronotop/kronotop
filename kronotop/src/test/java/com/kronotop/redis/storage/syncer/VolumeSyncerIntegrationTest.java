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

package com.kronotop.redis.storage.syncer;

import com.apple.foundationdb.Transaction;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RouteKind;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.commandbuilder.kronotop.KrAdminCommandBuilder;
import com.kronotop.redis.handlers.string.StringValue;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.redis.storage.StringPack;
import com.kronotop.redis.storage.impl.OnHeapRedisShardImpl;
import com.kronotop.redis.storage.syncer.jobs.AppendStringJob;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.KeyEntry;
import com.kronotop.volume.Session;
import com.kronotop.volume.Volume;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.replication.BaseNetworkedVolumeIntegrationTest;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class VolumeSyncerIntegrationTest extends BaseNetworkedVolumeIntegrationTest {

    private void setSyncStandby(KronotopTestInstance standbyInstance) {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.route("SET", RouteKind.STANDBY.name(), ShardKind.REDIS.name(), standbyInstance.getMember().getId()).encode(buf);
            channel.writeInbound(buf);

            Object raw = channel.readOutbound();
            if (raw instanceof SimpleStringRedisMessage message) {
                assertEquals(Response.OK, message.content());
            } else if (raw instanceof ErrorRedisMessage message) {
                fail(message.content());
            }
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.syncStandby("SET", ShardKind.REDIS.name(), standbyInstance.getMember().getId()).encode(buf);
            channel.writeInbound(buf);

            Object raw = channel.readOutbound();
            if (raw instanceof SimpleStringRedisMessage message) {
                assertEquals(Response.OK, message.content());
            } else if (raw instanceof ErrorRedisMessage message) {
                fail(message.content());
            }
        }
    }

    @Test
    void sync_replication_when_standby_members_exists() throws IOException, InterruptedException {
        KronotopTestInstance standbyInstance = addNewInstance(true);
        setSyncStandby(standbyInstance);

        await().atMost(Duration.ofSeconds(5)).until(() -> {
            int numberOfShards = kronotopInstance.getContext().getConfig().getInt("redis.shards");
            Set<Integer> shards = new HashSet<>();
            RoutingService routing = kronotopInstance.getContext().getService(RoutingService.NAME);
            for (int shardId = 0; shardId < numberOfShards; shardId++) {
                Route route = routing.findRoute(ShardKind.REDIS, shardId);
                if (!route.syncStandbys().isEmpty()) {
                    shards.add(shardId);
                }
            }
            return shards.size() == numberOfShards;
        });

        RedisShard shard = new OnHeapRedisShardImpl(context, 0);
        String expectedKey = "key-1";
        String expectedValue = "value-1";

        shard.storage().put(expectedKey, new RedisValueContainer(new StringValue(expectedValue.getBytes(), 0L)));
        shard.volumeSyncQueue().add(new AppendStringJob("key-1"));

        VolumeSyncer volumeSyncer = new VolumeSyncer(context, shard);
        assertFalse(volumeSyncer.isQueueEmpty());
        volumeSyncer.run();
        assertTrue(volumeSyncer.isQueueEmpty());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Session session = new Session(tr, prefix);
            VolumeService standbyVolumeService = standbyInstance.getContext().getService(VolumeService.NAME);
            Volume standbyVolume = standbyVolumeService.findVolume(shard.volume().getConfig().name());
            Iterable<KeyEntry> iterable = standbyVolume.getRange(session);
            for (KeyEntry keyEntry : iterable) {
                StringPack pack = StringPack.unpack(keyEntry.entry());
                assertEquals(expectedKey, pack.key());
                assertArrayEquals(expectedValue.getBytes(), pack.stringValue().value());
                assertEquals(0L, pack.stringValue().ttl());
            }
        }
    }
}
