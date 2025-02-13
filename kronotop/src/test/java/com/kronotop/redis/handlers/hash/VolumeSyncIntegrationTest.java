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

package com.kronotop.redis.handlers.hash;

import com.apple.foundationdb.Transaction;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.commandbuilder.redis.RedisCommandBuilder;
import com.kronotop.redis.BaseVolumeSyncIntegrationTest;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.storage.HashFieldPack;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.volume.KeyEntry;
import com.kronotop.volume.VolumeSession;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class VolumeSyncIntegrationTest extends BaseVolumeSyncIntegrationTest {
    private final String field = "field";

    @Test
    public void test_HSET() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.hset(key, field, value).encode(buf);
        channel.writeInbound(buf);

        await().atMost(5, TimeUnit.SECONDS).until(() -> checkOnVolume(key, (keyEntry -> {
            try {
                HashFieldPack pack = HashFieldPack.unpack(keyEntry.entry());
                if (!pack.key().equals(key)) {
                    return false;
                }
                if (!pack.field().equals(field)) {
                    return false;
                }
                String hashFieldValue = new String(pack.hashFieldValue().value());
                return hashFieldValue.equals(value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        })));
    }

    @Test
    public void test_HDEL() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.hset(key, field, value).encode(buf);
            channel.writeInbound(buf);
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> volumeContainsHashField(key, field));

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.hdel(key, field).encode(buf);
            channel.writeInbound(buf);
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> !volumeContainsHashField(key, field));
    }

    @Test
    public void test_HMSET() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        Map<String, String> pairs = getKeyValuePairs();
        cmd.hmset(key, pairs).encode(buf);
        channel.writeInbound(buf);

        await().atMost(5, TimeUnit.SECONDS).until(() -> checkOnVolume(key, (keyEntry -> {
            RedisService service = kronotopInstance.getContext().getService(RedisService.NAME);
            RedisShard shard = service.findShard(key, ShardStatus.READONLY);
            try (Transaction tr = service.getContext().getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
                Iterable<KeyEntry> iterable = shard.volume().getRange(session);

                Map<String, String> result = new HashMap<>();
                for (KeyEntry entry : iterable) {
                    HashFieldPack hashFieldPack = HashFieldPack.unpack(entry.entry());
                    result.put(hashFieldPack.field(), new String(hashFieldPack.hashFieldValue().value()));
                }
                return pairs.entrySet().stream().allMatch(e -> e.getValue().equals(result.get(e.getKey())));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        })));
    }

    @Test
    public void test_HINCRBY() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.hincrby(key, field, 2).encode(buf);
        channel.writeInbound(buf);

        await().atMost(5, TimeUnit.SECONDS).until(() -> checkOnVolume(key, (keyEntry -> {
            try {
                HashFieldPack pack = HashFieldPack.unpack(keyEntry.entry());
                if (!pack.key().equals(key)) {
                    return false;
                }
                if (!pack.field().equals(field)) {
                    return false;
                }
                Integer hashFieldValue = Integer.parseInt(new String(pack.hashFieldValue().value()));
                return hashFieldValue.equals(2);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        })));
    }

    @Test
    public void test_HINCRBYFLOAT() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.hincrbyfloat(key, field, 3.14).encode(buf);
        channel.writeInbound(buf);

        await().atMost(5, TimeUnit.SECONDS).until(() -> checkOnVolume(key, (keyEntry -> {
            try {
                HashFieldPack pack = HashFieldPack.unpack(keyEntry.entry());
                if (!pack.key().equals(key)) {
                    return false;
                }
                if (!pack.field().equals(field)) {
                    return false;
                }
                Double hashFieldValue = Double.parseDouble(new String(pack.hashFieldValue().value()));
                return hashFieldValue.equals(3.14);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        })));
    }

    @Test
    public void test_HSETNX() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.hsetnx(key, field, value).encode(buf);
        channel.writeInbound(buf);

        await().atMost(5, TimeUnit.SECONDS).until(() -> checkOnVolume(key, (keyEntry -> {
            try {
                HashFieldPack pack = HashFieldPack.unpack(keyEntry.entry());
                if (!pack.key().equals(key)) {
                    return false;
                }
                if (!pack.field().equals(field)) {
                    return false;
                }
                String hashFieldValue = new String(pack.hashFieldValue().value());
                return hashFieldValue.equals(value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        })));
    }
}
