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

package com.kronotop.redis;

import com.apple.foundationdb.Transaction;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.redis.handlers.BaseRedisHandlerTest;
import com.kronotop.redis.storage.HashFieldPack;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.StringPack;
import com.kronotop.volume.KeyEntryPair;
import com.kronotop.volume.VolumeSession;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.function.Predicate;

public class BaseVolumeSyncIntegrationTest extends BaseRedisHandlerTest {
    protected static final String key = "key";
    protected static final String value = "value";

    protected boolean volumeContainsStringKey(String key) {
        RedisService service = instance.getContext().getService(RedisService.NAME);
        RedisShard shard = service.findShard(key, ShardStatus.READONLY);
        try (Transaction tr = service.getContext().getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            // TODO: This can be done without a loop
            Iterable<KeyEntryPair> iterable = shard.volume().getRange(session);
            for (KeyEntryPair entry : iterable) {
                StringPack pack = StringPack.unpack(entry.entry());
                if (key.equals(pack.key())) {
                    return true;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return false;
    }

    protected boolean volumeContainsHashField(String hashKey, String field) {
        RedisService service = instance.getContext().getService(RedisService.NAME);
        RedisShard shard = service.findShard(hashKey, ShardStatus.READONLY);
        try (Transaction tr = service.getContext().getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            // TODO: This can be done without a loop
            Iterable<KeyEntryPair> iterable = shard.volume().getRange(session);
            for (KeyEntryPair entry : iterable) {
                HashFieldPack pack = HashFieldPack.unpack(entry.entry());
                if (pack.key().equals(hashKey) && pack.field().equals(field)) {
                    return true;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return false;
    }

    protected boolean checkOnVolume(String key, Predicate<KeyEntryPair> f) {
        RedisService service = instance.getContext().getService(RedisService.NAME);
        RedisShard shard = service.findShard(key, ShardStatus.READONLY);
        try (Transaction tr = service.getContext().getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            Iterable<KeyEntryPair> iterable = shard.volume().getRange(session);
            for (KeyEntryPair entry : iterable) {
                if (f.test(entry)) {
                    return true;
                }
            }
        }
        return false;
    }

    protected HashMap<String, String> getKeyValuePairs() {
        HashMap<String, String> pairs = new HashMap<>();
        pairs.put("mykey-1{nodeA}", "myvalue-1");
        pairs.put("mykey-2{nodeA}", "myvalue-2");
        pairs.put("mykey-3{nodeA}", "myvalue-3");
        return pairs;
    }
}
