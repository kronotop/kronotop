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

package com.kronotop.redis.storage;

import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.hash.HashFieldValue;
import com.kronotop.redis.handlers.hash.HashValue;
import com.kronotop.redis.handlers.string.StringValue;
import com.kronotop.redis.storage.syncer.VolumeSyncer;
import com.kronotop.redis.storage.syncer.jobs.AppendHashFieldJob;
import com.kronotop.redis.storage.syncer.jobs.AppendStringJob;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RedisShardLoaderTest extends BaseStorageTest {

    @Test
    public void test_load_STRING() {
        final String key = "foobar";
        final byte[] value = "barfoo".getBytes();

        final RedisService service = context.getService(RedisService.NAME);
        RedisShard shard = service.findShard(key);
        shard.storage().put(key, new RedisValueContainer(new StringValue(value)));
        shard.volumeSyncQueue().add(new AppendStringJob(key));

        VolumeSyncer volumeSyncer = new VolumeSyncer(context, shard);
        volumeSyncer.run();

        shard.storage().remove(key);
        assertNull(shard.storage().get(key));

        // RedisShardLoader should load the key/value pair from the underlying volume.
        RedisShardLoader loader = new RedisShardLoader(context, shard);
        loader.load();
        RedisValueContainer container = shard.storage().get(key);
        assertNotNull(container);

        assertArrayEquals(value, container.string().value());
        assertNotNull(container.baseRedisValue().versionstamp());
    }

    @Test
    public void test_load_HashField() {
        final String key = "foobar";
        final byte[] value = "barfoo".getBytes();
        final String field = "bar";

        final RedisService service = context.getService(RedisService.NAME);
        RedisShard shard = service.findShard("foobar");

        HashValue hashValue = new HashValue();
        hashValue.put(field, new HashFieldValue(value));

        shard.storage().put(key, new RedisValueContainer(hashValue));
        shard.volumeSyncQueue().add(new AppendHashFieldJob(key, field));

        VolumeSyncer volumeSyncer = new VolumeSyncer(context, shard);
        volumeSyncer.run();

        shard.storage().remove(key);
        assertNull(shard.storage().get(key));

        RedisShardLoader loader = new RedisShardLoader(context, shard);
        loader.load();
        RedisValueContainer container = shard.storage().get(key);
        assertNotNull(container);

        assertNotNull(container.hash().get(field));
        assertArrayEquals(value, container.hash().get(field).value());
        assertNotNull(container.hash().get(field).versionstamp());
    }
}
