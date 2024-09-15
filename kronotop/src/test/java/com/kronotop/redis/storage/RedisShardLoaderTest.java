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

import com.apple.foundationdb.Transaction;
import com.kronotop.redis.storage.persistence.*;
import com.kronotop.redis.storage.persistence.jobs.AppendStringJob;
import com.kronotop.redis.string.StringValue;
import com.kronotop.redis.storage.impl.OnHeapRedisShardImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RedisShardLoaderTest extends BaseStorageTest {
    @Test
    public void testDataStructureLoader_STRING() {
        RedisShard shard = new OnHeapRedisShardImpl(context, 0);

        for (int i = 0; i < 10; i++) {
            String key = String.format("key-%d", i);
            String value = String.format("value-%d", i);
            shard.storage().put(key, new RedisValueContainer(new StringValue(value.getBytes(), 0L)));
            shard.persistenceQueue().add(new AppendStringJob(key));
        }

        Persistence persistence = new Persistence(context, shard);
        persistence.run();

        RedisShard newShard = new OnHeapRedisShardImpl(context, 0);
        RedisShardLoader shardLoader = new RedisShardLoader(context, newShard);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            shardLoader.load(tr, DataStructure.STRING);
        }

        assertEquals(shard.storage().size(), newShard.storage().size());

        for (String key : newShard.storage().keySet()) {
            Object newObj = newShard.storage().get(key);
            StringValue newValue = (StringValue) newObj;

            Object obj = shard.storage().get(key);
            StringValue value = (StringValue) obj;

            assertEquals(value.ttl(), newValue.ttl());
            assertEquals(new String(value.value()), new String(newValue.value()));
        }
    }
}
