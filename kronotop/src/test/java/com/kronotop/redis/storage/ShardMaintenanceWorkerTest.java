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

import com.kronotop.ServiceContext;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.storage.impl.OnHeapRedisShardImpl;
import com.kronotop.redis.storage.persistence.RedisValueContainer;
import com.kronotop.redis.storage.persistence.jobs.AppendStringJob;
import com.kronotop.redis.string.StringValue;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShardMaintenanceWorkerTest extends BaseStorageTest {
    @Test
    public void testRun() {
        RedisShard shard = new OnHeapRedisShardImpl(context, 0);
        shard.storage().put("key-1", new RedisValueContainer(new StringValue("value-1".getBytes(), 0L)));
        shard.persistenceQueue().add(new AppendStringJob("key-1"));

        ServiceContext<RedisShard> redisContext = context.getServiceContext(RedisService.NAME);
        redisContext.shards().put(0, shard);
        RedisShardMaintenanceWorker shardMaintenanceWorker = new RedisShardMaintenanceWorker(context, 0);
        shardMaintenanceWorker.run();

        assertEquals(0, shard.persistenceQueue().size());

    }
}
