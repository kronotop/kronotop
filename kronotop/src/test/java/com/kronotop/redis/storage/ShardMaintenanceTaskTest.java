/*
 * Copyright (c) 2023 Kronotop
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

import com.kronotop.redis.StringValue;
import com.kronotop.redis.storage.impl.OnHeapShardImpl;
import com.kronotop.redis.storage.persistence.StringKey;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShardMaintenanceTaskTest extends BaseStorageTest {
    @Test
    public void testRun() {
        Shard shard = new OnHeapShardImpl(0);
        shard.put("key-1", new StringValue("value-1".getBytes(), 0));
        shard.getPersistenceQueue().add(new StringKey("key-1"));

        context.getLogicalDatabase().getShards().put(0, shard);
        ShardMaintenanceTask shardMaintenanceTask = new ShardMaintenanceTask(context, 0);
        shardMaintenanceTask.run();

        assertEquals(0, shard.getPersistenceQueue().size());

    }
}
