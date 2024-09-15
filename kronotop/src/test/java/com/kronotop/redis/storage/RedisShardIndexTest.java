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

import com.kronotop.redis.storage.impl.OnHeapRedisShardImpl;
import com.kronotop.redis.storage.impl.RedisShardIndex;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class RedisShardIndexTest extends BaseStorageTest {
    @Test
    public void test_add() {
        RedisShard shard = new OnHeapRedisShardImpl(context, 0);
        shard.setReadOnly(true);
        RedisShardIndex index = new RedisShardIndex(0, shard);
        assertThrows(ShardReadOnlyException.class, () -> index.add("foo"));
    }

    @Test
    public void test_remove() {
        RedisShard shard = new OnHeapRedisShardImpl(context, 0);
        shard.setReadOnly(true);
        RedisShardIndex index = new RedisShardIndex(0, shard);
        assertThrows(ShardReadOnlyException.class, () -> index.remove("foo"));
    }
}