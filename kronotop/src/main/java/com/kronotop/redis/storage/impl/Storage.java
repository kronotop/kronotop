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

package com.kronotop.redis.storage.impl;

import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.redis.storage.ShardInoperableException;
import com.kronotop.redis.storage.ShardReadOnlyException;

import javax.annotation.Nonnull;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

public class Storage extends ConcurrentHashMap<String, RedisValueContainer> {
    private final RedisShard shard;

    public Storage(RedisShard shard) {
        this.shard = shard;
    }

    private void checkShardStatus() {
        if (shard.status().equals(ShardStatus.READONLY)) {
            throw new ShardReadOnlyException(shard.id());
        }
        if (shard.status().equals(ShardStatus.INOPERABLE)) {
            throw new ShardInoperableException(shard.id());
        }
    }

    @Override
    public RedisValueContainer put(@Nonnull String key, @Nonnull RedisValueContainer value) {
        checkShardStatus();
        return super.put(key, value);
    }

    @Override
    public RedisValueContainer remove(@Nonnull Object key) {
        checkShardStatus();
        return super.remove(key);
    }

    @Override
    public boolean remove(@Nonnull Object key, Object value) {
        checkShardStatus();
        return super.remove(key, value);
    }

    @Override
    public RedisValueContainer compute(String key, @Nonnull BiFunction<? super String, ? super RedisValueContainer, ? extends RedisValueContainer> remappingFunction) {
        checkShardStatus();
        return super.compute(key, remappingFunction);
    }

    @Override
    public RedisValueContainer computeIfAbsent(String key, @Nonnull Function<? super String, ? extends RedisValueContainer> mappingFunction) {
        checkShardStatus();
        return super.computeIfAbsent(key, mappingFunction);
    }
}
