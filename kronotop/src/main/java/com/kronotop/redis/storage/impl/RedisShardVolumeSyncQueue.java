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

import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.ShardReadOnlyException;
import com.kronotop.redis.storage.syncer.VolumeSyncQueue;
import com.kronotop.redis.storage.syncer.impl.OnHeapVolumeSyncQueue;
import com.kronotop.redis.storage.syncer.jobs.VolumeSyncJob;

/**
 * A class that represents an in-memory shard syncer queue.
 * It extends the {@link OnHeapVolumeSyncQueue} class and implements the {@link VolumeSyncQueue} interface.
 * The queue is associated with a specific shard.
 *
 * <p>
 * Usage example:
 * <pre>{@code
 * Shard shard = new OnHeapShardImpl(0);
 * shard.setReadOnly(true);
 *
 * RedisShardVolumeSyncQueue queue = new RedisShardVolumeSyncQueue(shard);
 * queue.add(new StringKey("foo"));
 * }</pre>
 */
public class RedisShardVolumeSyncQueue extends OnHeapVolumeSyncQueue implements VolumeSyncQueue {
    private final RedisShard shard;

    public RedisShardVolumeSyncQueue(RedisShard shard) {
        this.shard = shard;
    }

    @Override
    public void add(VolumeSyncJob job) {
        if (shard.isReadOnly()) {
            throw new ShardReadOnlyException(shard.id());
        }
        super.add(job);
    }
}
