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

import com.kronotop.redis.storage.Shard;
import com.kronotop.redis.storage.ShardReadOnlyException;
import com.kronotop.redis.storage.persistence.Key;
import com.kronotop.redis.storage.persistence.PersistenceQueue;
import com.kronotop.redis.storage.persistence.impl.OnHeapPersistenceQueue;

/**
 * A class that represents an in-memory shard persistence queue.
 * It extends the {@link OnHeapPersistenceQueue} class and implements the {@link PersistenceQueue} interface.
 * The queue is associated with a specific shard.
 *
 * <p>
 * Usage example:
 * <pre>{@code
 * Shard shard = new OnHeapShardImpl(0);
 * shard.setReadOnly(true);
 *
 * InMemoryShardPersistenceQueue queue = new InMemoryShardPersistenceQueue(shard);
 * queue.add(new StringKey("foo"));
 * }</pre>
 */
public class InMemoryShardPersistenceQueue extends OnHeapPersistenceQueue implements PersistenceQueue {
    private final Shard shard;

    public InMemoryShardPersistenceQueue(Shard shard) {
        this.shard = shard;
    }

    @Override
    public void add(Key key) {
        if (shard.isReadOnly()) {
            throw new ShardReadOnlyException(shard.getId());
        }
        super.add(key);
    }
}
