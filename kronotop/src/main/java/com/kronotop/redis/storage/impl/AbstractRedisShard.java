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

import com.google.common.util.concurrent.Striped;
import com.kronotop.cluster.sharding.impl.ShardImpl;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.index.Index;
import com.kronotop.redis.storage.persistence.PersistenceQueue;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * This abstract class represents a Redis shard implementation that extends the ShardImpl class and implements the RedisShard interface.
 * It provides functionality for creating or opening a volume, managing storage, and controlling shard properties.
 */
public abstract class AbstractRedisShard extends ShardImpl implements RedisShard {
    private final Index index;
    private final PersistenceQueue persistenceQueue;
    private final Striped<ReadWriteLock> striped = Striped.lazyWeakReadWriteLock(271);
    private final ConcurrentMap<String, Object> storage;
    private volatile boolean readOnly;
    private volatile boolean operable;

    protected AbstractRedisShard(Integer id) {
        super(id);

        this.persistenceQueue = new RedisShardPersistenceQueue(this);
        this.index = new RedisShardIndex(id, this);
        this.storage = new Storage(this);
    }

    @Override
    public ConcurrentMap<String, Object> storage() {
        return storage;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    public Striped<ReadWriteLock> striped() {
        return striped;
    }

    @Override
    public Index index() {
        return index;
    }

    @Override
    public PersistenceQueue persistenceQueue() {
        return persistenceQueue;
    }

    @Override
    public boolean isOperable() {
        return operable;
    }

    @Override
    public void setOperable(boolean operable) {
        this.operable = operable;
    }
}