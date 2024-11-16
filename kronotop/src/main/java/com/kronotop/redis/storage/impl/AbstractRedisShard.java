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
import com.kronotop.Context;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.impl.ShardImpl;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.redis.storage.index.Index;
import com.kronotop.redis.storage.syncer.VolumeSyncQueue;
import com.kronotop.volume.Volume;
import com.kronotop.volume.VolumeConfig;
import com.kronotop.volume.VolumeConfigGenerator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * This abstract class represents a Redis shard implementation that extends the ShardImpl class and implements the RedisShard interface.
 * It provides functionality for creating or opening a volume, managing storage, and controlling shard properties.
 */
public abstract class AbstractRedisShard extends ShardImpl implements RedisShard {
    private final Index index;
    private final VolumeSyncQueue volumeSyncQueue;
    private final Striped<ReadWriteLock> striped = Striped.lazyWeakReadWriteLock(271);
    private final ConcurrentMap<String, RedisValueContainer> storage;
    private final Volume volume;
    private volatile boolean operable;

    protected AbstractRedisShard(Context context, Integer id) {
        super(context, ShardKind.REDIS, id);

        this.volumeSyncQueue = new RedisShardVolumeSyncQueue(this);
        this.index = new RedisShardIndex(id, this);
        this.storage = new Storage(this);

        VolumeConfig volumeConfig = new VolumeConfigGenerator(context, ShardKind.REDIS, id).volumeConfig();
        try {
            this.volume = volumeService.newVolume(volumeConfig);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public ShardKind kind() {
        return ShardKind.REDIS;
    }

    @Override
    public ConcurrentMap<String, RedisValueContainer> storage() {
        return storage;
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
    public VolumeSyncQueue volumeSyncQueue() {
        return volumeSyncQueue;
    }

    @Override
    public Volume volume() {
        return volume;
    }

    @Override
    public void close() {
        volume.close();
    }

    @Override
    public void setOperable(boolean operable) {
        this.operable = operable;
    }

    @Override
    public boolean operable() {
        return operable;
    }
}