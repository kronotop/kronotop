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


package com.kronotop.redis.storage.impl;

import com.google.common.util.concurrent.Striped;
import com.kronotop.redis.storage.Shard;
import com.kronotop.redis.storage.ShardReadOnlyException;
import com.kronotop.redis.storage.index.Index;
import com.kronotop.redis.storage.persistence.PersistenceQueue;

import javax.annotation.Nonnull;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class AbstractInMemoryShard extends ConcurrentHashMap<String, Object> implements Shard {
    private final Integer id;
    private final Index index;
    private final PersistenceQueue persistenceQueue;
    private final Striped<ReadWriteLock> striped = Striped.lazyWeakReadWriteLock(271);
    private volatile boolean readOnly;
    private volatile boolean operable;

    protected AbstractInMemoryShard(Integer id) {
        this.id = id;
        this.persistenceQueue = new InMemoryShardPersistenceQueue(this);
        this.index = new InMemoryShardIndex(id, this);
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public Striped<ReadWriteLock> getStriped() {
        return striped;
    }

    public Index getIndex() {
        return index;
    }

    public PersistenceQueue getPersistenceQueue() {
        return persistenceQueue;
    }

    public boolean isOperable() {
        return operable;
    }

    public void setOperable(boolean operable) {
        this.operable = operable;
    }

    public Integer getId() {
        return id;
    }

    @Override
    public Object put(@Nonnull String key, @Nonnull Object value) {
        if (readOnly) {
            throw new ShardReadOnlyException(id);
        }
        return super.put(key, value);
    }

    @Override
    public Object remove(@Nonnull Object key) {
        if (readOnly) {
            throw new ShardReadOnlyException(id);
        }
        return super.remove(key);
    }

    @Override
    public boolean remove(@Nonnull Object key, Object value) {
        if (readOnly) {
            throw new ShardReadOnlyException(id);
        }
        return super.remove(key, value);
    }

    @Override
    public Object compute(String key, @Nonnull BiFunction<? super String, ? super Object, ?> remappingFunction) {
        if (readOnly) {
            throw new ShardReadOnlyException(id);
        }
        return super.compute(key, remappingFunction);
    }

    @Override
    public Object computeIfAbsent(String key, @Nonnull Function<? super String, ?> mappingFunction) {
        if (readOnly) {
            throw new ShardReadOnlyException(id);
        }
        return super.computeIfAbsent(key, mappingFunction);
    }
}