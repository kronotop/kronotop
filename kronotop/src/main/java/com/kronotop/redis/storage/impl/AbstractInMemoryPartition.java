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
import com.kronotop.redis.storage.index.Index;
import com.kronotop.redis.storage.index.impl.IndexImpl;
import com.kronotop.redis.storage.persistence.PersistenceQueue;
import com.kronotop.redis.storage.persistence.impl.OnHeapPersistenceQueue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;

public abstract class AbstractInMemoryPartition extends ConcurrentHashMap<String, Object> implements ConcurrentMap<String, Object> {
    private final Integer id;
    private final Index index = new IndexImpl();
    private final PersistenceQueue persistenceQueue = new OnHeapPersistenceQueue();
    private final Striped<ReadWriteLock> striped = Striped.lazyWeakReadWriteLock(271);

    protected AbstractInMemoryPartition(Integer id) {
        this.id = id;
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

    public Integer getId() {
        return id;
    }
}
