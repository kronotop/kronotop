/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket.pipeline;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

public abstract class AbstractDataSink<K, V> {
    private final int parentNodeId;

    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Map<K, V> sink = new LinkedHashMap<>();

    public AbstractDataSink(int parentNodeId) {
        this.parentNodeId = parentNodeId;
    }

    public int parentNodeId() {
        return parentNodeId;
    }

    public void append(K key, V value) {
        lock.writeLock().lock();
        try {
            sink.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public V get(K key) {
        lock.readLock().lock();
        try {
            return sink.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return sink.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            sink.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void forEach(BiConsumer<? super K, ? super V> action) {
        sink.forEach(action);
    }
}


