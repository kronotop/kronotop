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


