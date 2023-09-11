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

package com.kronotop.redis.storage.index.impl;

import com.kronotop.redis.storage.index.Index;
import com.kronotop.redis.storage.index.Projection;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class IndexImpl implements Index {
    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();
    private final List<BufferEntry> buffer = new ArrayList<>();
    private final ConcurrentHashMap<String, Long> keys = new ConcurrentHashMap<>();
    private final ConcurrentSkipListMap<Long, String> index = new ConcurrentSkipListMap<>();
    private final FlakeIdGenerator flakeIdGenerator;

    public IndexImpl(long partitionId) {
        this.flakeIdGenerator = new FlakeIdGenerator(partitionId);
    }

    public void add(String key) {
        if (keys.containsKey(key)) {
            return;
        }

        rwlock.writeLock().lock();
        try {
            long id = flakeIdGenerator.nextId();
            buffer.add(new BufferEntry(id, key));
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    public void remove(String key) {
        Long id = keys.remove(key);
        if (id != null) {
            index.remove(id);
        } else {
            rwlock.writeLock().lock();
            try {
                for (int i = 0; i < buffer.size(); i++) {
                    BufferEntry entry = buffer.get(i);
                    if (entry.key.equals(key)) {
                        buffer.remove(i);
                        break;
                    }
                }
            } finally {
                rwlock.writeLock().unlock();
            }
        }
    }

    public Long head() {
        try {
            Long head = index.firstKey();
            if (head != null) {
                return head - 1;
            }
        } catch (NoSuchElementException e) {
            // Index is empty.
        }

        rwlock.readLock().lock();
        try {
            if (buffer.isEmpty()) {
                return 0L;
            }
            return buffer.get(0).id - 1;
        } finally {
            rwlock.readLock().unlock();
        }
    }

    public void flushBuffer() {
        rwlock.writeLock().lock();
        try {
            int total = 0;
            for (ListIterator<BufferEntry> iterator = buffer.listIterator(); iterator.hasNext(); ) {
                BufferEntry entry = iterator.next();
                keys.put(entry.key, entry.id);
                index.put(entry.id, entry.key);
                iterator.remove();
                total++;
                if (total > 10000) {
                    break;
                }
            }
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    public Projection getProjection(long cursor, int count) {
        List<String> aggregatedKeys = new ArrayList<>();

        long nextCursor = cursor;
        for (int i = 0; i < count; i++) {
            Map.Entry<Long, String> entry = index.higherEntry(nextCursor);
            if (entry == null) {
                break;
            }
            aggregatedKeys.add(entry.getValue());
            nextCursor = entry.getKey();
        }

        if (aggregatedKeys.size() >= count) {
            rwlock.readLock().lock();
            try {
                if (buffer.isEmpty() && index.higherKey(nextCursor) == null) {
                    nextCursor = 0;
                }
            } finally {
                rwlock.readLock().unlock();
            }
            return new Projection(aggregatedKeys, nextCursor);
        }

        rwlock.readLock().lock();
        try {
            if (buffer.isEmpty() && index.higherKey(nextCursor) == null) {
                return new Projection(aggregatedKeys, 0);
            }

            boolean bottom = false;
            for (int i = 0; i < buffer.size(); i++) {
                if (i + 1 == buffer.size()) {
                    bottom = true;
                }

                BufferEntry bufferEntry = buffer.get(i);
                if (keys.containsKey(bufferEntry.key) || nextCursor >= bufferEntry.id) {
                    continue;
                }
                aggregatedKeys.add(bufferEntry.key);
                nextCursor = bufferEntry.id;

                if (aggregatedKeys.size() >= count) {
                    break;
                }
            }

            if ((buffer.isEmpty() || bottom) && index.higherKey(nextCursor) == null) {
                nextCursor = 0;
            }
        } finally {
            rwlock.readLock().unlock();
        }

        return new Projection(aggregatedKeys, nextCursor);
    }

    static class BufferEntry {
        private final String key;
        private final Long id;

        public BufferEntry(Long id, String key) {
            this.id = id;
            this.key = key;
        }
    }
}
