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

package com.kronotop.redis.storage.index.impl;

import com.kronotop.redis.storage.index.Index;
import com.kronotop.redis.storage.index.Projection;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * IndexImpl is an implementation of the Index interface that provides thread-safe
 * operations for managing an in-memory buffer and index for key-value mapping.
 * This class uses multiple data structures, such as a buffer, map, and skip-list,
 * to ensure efficient storage and retrieval of data while allowing concurrent
 * access and modifications.
 * <p>
 * Thread safety is achieved through ReadWriteLock for controlling
 * read and write access, along with additional thread-safe collections.
 */
public class IndexImpl implements Index {
    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();
    private final List<BufferEntry> buffer = new ArrayList<>();
    private final ConcurrentHashMap<String, Long> keys = new ConcurrentHashMap<>();
    private final ConcurrentSkipListMap<Long, String> index = new ConcurrentSkipListMap<>();
    private final FlakeIdGenerator flakeIdGenerator;

    public IndexImpl(long shardId) {
        this.flakeIdGenerator = new FlakeIdGenerator(shardId);
    }

    /**
     * Adds a unique key to the buffer and assigns it a generated identifier if it does not already exist.
     * Ensures thread safety through the use of a write lock.
     *
     * @param key the key to be added to the buffer. Must not yet exist in the current key map.
     */
    public void add(String key) {
        if (keys.containsKey(key)) {
            return;
        }

        rwlock.writeLock().lock();
        try {
            long id = flakeIdGenerator.nextId();
            buffer.add(new BufferEntry(id, key));
            keys.put(key, id);
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    /**
     * Returns the total size of the index by combining the sizes of the in-memory buffer
     * and the index data structure. This is a thread-safe operation and ensures
     * consistent access to the data using a read lock.
     *
     * @return the total number of elements in the in-memory buffer and the index
     */
    public int size() {
        rwlock.readLock().lock();
        try {
            return buffer.size() + index.size();
        } finally {
            rwlock.readLock().unlock();
        }
    }

    /**
     * Removes the specified key from the index and associated data structures.
     * Ensures thread safety through a write lock while modifying the buffer.
     *
     * @param key the key to be removed from the index and buffer
     */
    public void remove(String key) {
        Long id = keys.remove(key);
        if (id != null) {
            index.remove(id);
        }
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

    /**
     * Retrieves a random key from the index or buffer. The method first attempts to
     * find a random key from the index. If the index does not contain any entries,
     * it attempts to retrieve a random key from an in-memory buffer. If both the index
     * and buffer are empty, a NoSuchElementException is thrown.
     * <p>
     * Thread safety is ensured by acquiring a read lock when accessing the buffer.
     *
     * @return a random key as a String from the index or buffer
     * @throws NoSuchElementException if both the index and buffer are empty
     */
    public String random() {
        try {
            if (index.size() == 1) {
                return index.get(index.firstKey());
            }
            Long max = index.lastKey();
            Long min = index.firstKey();
            long randId = ThreadLocalRandom.current().nextLong(max - min) + min;
            Map.Entry<Long, String> entry = index.ceilingEntry(randId);
            return entry.getValue();
        } catch (NoSuchElementException e) {
            rwlock.readLock().lock();
            try {
                if (!buffer.isEmpty()) {
                    int randId = ThreadLocalRandom.current().nextInt(buffer.size());
                    BufferEntry entry = buffer.get(randId);
                    return entry.key;
                }
            } finally {
                rwlock.readLock().unlock();
            }
        }

        throw new NoSuchElementException();
    }

    /**
     * Retrieves the position marker of the index or buffer, adjusted by subtracting one.
     * If the index contains entries, the position marker is derived from the first key in the index.
     * Otherwise, the method acquires a read lock to safely access the in-memory buffer
     * and calculates the position marker based on the first buffer entry's identifier.
     * If both the index and buffer are empty, the method returns zero as the default value.
     * <p>
     * Thread safety is ensured through the appropriate use of a read lock while accessing the buffer.
     *
     * @return the position marker of the index or buffer (decremented by one), or zero if both are empty
     */
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
            return buffer.getFirst().id - 1;
        } finally {
            rwlock.readLock().unlock();
        }
    }

    /**
     * Flushes a portion of the in-memory buffer by transferring its entries to the index.
     * This operation locks the write lock to ensure thread safety during the flush process.
     * The buffer entries are sequentially iterated, and each entry is added to the index.
     * Processed entries are then removed from the buffer.
     * <p>
     * The operation is designed to handle up to a maximum of 10,000 entries in one flush
     * cycle to maintain performance and prevent potential memory or processing overhead.
     * If the buffer is already empty, the method returns immediately without performing
     * any operations.
     * <p>
     * Thread safety is ensured through the use of a write lock, which is acquired before
     * starting the process and released upon completion or in case of any exceptions.
     */
    public void flush() {
        rwlock.writeLock().lock();
        try {
            if (buffer.isEmpty()) {
                // It's cheap.
                return;
            }

            int total = 0;
            for (ListIterator<BufferEntry> iterator = buffer.listIterator(); iterator.hasNext(); ) {
                BufferEntry entry = iterator.next();
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

    /**
     * Retrieves a projection of keys from the index and buffer, beginning at the given cursor
     * and including up to the specified count of entries. The method traverses the index and,
     * if necessary, the in-memory buffer to aggregate the requested keys. It also determines
     * the next cursor position based on traversal results.
     * <p>
     * Thread safety is ensured using read locks when accessing shared data structures, such as
     * the index and buffer.
     *
     * @param cursor the starting position in the index or buffer from which to retrieve keys
     * @param count  the maximum number of keys to include in the projection
     * @return a {@link Projection} object containing the aggregated keys and the next cursor
     */
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
                if (nextCursor >= bufferEntry.id) {
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

    /**
     * Represents an immutable buffer entry consisting of a unique identifier and a key.
     * This class is used to store data temporarily in the in-memory buffer of the IndexImpl instance.
     * <p>
     * Instances of this class are created when adding new entries to the buffer and are uniquely
     * identified by their generated identifier.
     * <p>
     * Thread safety is ensured by the containing data structure (e.g., the in-memory buffer), as
     * this object is immutable and its state cannot be modified after creation.
     */
    static class BufferEntry {
        private final String key;
        private final Long id;

        public BufferEntry(Long id, String key) {
            this.id = id;
            this.key = key;
        }
    }
}
