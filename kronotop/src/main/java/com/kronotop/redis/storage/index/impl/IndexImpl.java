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

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class IndexImpl implements Index {
    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();
    private final List<Long> indexes = new ArrayList<>(1000000);
    private final ConcurrentHashMap<Long, String> keys = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> reverseKeys = new ConcurrentHashMap<>();
    private final AtomicLong counter = new AtomicLong(0);
    private final Random random = new Random();

    public IndexImpl() {
    }

    public void update(String key) {
        long index = counter.incrementAndGet();
        Long previousIndex = reverseKeys.remove(key);
        if (previousIndex != null) {
            keys.remove(previousIndex);
        }

        rwlock.writeLock().lock();
        try {
            indexes.add(index);
        } finally {
            rwlock.writeLock().unlock();
        }
        keys.put(index, key);
        reverseKeys.put(key, index);
    }

    public void drop(String key) {
        Long index = reverseKeys.remove(key);
        if (index != null)
            keys.remove(index);
    }

    public Projection getProjection(int offset, int count) {
        List<String> aggregatedKeys = new ArrayList<>();
        rwlock.readLock().lock();
        try {
            ListIterator<Long> iterator = indexes.listIterator(offset);
            int total = 0;
            int cursor = offset;
            while (iterator.hasNext()) {
                if (total >= count) {
                    break;
                }
                cursor++;

                long index = iterator.next();
                String key = keys.get(index);
                if (key != null) {
                    aggregatedKeys.add(key);
                    total++;
                }
            }

            if (!iterator.hasNext()) {
                cursor = 0;
            }
            return new Projection(aggregatedKeys, cursor);
        } catch (IndexOutOfBoundsException e) {
            return new Projection(aggregatedKeys, 0);
        } finally {
            rwlock.readLock().unlock();
        }
    }

    public List<String> tryGetRandomKeys(int count) {
        List<Long> randomIndexes = new ArrayList<>();
        List<String> randomKeys = new ArrayList<>();
        rwlock.readLock().lock();
        try {
            if (indexes.size() == 0) {
                return randomKeys;
            }
            for (int i = 0; i < count; i++) {
                int randomNum = random.nextInt((indexes.size() + 1));
                Long index = indexes.get(randomNum);
                randomIndexes.add(index);
            }
        } finally {
            rwlock.readLock().unlock();
        }

        for (Long randomIndex : randomIndexes) {
            String key = keys.get(randomIndex);
            randomKeys.add(key);
        }
        return randomKeys;
    }
}
