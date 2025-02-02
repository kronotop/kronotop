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

package com.kronotop.redis.storage.syncer.impl;

import com.kronotop.redis.storage.syncer.VolumeSyncQueue;
import com.kronotop.redis.storage.syncer.jobs.VolumeSyncJob;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A class that represents an in-memory volume sync queue implementation. It implements the {@link VolumeSyncQueue} interface.
 * <p>
 * The queue ensures that the business logic is executed atomically. This class uses a {@link Lock} to synchronize
 * concurrent calls to the queue methods.
 * <p>
 * It also uses a {@link ConcurrentHashMap} to maintain an index of the keys in the queue. The index is used to avoid adding
 * duplicate keys to the queue. The class uses a {@link ConcurrentLinkedQueue} to maintain the actual queue of keys.
 */
public class OnHeapVolumeSyncQueue implements VolumeSyncQueue {
    private final Lock lock = new ReentrantLock(true);
    private final ConcurrentHashMap<VolumeSyncJob, Boolean> index = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<VolumeSyncJob> queue = new ConcurrentLinkedQueue<>();

    public void add(VolumeSyncJob job) {
        // Runs the business logic atomically.
        index.compute(job, (k, value) -> {
            if (value == null) {
                queue.add(job);
                return true;
            }
            // Already added to the queue.
            return value;
        });
    }

    public List<VolumeSyncJob> poll(int count) {
        List<VolumeSyncJob> result = new ArrayList<>();
        // We use a lock here because concurrent calls to poll and clear
        // methods may lead to very nasty concurrency bugs.
        lock.lock();
        try {
            for (int i = 0; i < count; i++) {
                // Read-only - peek retrieves, but does not remove, the head of
                // this queue, or returns null if this queue is empty.
                VolumeSyncJob key = queue.peek();
                if (key == null) {
                    break;
                }
                // Runs the business logic atomically.
                index.compute(key, (k, value) -> {
                    if (value != null) {
                        result.add(queue.poll());
                    }
                    // Remove
                    return null;
                });
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    public void clear() {
        lock.lock();
        try {
            queue.clear();
            index.clear();
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        return queue.size();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }
}
