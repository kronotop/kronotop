/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.worker;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * A thread-safe registry for managing workers organized by prefix and tag.
 *
 * <p>Workers are stored in a two-level hierarchy: prefix → tag → list of workers.
 * This enables efficient prefix-based lookups for operations like namespace removal,
 * where all workers under a namespace prefix must be shut down.
 *
 * <p>Workers are expected to remove themselves from the registry upon completion
 * via the {@link #remove(String, Worker)} method.
 */
public class WorkerRegistry {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final ConcurrentSkipListMap<String, Map<String, List<Worker>>> registry = new ConcurrentSkipListMap<>();

    /**
     * Registers a worker under the specified namespace.
     *
     * @param prefix the namespace to register the worker under
     * @param worker the worker to register
     */
    public void put(String prefix, Worker worker) {
        lock.writeLock().lock();
        try {
            Map<String, List<Worker>> workers = registry.computeIfAbsent(prefix, (ignored) -> new HashMap<>());
            List<Worker> registeredWorkers = workers.computeIfAbsent(worker.getTag(), (ignored) -> new ArrayList<>());
            registeredWorkers.add(worker);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Retrieves all workers with the specified tag under the given namespace.
     *
     * @param prefix the namespace to search in
     * @param tag    the tag of the workers to retrieve
     * @return a list of workers, or an empty list if none found
     */
    public List<Worker> get(String prefix, String tag) {
        lock.readLock().lock();
        try {
            Map<String, List<Worker>> workers = registry.get(prefix);
            if (workers == null) {
                return List.of();
            }
            List<Worker> registeredWorkers = workers.get(tag);
            if (registeredWorkers == null || registeredWorkers.isEmpty()) {
                return List.of();
            }
            return List.copyOf(registeredWorkers);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Retrieves all workers under namespaces matching the given prefix.
     *
     * @param prefix the namespace prefix to match
     * @return an unmodifiable map of matching namespaces to their workers
     */
    public NavigableMap<String, Map<String, List<Worker>>> get(String prefix) {
        lock.readLock().lock();
        try {
            NavigableMap<String, Map<String, List<Worker>>> subMap = registry.subMap(prefix, true, prefix + Character.MAX_VALUE, true);
            NavigableMap<String, Map<String, List<Worker>>> snapshot = subMap.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> e.getValue().entrySet().stream()
                                    .collect(Collectors.toMap(
                                            Map.Entry::getKey,
                                            ie -> new ArrayList<>(ie.getValue()) // Copy the list
                                    )),
                            (oldV, newV) -> oldV,
                            TreeMap::new
                    ));
            return Collections.unmodifiableNavigableMap(snapshot);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Removes a specific worker from the given namespace.
     *
     * @param prefix the namespace containing the worker
     * @param worker the worker to remove
     * @return true if the worker was removed, false if not found
     */
    public boolean remove(String prefix, Worker worker) {
        lock.writeLock().lock();
        try {
            Map<String, List<Worker>> workers = registry.get(prefix);
            if (workers == null) {
                return false;
            }
            List<Worker> registeredWorkers = workers.get(worker.getTag());
            if (registeredWorkers == null) {
                return false;
            }
            boolean result = registeredWorkers.remove(worker);
            // cleanup
            if (registeredWorkers.isEmpty()) {
                workers.remove(worker.getTag());
            }
            if (workers.isEmpty()) {
                registry.remove(prefix);
            }
            return result;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes all workers under namespaces matching the given prefix.
     *
     * @param prefix the namespace prefix to match for removal
     */
    public void remove(String prefix) {
        lock.writeLock().lock();
        try {
            NavigableMap<String, Map<String, List<Worker>>> subMap = registry.subMap(prefix, true, prefix + Character.MAX_VALUE, true);
            subMap.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
