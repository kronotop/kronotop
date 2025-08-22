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

package com.kronotop.bucket.executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Cursor {
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Map<Integer, CursorState> states = new HashMap<>();


    public void setState(int id, CursorState state) {
        lock.writeLock().lock();
        try {
            states.put(id, state);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // Convenience methods for bounds management via CursorState and PhysicalNode IDs

    /**
     * Retrieves bounds for a PhysicalNode by its ID.
     *
     * @param nodeId the PhysicalNode ID
     * @return the Bounds associated with the node, or null if not found
     */
    public Bounds getBounds(int nodeId) {
        lock.readLock().lock();
        try {
            CursorState state = states.get(nodeId);
            return state != null ? state.bounds() : null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Sets bounds for a PhysicalNode by its ID.
     *
     * @param nodeId the PhysicalNode ID
     * @param bounds the Bounds to set
     */
    public void setBounds(int nodeId, Bounds bounds) {
        lock.writeLock().lock();
        try {
            CursorState newState = new CursorState(bounds);
            states.put(nodeId, newState);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<CursorState> getAllCursorStates() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(states.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Clears all cursor states.
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            states.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Gets the number of cursor states.
     *
     * @return the number of cursor states
     */
    public int size() {
        lock.readLock().lock();
        try {
            return states.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Copies all cursor states from another Cursor instance.
     * This replaces the deprecated bounds() copying approach.
     *
     * @param other the cursor to copy states from
     */
    public void copyStatesFrom(Cursor other) {
        lock.writeLock().lock();
        try {
            states.clear();
            states.putAll(other.states);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Copies cursor states to another Cursor instance.
     * This replaces the deprecated bounds() copying approach.
     *
     * @param other the cursor to copy states to
     */
    public void copyStatesTo(Cursor other) {
        other.copyStatesFrom(this);
    }
}
