/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.bucket.vector;

/**
 * Hands out generation ids for vector graph index nodes.
 * <p>
 * The same id is returned for a fixed number of calls. When the call count
 * passes the capacity, the id moves to the next value. Thread-safe.
 */
public class VectorGraphIndexGenerationId {
    private final int capacity;
    private long generationId;
    private int counter;

    /**
     * @param initial  first generation id to return
     * @param capacity number of calls served before the id moves to the next value
     */
    public VectorGraphIndexGenerationId(long initial, int capacity) {
        this.generationId = initial;
        this.capacity = capacity;
    }

    /**
     * Returns the current generation id and counts the call.
     * The id grows by one once the call count passes the capacity.
     */
    public synchronized long getGenerationId() {
        if (++counter > capacity) {
            counter = 0;
            generationId++;
        }
        return generationId;
    }
}
