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

import org.bson.types.ObjectId;

/**
 * Hands out generation ids for vector graph index nodes.
 * <p>
 * The same id is returned for a fixed number of calls. When the call count
 * passes the minimum capacity, a new ObjectId becomes the current id. Thread-safe.
 */
public class GenerationCounter {
    private final int minCapacity;
    private volatile ObjectId generationId;
    private volatile byte[] generationIdAsBytes;
    private int counter;

    /**
     * @param initial     first generation id to return
     * @param minCapacity number of calls served before a new id is created
     */
    public GenerationCounter(ObjectId initial, int minCapacity) {
        this.generationId = initial;
        this.generationIdAsBytes = initial.toByteArray();
        this.minCapacity = minCapacity;
    }

    /**
     * Returns the current generation id and counts the call.
     * A new ObjectId replaces the current id once the call count passes the minimum capacity.
     */
    public synchronized ObjectId next() {
        if (++counter > minCapacity) {
            counter = 0;
            generationId = new ObjectId();
            generationIdAsBytes = generationId.toByteArray();
        }
        return generationId;
    }

    /**
     * Returns the current generation id without counting the call.
     * Readers run in parallel; they wait while {@link #next()} runs.
     */
    public ObjectId current() {
        return generationId;
    }

    /**
     * Returns the byte form of the current generation id without counting the call.
     * The array is cached per generation; callers must not modify it.
     */
    public byte[] currentAsBytes() {
        return generationIdAsBytes;
    }
}
