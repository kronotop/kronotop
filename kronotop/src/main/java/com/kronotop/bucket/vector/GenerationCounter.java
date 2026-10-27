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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hands out generation ids for vector graph index nodes.
 * <p>
 * {@link #take()} counts one use of the current generation and returns its byte form.
 * {@link #next()} returns the current generation id and starts a new one once the
 * use count passes the minimum capacity. Thread-safe.
 */
public class GenerationCounter {
    private final int minCapacity;
    private volatile ObjectId generationId;
    private volatile byte[] generationIdAsBytes;
    private final AtomicInteger counter = new AtomicInteger();

    /**
     * @param initial     first generation id to return
     * @param minCapacity number of {@link #take()} calls served before a new id is created
     */
    public GenerationCounter(ObjectId initial, int minCapacity) {
        this.generationId = initial;
        this.generationIdAsBytes = initial.toByteArray();
        this.minCapacity = minCapacity;
    }

    /**
     * Returns the current generation id. Does not count the call.
     * A new ObjectId replaces the current id once the {@link #take()} count passes the minimum capacity.
     */
    public synchronized ObjectId next() {
        if (counter.get() > minCapacity) {
            counter.set(0);
            generationId = new ObjectId();
            generationIdAsBytes = generationId.toByteArray();
        }
        return generationId;
    }

    /**
     * Counts one use of the current generation and returns its byte form.
     * The array is cached per generation; callers must not modify it.
     * Lock-free; runs in parallel with other callers.
     */
    public byte[] take() {
        counter.incrementAndGet();
        return generationIdAsBytes;
    }
}
