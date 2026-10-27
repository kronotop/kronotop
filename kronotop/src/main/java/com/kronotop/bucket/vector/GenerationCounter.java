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
 * Returns the generation id for vector mutations. All mutations with the same id go to the same
 * on-heap graph. A generation serves a bounded number of mutations, then a new id replaces it.
 * Thread-safe.
 */
public class GenerationCounter {
    private final int minCapacity;
    private volatile byte[] generationIdAsBytes;
    private final AtomicInteger counter = new AtomicInteger();

    /**
     * @param initial     generation id in use at start
     * @param minCapacity mutations served by one generation before a new id is created
     */
    public GenerationCounter(ObjectId initial, int minCapacity) {
        this.generationIdAsBytes = initial.toByteArray();
        this.minCapacity = minCapacity;
    }

    /**
     * Counts one mutation and returns its generation id as bytes.
     * The array is shared by all mutations of the same generation; callers must not modify it.
     * Lock-free except on the call that starts a new generation.
     */
    public byte[] take() {
        if (counter.incrementAndGet() > minCapacity) {
            synchronized (this) {
                if (counter.get() > minCapacity) {
                    counter.set(0);
                    generationIdAsBytes = new ObjectId().toByteArray();
                }
            }
        }
        return generationIdAsBytes;
    }
}
