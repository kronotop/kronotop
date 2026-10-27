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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VectorGraphIndexGenerationIdTest {

    private static final int CAPACITY = 100;

    @Test
    void shouldReturnInitialGenerationIdOnFirstCall() {
        // Behavior: the first call returns the initial generation id unchanged.
        VectorGraphIndexGenerationId genId = new VectorGraphIndexGenerationId(7, CAPACITY);
        assertEquals(7, genId.getGenerationId());
    }

    @Test
    void shouldKeepGenerationIdStableForCapacityCalls() {
        // Behavior: the first 100 calls all return the initial generation id.
        VectorGraphIndexGenerationId genId = new VectorGraphIndexGenerationId(0, CAPACITY);
        for (int i = 0; i < CAPACITY; i++) {
            assertEquals(0, genId.getGenerationId(), "call " + (i + 1));
        }
    }

    @Test
    void shouldIncrementGenerationIdAfterCapacityCalls() {
        // Behavior: the call after 100 calls returns initial + 1.
        VectorGraphIndexGenerationId genId = new VectorGraphIndexGenerationId(0, CAPACITY);
        for (int i = 0; i < CAPACITY; i++) {
            genId.getGenerationId();
        }
        assertEquals(1, genId.getGenerationId());
    }

    @Test
    void shouldIncrementGenerationIdByOneOnEachOverflow() {
        // Behavior: every time the counter passes the capacity, the generation id grows by exactly one.
        VectorGraphIndexGenerationId genId = new VectorGraphIndexGenerationId(0, CAPACITY);
        long previous = genId.getGenerationId();
        long maxSeen = previous;
        for (int i = 0; i < CAPACITY * 5; i++) {
            long current = genId.getGenerationId();
            assertTrue(current == previous || current == previous + 1, "jump at call " + (i + 2));
            previous = current;
            maxSeen = Math.max(maxSeen, current);
        }
        assertTrue(maxSeen >= 4);
    }
    
    @Test
    void shouldIncrementOnEveryCallWhenCapacityIsZero() {
        // Behavior: with capacity 0 each call moves to the next generation id.
        VectorGraphIndexGenerationId genId = new VectorGraphIndexGenerationId(0, 0);
        assertEquals(1, genId.getGenerationId());
        assertEquals(2, genId.getGenerationId());
        assertEquals(3, genId.getGenerationId());
    }
}
