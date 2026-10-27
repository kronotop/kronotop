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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class GenerationCounterTest {

    private static final int CAPACITY = 100;

    @Test
    void shouldReturnInitialGenerationIdOnFirstCall() {
        // Behavior: the first call returns the initial generation id unchanged.
        ObjectId initial = new ObjectId();
        GenerationCounter genId = new GenerationCounter(initial, CAPACITY);
        assertEquals(initial, genId.next());
    }

    @Test
    void shouldKeepGenerationIdStableForCapacityCalls() {
        // Behavior: the first 100 calls all return the initial generation id.
        ObjectId initial = new ObjectId();
        GenerationCounter genId = new GenerationCounter(initial, CAPACITY);
        for (int i = 0; i < CAPACITY; i++) {
            assertEquals(initial, genId.next(), "call " + (i + 1));
        }
    }

    @Test
    void shouldRotateGenerationIdAfterCapacityCalls() {
        // Behavior: the call after 100 calls returns a new generation id that differs from the initial one.
        ObjectId initial = new ObjectId();
        GenerationCounter genId = new GenerationCounter(initial, CAPACITY);
        for (int i = 0; i < CAPACITY; i++) {
            genId.next();
        }
        assertNotEquals(initial, genId.next());
    }

    @Test
    void shouldRotateGenerationIdOnceOnEachOverflow() {
        // Behavior: the generation id changes exactly once per overflow and never returns to a previous value.
        GenerationCounter genId = new GenerationCounter(new ObjectId(), CAPACITY);
        ObjectId previous = genId.next();
        int rotations = 0;
        for (int i = 0; i < CAPACITY * 5; i++) {
            ObjectId current = genId.next();
            if (!current.equals(previous)) {
                rotations++;
                assertEquals(1, current.compareTo(previous), "rotation at call " + (i + 2) + " must produce a newer id");
            }
            previous = current;
        }
        assertEquals(4, rotations);
    }

    @Test
    void shouldReturnCurrentGenerationIdWithoutCountingTheCall() {
        // Behavior: current() and currentAsBytes() return the id that next() would return, and never advance the counter.
        ObjectId initial = new ObjectId();
        GenerationCounter genId = new GenerationCounter(initial, CAPACITY);
        for (int i = 0; i < CAPACITY * 2; i++) {
            assertEquals(initial, genId.current(), "current call " + (i + 1));
            assertArrayEquals(initial.toByteArray(), genId.currentAsBytes(), "currentAsBytes call " + (i + 1));
        }
        for (int i = 0; i < CAPACITY; i++) {
            assertEquals(initial, genId.next(), "next call " + (i + 1));
        }
        ObjectId rotated = genId.next();
        assertNotEquals(initial, rotated);
        assertEquals(rotated, genId.current());
        assertArrayEquals(rotated.toByteArray(), genId.currentAsBytes());
    }

    @Test
    void shouldRotateOnEveryCallWhenCapacityIsZero() {
        // Behavior: with capacity 0 each call returns a new generation id.
        ObjectId initial = new ObjectId();
        GenerationCounter genId = new GenerationCounter(initial, 0);
        ObjectId first = genId.next();
        ObjectId second = genId.next();
        ObjectId third = genId.next();
        assertNotEquals(initial, first);
        assertNotEquals(first, second);
        assertNotEquals(second, third);
    }
}
