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
    void shouldNotRotateWhenOnlyNextIsCalled() {
        // Behavior: next() does not count calls, so repeated next() calls never rotate the generation id.
        ObjectId initial = new ObjectId();
        GenerationCounter genId = new GenerationCounter(initial, CAPACITY);
        for (int i = 0; i < CAPACITY * 2; i++) {
            assertEquals(initial, genId.next(), "next call " + (i + 1));
        }
    }

    @Test
    void shouldKeepGenerationIdStableForCapacityTakeCalls() {
        // Behavior: take() counts calls; after exactly 100 take() calls, next() still returns the initial id.
        ObjectId initial = new ObjectId();
        GenerationCounter genId = new GenerationCounter(initial, CAPACITY);
        for (int i = 0; i < CAPACITY; i++) {
            assertArrayEquals(initial.toByteArray(), genId.take(), "take call " + (i + 1));
        }
        assertEquals(initial, genId.next());
    }

    @Test
    void shouldRotateGenerationIdAfterCapacityTakeCalls() {
        // Behavior: once take() has been called more than 100 times, next() returns a new id and take() follows it.
        ObjectId initial = new ObjectId();
        GenerationCounter genId = new GenerationCounter(initial, CAPACITY);
        for (int i = 0; i < CAPACITY + 1; i++) {
            genId.take();
        }
        ObjectId rotated = genId.next();
        assertNotEquals(initial, rotated);
        assertArrayEquals(rotated.toByteArray(), genId.take());
    }

    @Test
    void shouldResetCounterAfterRotation() {
        // Behavior: rotation resets the counter, so the next overflow needs another 101 take() calls.
        ObjectId initial = new ObjectId();
        GenerationCounter genId = new GenerationCounter(initial, CAPACITY);
        for (int i = 0; i < CAPACITY + 1; i++) {
            genId.take();
        }
        ObjectId rotated = genId.next();
        assertNotEquals(initial, rotated);
        assertEquals(rotated, genId.next());
        for (int i = 0; i < CAPACITY; i++) {
            genId.take();
        }
        assertEquals(rotated, genId.next());
        genId.take();
        assertNotEquals(rotated, genId.next());
    }

    @Test
    void shouldRotateGenerationIdOnceOnEachOverflow() {
        // Behavior: the generation id changes exactly once per overflow and every new id is newer than the previous one.
        GenerationCounter genId = new GenerationCounter(new ObjectId(), CAPACITY);
        ObjectId previous = genId.next();
        for (int round = 0; round < 5; round++) {
            for (int i = 0; i < CAPACITY; i++) {
                genId.take();
                assertEquals(previous, genId.next(), "round " + round + " call " + (i + 1));
            }
            genId.take();
            ObjectId current = genId.next();
            assertNotEquals(previous, current, "round " + round);
            assertEquals(1, current.compareTo(previous), "round " + round + " must produce a newer id");
            previous = current;
        }
    }

    @Test
    void shouldRotateAfterEveryTakeCallWhenCapacityIsZero() {
        // Behavior: with capacity 0 a single take() call makes the following next() return a new generation id.
        ObjectId initial = new ObjectId();
        GenerationCounter genId = new GenerationCounter(initial, 0);
        genId.take();
        ObjectId first = genId.next();
        genId.take();
        ObjectId second = genId.next();
        genId.take();
        ObjectId third = genId.next();
        assertNotEquals(initial, first);
        assertNotEquals(first, second);
        assertNotEquals(second, third);
    }
}
