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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class GenerationCounterTest {

    private static final int CAPACITY = 100;

    @Test
    void shouldKeepGenerationIdStableForCapacityTakeCalls() {
        // Behavior: the first 100 take() calls all return the initial generation id.
        ObjectId initial = new ObjectId();
        GenerationCounter genId = new GenerationCounter(initial, CAPACITY);
        for (int i = 0; i < CAPACITY; i++) {
            assertArrayEquals(initial.toByteArray(), genId.take(), "take call " + (i + 1));
        }
    }

    @Test
    void shouldRotateGenerationIdOnTakeCallPastCapacity() {
        // Behavior: the take() call that passes the capacity returns a new generation id, and later calls follow it.
        ObjectId initial = new ObjectId();
        GenerationCounter genId = new GenerationCounter(initial, CAPACITY);
        for (int i = 0; i < CAPACITY; i++) {
            genId.take();
        }
        byte[] rotated = genId.take();
        assertNotEquals(initial, new ObjectId(rotated));
        assertArrayEquals(rotated, genId.take());
    }

    @Test
    void shouldResetCounterAfterRotation() {
        // Behavior: rotation resets the counter, so the next rotation needs another 101 take() calls.
        GenerationCounter genId = new GenerationCounter(new ObjectId(), CAPACITY);
        for (int i = 0; i < CAPACITY; i++) {
            genId.take();
        }
        byte[] rotated = genId.take();
        for (int i = 0; i < CAPACITY; i++) {
            assertArrayEquals(rotated, genId.take(), "take call " + (i + 1));
        }
        assertFalse(Arrays.equals(rotated, genId.take()));
    }

    @Test
    void shouldRotateGenerationIdOnceOnEachOverflow() {
        // Behavior: the generation id changes exactly once per overflow and every new id is newer than the previous one.
        ObjectId initial = new ObjectId();
        GenerationCounter genId = new GenerationCounter(initial, CAPACITY);
        ObjectId previous = initial;
        for (int round = 0; round < 5; round++) {
            for (int i = 0; i < CAPACITY; i++) {
                assertEquals(previous, new ObjectId(genId.take()), "round " + round + " call " + (i + 1));
            }
            ObjectId current = new ObjectId(genId.take());
            assertNotEquals(previous, current, "round " + round);
            assertEquals(1, current.compareTo(previous), "round " + round + " must produce a newer id");
            previous = current;
        }
    }

    @Test
    void shouldRotateOnEveryTakeCallWhenCapacityIsZero() {
        // Behavior: with capacity 0 every take() call returns a new generation id.
        ObjectId initial = new ObjectId();
        GenerationCounter genId = new GenerationCounter(initial, 0);
        ObjectId first = new ObjectId(genId.take());
        ObjectId second = new ObjectId(genId.take());
        ObjectId third = new ObjectId(genId.take());
        assertNotEquals(initial, first);
        assertNotEquals(first, second);
        assertNotEquals(second, third);
    }

    @Test
    void shouldRotateOnceWhenConcurrentTakeCallsPassCapacity() throws Exception {
        // Behavior: concurrent take() calls past the capacity produce at most one new generation id per overflow,
        // never one id per racing thread.
        GenerationCounter genId = new GenerationCounter(new ObjectId(), CAPACITY);
        int threads = 8;
        int callsPerThread = 1_000;
        Set<ObjectId> seen = ConcurrentHashMap.newKeySet();
        try (ExecutorService executor = Executors.newFixedThreadPool(threads)) {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    for (int i = 0; i < callsPerThread; i++) {
                        seen.add(new ObjectId(genId.take()));
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get();
            }
        }
        int maxIds = (threads * callsPerThread) / (CAPACITY + 1) + 2;
        assertTrue(seen.size() <= maxIds, "seen " + seen.size() + " ids, max " + maxIds);
        assertTrue(seen.size() > 1);
    }
}
