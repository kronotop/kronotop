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

package com.kronotop;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class InFlightTest {

    @Test
    void shouldCountZeroWhenNoOperation() {
        // Behavior: A new InFlight has no operations in flight.
        InFlight inFlight = new InFlight();
        assertEquals(0, inFlight.count());
    }

    @Test
    void shouldCountEnteredOperations() {
        // Behavior: count() grows with enter() and shrinks with exit().
        InFlight inFlight = new InFlight();
        inFlight.enter();
        inFlight.enter();
        assertEquals(2, inFlight.count());
        inFlight.exit();
        assertEquals(1, inFlight.count());
        inFlight.exit();
        assertEquals(0, inFlight.count());
    }

    @Test
    void shouldReturnImmediatelyWhenNoOperationInFlight() {
        // Behavior: awaitCompletion() returns without blocking when nothing is in flight.
        InFlight inFlight = new InFlight();
        assertTimeoutPreemptively(Duration.ofSeconds(5), inFlight::awaitCompletion);
    }

    @Test
    void shouldWaitUntilAllOperationsExit() throws InterruptedException {
        // Behavior: awaitCompletion() blocks until every entered operation calls exit().
        InFlight inFlight = new InFlight();
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean completed = new AtomicBoolean(false);

        inFlight.enter();
        Thread worker = new Thread(() -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                inFlight.exit();
            }
        });
        worker.start();

        Thread waiter = new Thread(() -> {
            try {
                inFlight.awaitCompletion();
                completed.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        waiter.start();

        waiter.join(200);
        assertFalse(completed.get());

        release.countDown();
        waiter.join(TimeUnit.SECONDS.toMillis(5));
        worker.join(TimeUnit.SECONDS.toMillis(5));
        assertTrue(completed.get());
    }

    @Test
    void shouldRejectEnterAfterCompletion() throws InterruptedException {
        // Behavior: enter() throws IllegalStateException once awaitCompletion() has finished.
        InFlight inFlight = new InFlight();
        inFlight.awaitCompletion();
        assertThrows(IllegalStateException.class, inFlight::enter);
    }

    @Test
    void shouldInterruptAwaitCompletion() throws InterruptedException {
        // Behavior: A thread blocked in awaitCompletion() gets InterruptedException when interrupted.
        InFlight inFlight = new InFlight();
        inFlight.enter();
        AtomicReference<Throwable> thrown = new AtomicReference<>();

        Thread waiter = new Thread(() -> {
            try {
                inFlight.awaitCompletion();
            } catch (InterruptedException e) {
                thrown.set(e);
            }
        });
        waiter.start();
        await().atMost(5, TimeUnit.SECONDS).until(() -> waiter.getState() == Thread.State.WAITING);

        waiter.interrupt();
        waiter.join(TimeUnit.SECONDS.toMillis(5));
        assertInstanceOf(InterruptedException.class, thrown.get());

        inFlight.exit();
    }
}
