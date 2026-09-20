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

import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tracks operations that shutdown must wait for. An operation calls {@link #enter()} before it starts and
 * {@link #exit()} in a finally block when it ends. Shutdown calls {@link #awaitCompletion(long, TimeUnit)} once
 * and blocks until every entered operation has exited or the timeout passes. Operations are never interrupted
 * or cancelled.
 */
public class InFlight {
    private final Phaser phaser = new Phaser(1);
    private volatile boolean awaiting;

    /**
     * Marks the start of an operation.
     *
     * @throws IllegalStateException if shutdown has already completed
     */
    public void enter() {
        if (phaser.register() < 0) {
            throw new IllegalStateException("Cannot enter a new operation, Kronotop is shutting down");
        }
    }

    /**
     * Marks the end of an operation. Call it in a finally block after {@link #enter()}.
     */
    public void exit() {
        phaser.arriveAndDeregister();
    }

    /**
     * Returns the number of operations that entered and have not exited yet.
     */
    public int count() {
        int parties = phaser.getRegisteredParties();
        return awaiting ? parties : parties - 1;
    }

    /**
     * Blocks until every entered operation has exited or the timeout passes. Returns at once when nothing
     * is in flight. After it returns normally, {@link #enter()} is rejected.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the timeout argument
     * @throws InterruptedException if the calling thread is interrupted while waiting
     * @throws TimeoutException     if operations are still in flight when the timeout passes
     */
    public void awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        awaiting = true;
        int phase = phaser.arriveAndDeregister();
        phaser.awaitAdvanceInterruptibly(phase, timeout, unit);
    }
}
