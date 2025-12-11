/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.volume.replication;

import java.util.concurrent.atomic.AtomicInteger;

public class MockReplicationTask implements ReplicationTask {
    private final AtomicInteger startCallCount = new AtomicInteger(0);
    private final AtomicInteger shutdownCallCount = new AtomicInteger(0);
    private volatile boolean alwaysFail = false;
    private volatile RuntimeException exceptionToThrow = null;

    @Override
    public void start() {
        startCallCount.incrementAndGet();
        if (alwaysFail && exceptionToThrow != null) {
            throw exceptionToThrow;
        }
    }

    @Override
    public void reconnect() {
    }

    @Override
    public void shutdown() {
        shutdownCallCount.incrementAndGet();
    }

    public void setAlwaysFail(boolean alwaysFail, RuntimeException exception) {
        this.alwaysFail = alwaysFail;
        this.exceptionToThrow = exception;
    }

    public int getStartCallCount() {
        return startCallCount.get();
    }

    public int getShutdownCallCount() {
        return shutdownCallCount.get();
    }
}
