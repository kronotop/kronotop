/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop;

import java.util.concurrent.locks.LockSupport;

/**
 * The CachedTimeService class is a service responsible for maintaining and providing
 * a periodically updated snapshot of the current system time in milliseconds.
 * The service uses a background thread to keep a cached copy of the current system time
 * refreshed at regular intervals (every millisecond).
 */
public class CachedTimeService extends BaseKronotopService implements KronotopService {
    public static String NAME = "CachedTime";
    private final Thread updater;
    private volatile long currentTimeInMilliseconds;
    private volatile boolean shutdown;

    public CachedTimeService(Context context) {
        super(context, NAME);
        updater = new Thread(() -> {
            Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
            while (!shutdown) {
                update();
                LockSupport.parkNanos(1_000_000); // 1ms
            }
        });
        updater.setDaemon(true);
        updater.setName("kr-cached-time-service");
    }

    private void update() {
        currentTimeInMilliseconds = System.currentTimeMillis();
    }

    public void start() {
        updater.start();
    }

    /**
     * Retrieves the current cached system time in milliseconds.
     * This method delegates the call to the {@code CachedTime} instance
     * to return a snapshot of the time that is periodically updated
     * by a background thread.
     *
     * @return the current cached system time in milliseconds.
     */
    public long getCurrentTimeInMilliseconds() {
        return currentTimeInMilliseconds;
    }

    @Override
    public void shutdown() {
        shutdown = true;
        if (updater != null && updater.isAlive()) {
            updater.interrupt();
        }
    }
}
