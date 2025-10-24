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
 * A service that provides cached access to the current system time with millisecond precision.
 * <p>
 * This service maintains a cached timestamp that is updated approximately every millisecond
 * by a low-priority background thread. This approach reduces the overhead of frequent
 * {@code System.currentTimeMillis()} calls while maintaining reasonable time accuracy
 * for applications that can tolerate slight time skew (up to 1ms).
 * <p>
 * The service is particularly useful in high-throughput scenarios where time is frequently
 * accessed but microsecond precision is not required, such as TTL checks, timeout calculations,
 * or timestamp generation for non-critical operations.
 * <p>
 * Thread-safety: This service is thread-safe. The cached time value is stored in a volatile
 * field, ensuring visibility across threads.
 *
 * @see BaseKronotopService
 * @see KronotopService
 */
public class CachedTimeService extends BaseKronotopService implements KronotopService {
    public static String NAME = "CachedTime";
    private final Thread updater;
    private volatile long currentTimeInMilliseconds;
    private volatile boolean shutdown;

    /**
     * Constructs a new CachedTimeService with the specified context.
     * <p>
     * Creates a daemon thread that will continuously update the cached time value
     * at approximately 1ms intervals. The updater thread runs at minimum priority
     * to minimize impact on application performance.
     *
     * @param context the Kronotop context providing access to system resources
     */
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

    /**
     * Starts the cached time service by launching the background updater thread.
     * <p>
     * Once started, the service will begin updating the cached time value approximately
     * every millisecond. This method should be called once during service initialization.
     * <p>
     * Note: The service must be started before {@link #getCurrentTimeInMilliseconds()}
     * will return meaningful values.
     */
    public void start() {
        updater.start();
    }

    /**
     * Returns the cached current time in milliseconds since the Unix epoch.
     * <p>
     * This method returns the most recently cached time value, which is updated
     * approximately every millisecond by the background thread. The returned value
     * may be up to 1ms behind the actual system time.
     * <p>
     * This method is lock-free and has minimal overhead compared to calling
     * {@code System.currentTimeMillis()} directly.
     *
     * @return the cached current time in milliseconds since January 1, 1970 UTC
     */
    public long getCurrentTimeInMilliseconds() {
        return currentTimeInMilliseconds;
    }

    /**
     * Shuts down the cached time service and stops the background updater thread.
     * <p>
     * This method sets the shutdown flag and interrupts the updater thread if it's
     * still running. After shutdown, the service will no longer update the cached
     * time value.
     * <p>
     * This method is idempotent and can be called multiple times safely.
     */
    @Override
    public void shutdown() {
        shutdown = true;
        if (updater != null && updater.isAlive()) {
            updater.interrupt();
        }
    }
}
