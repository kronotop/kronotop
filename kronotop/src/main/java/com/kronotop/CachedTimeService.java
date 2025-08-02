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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The CachedTimeService class extends the BaseKronotopService and implements the KronotopService
 * interface. This service provides a mechanism to manage and access a cached system time
 * that is periodically updated in the background. The frequent system time updates allow
 * efficient access to the current time with reduced overhead compared to directly calling
 * {@code System.currentTimeMillis()}.
 * <p>
 * The service uses a {@code ScheduledExecutorService} to execute the {@code CachedTime} instance,
 * which periodically refreshes the cached system time.
 * <p>
 * Key features of this service include:
 * - Caching of system time updated on a millisecond interval.
 * - Background thread execution for minimal performance impact.
 * - Graceful shutdown of background threads upon service termination.
 * <p>
 * This service is uniquely identified by the {@code NAME} "CachedTime".
 */
public class CachedTimeService extends BaseKronotopService implements KronotopService {
    public static String NAME = "CachedTime";
    private final CachedTime cachedTime = new CachedTime();
    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1,
            Thread.ofVirtual().name("kr.cached-time-service-", 0L).factory());

    public CachedTimeService(Context context) {
        super(context, NAME);
        executor.scheduleAtFixedRate(cachedTime, 0, 1, TimeUnit.MILLISECONDS);
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
        return cachedTime.getCurrentTimeInMilliseconds();
    }

    @Override
    public void shutdown() {
        executor.shutdownNow();
    }
}
