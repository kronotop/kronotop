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

/**
 * The CachedTime class provides a mechanism to maintain a frequently updated snapshot
 * of the current system time in milliseconds.
 * <p>
 * This class is designed to minimize the overhead of retrieving the system time by
 * caching it and periodically(every millisecond) refreshing the cached value using a background thread.
 * It implements the Runnable interface to allow periodic updates to the cached time.
 */
public class CachedTime implements Runnable {
    private volatile long currentTimeInMilliseconds;

    public CachedTime() {
        this.currentTimeInMilliseconds = System.currentTimeMillis();
    }

    /**
     * Retrieves the current cached system time in milliseconds.
     * This method returns a snapshot of the time that is periodically updated
     * by a background thread, minimizing the overhead of frequently calling
     * {@code System.currentTimeMillis()}.
     *
     * @return the current cached system time in milliseconds.
     */
    public long getCurrentTimeInMilliseconds() {
        return currentTimeInMilliseconds;
    }

    @Override
    public void run() {
        currentTimeInMilliseconds = System.currentTimeMillis();
    }
}
