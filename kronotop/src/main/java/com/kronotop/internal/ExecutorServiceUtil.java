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

package com.kronotop.internal;

import com.kronotop.KronotopException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for gracefully shutting down {@link ExecutorService} instances.
 */
public class ExecutorServiceUtil {
    /**
     * Default timeout value used for awaiting executor termination.
     */
    public static final long DEFAULT_TIMEOUT = 10;

    /**
     * Default time unit for the termination timeout.
     */
    public static final TimeUnit DEFAULT_TIMEOUT_TIMEUNIT = TimeUnit.SECONDS;

    /**
     * Immediately initiates shutdown and waits for termination.
     *
     * <p>Calls {@link ExecutorService#shutdownNow()} to cancel running tasks and
     * reject new submissions, then blocks until termination completes or the
     * default timeout (10 seconds) expires.
     *
     * @param executor the executor service to shut down; if null or already terminated, returns true immediately
     * @return true if the executor terminated within the timeout, false otherwise
     * @throws KronotopException if the waiting thread is interrupted
     */
    public static boolean shutdownNowThenAwaitTermination(ExecutorService executor) {
        if (executor == null || executor.isTerminated()) {
            return true;
        }

        executor.shutdownNow();
        try {
            return executor.awaitTermination(DEFAULT_TIMEOUT, DEFAULT_TIMEOUT_TIMEUNIT);
        } catch (InterruptedException exp) {
            Thread.currentThread().interrupt();
            throw new KronotopException(exp);
        }
    }
}
