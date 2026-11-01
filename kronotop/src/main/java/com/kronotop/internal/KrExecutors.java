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

package com.kronotop.internal;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

/**
 * Creates executors with a fixed number of threads and an unbounded task queue.
 */
public final class KrExecutors {

    private KrExecutors() {
    }

    /**
     * Returns a new executor that runs up to {@code maxThreads} tasks at once, queues the rest, and stops any
     * thread that stays idle for {@code keepAliveTime}.
     *
     * @param maxThreads    number of threads in the pool, must be greater than 0
     * @param keepAliveTime how long an idle thread waits for a task before it stops, must be greater than 0
     * @param timeUnit      unit of {@code keepAliveTime}
     * @param factory       creates the pool threads
     * @return the new executor
     * @throws IllegalArgumentException if {@code maxThreads} or {@code keepAliveTime} is 0 or less
     * @throws NullPointerException     if {@code timeUnit} or {@code factory} is null
     */
    public static ExecutorService newBoundedExecutor(
            int maxThreads,
            long keepAliveTime,
            TimeUnit timeUnit,
            ThreadFactory factory
    ) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                maxThreads,
                maxThreads,
                keepAliveTime,
                timeUnit,
                new LinkedBlockingQueue<>(),
                factory
        );
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /**
     * Returns a new executor with one thread per available processor, a one minute keep-alive time, and threads
     * named {@code kr-worker-N}.
     *
     * @return the new executor
     */
    public static ExecutorService newBoundedExecutor() {
        long keepAliveTime = 1L;
        int maxThreads = Runtime.getRuntime().availableProcessors();
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("kr-worker-%d").build();
        return newBoundedExecutor(maxThreads, keepAliveTime, TimeUnit.MINUTES, factory);
    }
}
