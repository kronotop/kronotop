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

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

/**
 * Utility class for creating and configuring custom ExecutorService instances optimized for Kronotop's workload patterns.
 * <p>
 * This class provides factory methods for creating bounded thread pool executors with specific characteristics:
 * <ul>
 *   <li>Dynamic thread scaling from 0 to a maximum number of threads</li>
 *   <li>LinkedBlockingQueue for unbounded task queuing with efficient buffering</li>
 *   <li>Core thread timeout to reclaim resources during idle periods</li>
 *   <li>Customizable thread naming for debugging and monitoring</li>
 * </ul>
 * <p>
 * All executors created by this class use a bounded thread pool strategy with unbounded task queuing,
 * preventing unbounded thread creation while allowing task buffering during peak loads. This ensures
 * efficient resource utilization through thread recycling and timeout mechanisms.
 *
 * @see ExecutorService
 * @see ThreadPoolExecutor
 */
public final class KrExecutors {

    private KrExecutors() {
    }

    /**
     * Creates a bounded executor service with customizable thread pool parameters.
     * <p>
     * This executor uses a {@link LinkedBlockingQueue} for task queuing, allowing tasks to be buffered
     * when all threads are busy. The executor scales dynamically from 0 to {@code maxThreads}, creating
     * new threads on demand up to the maximum limit. Once the maximum thread count is reached, additional
     * tasks are queued until threads become available.
     * <p>
     * Core thread timeout is enabled, allowing all threads (including core threads) to terminate after
     * being idle for the specified {@code keepAliveTime}. This ensures resource cleanup during low-activity periods.
     * <p>
     * The unbounded queue prevents task rejection under load, making this executor suitable for scenarios
     * where task buffering is preferred over immediate task rejection or caller-runs policies.
     *
     * @param maxThreads the maximum number of threads to allow in the pool. Must be greater than 0.
     * @param keepAliveTime the time limit for which idle threads may remain alive before being terminated.
     * @param timeUnit the time unit for the {@code keepAliveTime} parameter.
     * @param factory the factory to use when creating new threads. Typically used to set thread names and daemon status.
     * @return a new bounded {@link ExecutorService} configured with the specified parameters.
     * @throws IllegalArgumentException if {@code maxThreads} is less than or equal to 0.
     * @throws NullPointerException if {@code timeUnit} or {@code factory} is null.
     * @see ThreadPoolExecutor
     * @see LinkedBlockingQueue
     */
    public static ExecutorService newBoundedExecutor(
            int maxThreads,
            long keepAliveTime,
            TimeUnit timeUnit,
            ThreadFactory factory
    ) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                0,
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
     * Creates a bounded executor service with a default configuration suitable for general-purpose CPU-bound tasks.
     * <p>
     * This convenience method creates an executor with the following default settings:
     * <ul>
     *   <li>Maximum threads: Number of available processors ({@code Runtime.getRuntime().availableProcessors()})</li>
     *   <li>Keep-alive time: 1 minute</li>
     *   <li>Thread naming format: "kr-worker-%d" where %d is the thread sequence number</li>
     * </ul>
     * <p>
     * The executor is optimized for CPU-bound workloads where the thread count should match available processing cores.
     * For I/O-bound or virtual thread workloads, consider using custom parameters via
     * {@link #newBoundedExecutor(int, long, TimeUnit, ThreadFactory)}.
     *
     * @return a new bounded {@link ExecutorService} with default configuration.
     * @see #newBoundedExecutor(int, long, TimeUnit, ThreadFactory)
     */
    public static ExecutorService newBoundedExecutor() {
        long keepAliveTime = 1L;
        int maxThreads = Runtime.getRuntime().availableProcessors();
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("kr-worker-%d").build();
        return newBoundedExecutor(maxThreads, keepAliveTime, TimeUnit.MINUTES, factory);
    }
}
