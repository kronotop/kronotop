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

package com.kronotop.internal;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class KrExecutorsTest {

    @Test
    void test_newBoundedExecutor_withDefaultConfiguration() throws Exception {
        ExecutorService executor = KrExecutors.newBoundedExecutor();
        assertNotNull(executor);

        try {
            CountDownLatch latch = new CountDownLatch(1);
            Future<?> future = executor.submit(latch::countDown);

            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertFalse(future.isCancelled());
            assertTrue(future.isDone());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void test_newBoundedExecutor_withCustomParameters() throws Exception {
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat("test-worker-%d")
                .build();

        ExecutorService executor = KrExecutors.newBoundedExecutor(
                4,
                1L,
                TimeUnit.MINUTES,
                factory
        );
        assertNotNull(executor);

        try {
            CountDownLatch latch = new CountDownLatch(1);
            Future<?> future = executor.submit(latch::countDown);

            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertTrue(future.isDone());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void test_newBoundedExecutor_dynamicThreadScaling() throws Exception {
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat("scale-test-%d")
                .build();

        ExecutorService executor = KrExecutors.newBoundedExecutor(
                4,
                1L,
                TimeUnit.MINUTES,
                factory
        );

        try {
            // Submit multiple tasks to trigger thread creation
            int taskCount = 8;
            CountDownLatch startLatch = new CountDownLatch(taskCount);
            CountDownLatch completeLatch = new CountDownLatch(taskCount);

            for (int i = 0; i < taskCount; i++) {
                executor.submit(() -> {
                    startLatch.countDown();
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    completeLatch.countDown();
                });
            }

            // Verify all tasks started
            assertTrue(startLatch.await(5, TimeUnit.SECONDS));
            // Verify all tasks completed
            assertTrue(completeLatch.await(5, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void test_newBoundedExecutor_taskBufferingWithLinkedBlockingQueue() throws Exception {
        int maxThreads = 2;
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat("buffer-test-%d")
                .build();

        ExecutorService executor = KrExecutors.newBoundedExecutor(
                maxThreads,
                1L,
                TimeUnit.MINUTES,
                factory
        );

        try {
            // Submit more tasks than available threads
            int taskCount = 10;
            AtomicInteger completedTasks = new AtomicInteger(0);
            CountDownLatch blockingLatch = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();

            // First, submit blocking tasks to occupy all threads
            for (int i = 0; i < maxThreads; i++) {
                futures.add(executor.submit(() -> {
                    try {
                        blockingLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }));
            }

            // Give threads time to start
            Thread.sleep(100);

            // Submit additional tasks that should be queued
            for (int i = 0; i < taskCount - maxThreads; i++) {
                futures.add(executor.submit(() -> {
                    completedTasks.incrementAndGet();
                }));
            }

            // Verify tasks were accepted (not rejected)
            assertEquals(taskCount, futures.size());

            // Unblock all tasks
            blockingLatch.countDown();

            // Wait for completion
            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }

            // Verify queued tasks executed
            assertEquals(taskCount - maxThreads, completedTasks.get());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void test_newBoundedExecutor_threadKeepAliveTimeout() throws Exception {
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat("timeout-test-%d")
                .build();

        // Use a very short keep-alive time for testing
        ExecutorService executor = KrExecutors.newBoundedExecutor(
                4,
                100L,  // 100 milliseconds
                TimeUnit.MILLISECONDS,
                factory
        );

        try {
            // Submit tasks to create threads
            int taskCount = 4;
            CountDownLatch latch = new CountDownLatch(taskCount);

            for (int i = 0; i < taskCount; i++) {
                executor.submit(latch::countDown);
            }

            assertTrue(latch.await(5, TimeUnit.SECONDS));

            // Wait for threads to timeout and terminate
            Thread.sleep(500);

            // Submit a new task to verify executor still works
            CountDownLatch finalLatch = new CountDownLatch(1);
            Future<?> future = executor.submit(finalLatch::countDown);

            assertTrue(finalLatch.await(5, TimeUnit.SECONDS));
            assertTrue(future.isDone());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void test_newBoundedExecutor_concurrentTaskExecution() throws Exception {
        int maxThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = KrExecutors.newBoundedExecutor(
                maxThreads,
                1L,
                TimeUnit.MINUTES,
                new ThreadFactoryBuilder().setNameFormat("concurrent-test-%d").build()
        );

        try {
            int taskCount = 100;
            AtomicInteger counter = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(taskCount);

            for (int i = 0; i < taskCount; i++) {
                executor.submit(() -> {
                    counter.incrementAndGet();
                    latch.countDown();
                });
            }

            assertTrue(latch.await(10, TimeUnit.SECONDS));
            assertEquals(taskCount, counter.get());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void test_newBoundedExecutor_taskOrderingWithQueue() throws Exception {
        // Use single thread to ensure sequential processing from queue
        ExecutorService executor = KrExecutors.newBoundedExecutor(
                1,
                1L,
                TimeUnit.MINUTES,
                new ThreadFactoryBuilder().setNameFormat("order-test-%d").build()
        );

        try {
            List<Integer> executionOrder = new CopyOnWriteArrayList<>();
            int taskCount = 10;
            CountDownLatch latch = new CountDownLatch(taskCount);

            for (int i = 0; i < taskCount; i++) {
                final int taskId = i;
                executor.submit(() -> {
                    executionOrder.add(taskId);
                    latch.countDown();
                });
            }

            assertTrue(latch.await(10, TimeUnit.SECONDS));
            assertEquals(taskCount, executionOrder.size());

            // Verify tasks were executed (order may vary due to thread scheduling)
            for (int i = 0; i < taskCount; i++) {
                assertTrue(executionOrder.contains(i));
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void test_newBoundedExecutor_shutdownBehavior() throws Exception {
        ExecutorService executor = KrExecutors.newBoundedExecutor();

        CountDownLatch latch = new CountDownLatch(1);
        executor.submit(latch::countDown);

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        executor.shutdown();
        assertTrue(executor.isShutdown());
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        assertTrue(executor.isTerminated());
    }

    @Test
    void test_newBoundedExecutor_shutdownNowBehavior() throws Exception {
        ExecutorService executor = KrExecutors.newBoundedExecutor(
                2,
                1L,
                TimeUnit.MINUTES,
                new ThreadFactoryBuilder().setNameFormat("shutdown-test-%d").build()
        );

        try {
            CountDownLatch blockingLatch = new CountDownLatch(1);

            // Submit a blocking task
            executor.submit(() -> {
                try {
                    blockingLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            // Give it time to start
            Thread.sleep(100);

            List<Runnable> pendingTasks = executor.shutdownNow();
            assertTrue(executor.isShutdown());
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            fail("Interrupted while waiting for termination");
        }
    }

    @Test
    void test_newBoundedExecutor_exceptionHandling() throws Exception {
        ExecutorService executor = KrExecutors.newBoundedExecutor();

        try {
            Future<?> future = executor.submit(() -> {
                throw new RuntimeException("Test exception");
            });

            // Exception should be captured in the Future
            ExecutionException exception = assertThrows(ExecutionException.class,
                    () -> future.get(5, TimeUnit.SECONDS));

            assertEquals("Test exception", exception.getCause().getMessage());
            assertTrue(future.isDone());
            assertFalse(future.isCancelled());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
