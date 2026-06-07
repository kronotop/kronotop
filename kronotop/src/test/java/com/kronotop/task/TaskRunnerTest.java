/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.task;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskRunnerTest {

    @Test
    void shouldPreventOverlappingExecutions() throws InterruptedException {
        // Behavior: When a TaskRunner is invoked while a previous execution is still running,
        // the second invocation is skipped via the AtomicBoolean guard.

        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch blockingLatch = new CountDownLatch(1);
        AtomicInteger executionCount = new AtomicInteger();

        class SlowTask extends BaseTask implements Task {
            @Override
            public void task() {
                executionCount.incrementAndGet();
                taskStarted.countDown();
                try {
                    blockingLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public String name() {
                return "SlowTask";
            }

            @Override
            public boolean isFinished() {
                return false;
            }

            @Override
            public void cleanupMetadata() {
            }

            @Override
            public void shutdown() {
            }
        }

        TaskService.TaskRunner runner = new TaskService.TaskRunner(new SlowTask());

        // First invocation — spawns a virtual thread
        runner.run();
        assertTrue(taskStarted.await(5, TimeUnit.SECONDS), "Task should have started");

        // Second invocation — should be skipped because the first is still running
        runner.run();

        // Release the blocking task
        blockingLatch.countDown();

        // Give the virtual thread time to finish
        Thread.sleep(100);

        assertEquals(1, executionCount.get(), "Second invocation should have been skipped");
    }

    @Test
    void shouldAllowNextExecutionAfterPreviousCompletes() throws InterruptedException {
        // Behavior: After a previous execution completes and the guard is released,
        // the next invocation of TaskRunner should execute normally.

        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch firstBlock = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch secondBlock = new CountDownLatch(1);
        AtomicInteger executionCount = new AtomicInteger();

        class SlowTask extends BaseTask implements Task {
            @Override
            public void task() {
                int count = executionCount.incrementAndGet();
                if (count == 1) {
                    firstStarted.countDown();
                    try {
                        firstBlock.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    secondStarted.countDown();
                    try {
                        secondBlock.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            @Override
            public String name() {
                return "SlowTask";
            }

            @Override
            public boolean isFinished() {
                return false;
            }

            @Override
            public void cleanupMetadata() {
            }

            @Override
            public void shutdown() {
            }
        }

        TaskService.TaskRunner runner = new TaskService.TaskRunner(new SlowTask());

        // First execution
        runner.run();
        assertTrue(firstStarted.await(5, TimeUnit.SECONDS), "First execution should have started");
        firstBlock.countDown();

        // Wait for the virtual thread to complete and release the guard
        Thread.sleep(100);

        // The second execution — should succeed since the first one completed
        runner.run();
        assertTrue(secondStarted.await(5, TimeUnit.SECONDS), "Second execution should have started");
        secondBlock.countDown();

        // Wait for the virtual thread to complete
        Thread.sleep(100);

        assertEquals(2, executionCount.get(), "Both executions should have run");
    }
}
