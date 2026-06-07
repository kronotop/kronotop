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

import com.kronotop.BaseClusterTest;
import com.kronotop.instance.KronotopInstance;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class TaskServiceTest extends BaseClusterTest {

    @Test
    void shouldExecuteMultipleTasksWithDifferentNames() throws InterruptedException {
        // Behavior: Multiple tasks with unique names can be executed concurrently.
        KronotopInstance instance = getInstances().getFirst();
        TaskService service = instance.getContext().getService(TaskService.NAME);

        int NUMBER_OF_TASKS = 10;

        CountDownLatch latch = new CountDownLatch(NUMBER_OF_TASKS);
        AtomicInteger counter = new AtomicInteger();

        for (int i = 0; i < NUMBER_OF_TASKS; i++) {
            final String taskName = "TestTask-" + i;
            class TestTask extends BaseTask implements Task {
                @Override
                public void task() {
                    counter.incrementAndGet();
                    latch.countDown();
                }

                @Override
                public String name() {
                    return taskName;
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
            service.execute(new TestTask());
        }

        latch.await();
        assertEquals(NUMBER_OF_TASKS, counter.get());
    }

    @Test
    void shouldRejectDuplicateTaskExecution() {
        // Behavior: execute() throws TaskAlreadyExistsException when a task with the same name is already registered and not completed.
        KronotopInstance instance = getInstances().getFirst();
        TaskService service = instance.getContext().getService(TaskService.NAME);

        class TestTask extends BaseTask implements Task {
            private final CountDownLatch latch = new CountDownLatch(1);

            @Override
            public void task() {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public String name() {
                return "DuplicateTest";
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
                latch.countDown();
            }
        }

        TestTask first = new TestTask();
        service.execute(first);

        TestTask second = new TestTask();
        assertThrows(TaskAlreadyExistsException.class, () -> service.execute(second));

        // Cleanup
        service.shutdownAndRemoveTask("DuplicateTest");
    }

    @Test
    void shouldAllowReplacingCompletedTask() {
        // Behavior: execute() allows registering a new task when the existing task with the same name has completed.
        KronotopInstance instance = getInstances().getFirst();
        TaskService service = instance.getContext().getService(TaskService.NAME);

        AtomicInteger counter = new AtomicInteger();

        class TestTask extends BaseTask implements Task {
            private volatile boolean finished = false;

            @Override
            public void task() {
                counter.incrementAndGet();
                finished = true;
            }

            @Override
            public String name() {
                return "CompletedReplaceTest";
            }

            @Override
            public boolean isFinished() {
                return finished;
            }

            @Override
            public void cleanupMetadata() {
            }

            @Override
            public void shutdown() {
            }
        }

        service.execute(new TestTask());

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertTrue(service.getTask("CompletedReplaceTest").isFinished())
        );

        // Should succeed because the existing task is completed
        service.execute(new TestTask());

        await().atMost(Duration.ofSeconds(5)).until(() -> counter.get() >= 2);
    }

    @Test
    void shouldTrackTaskStats() {
        KronotopInstance instance = getInstances().getFirst();
        TaskService service = instance.getContext().getService(TaskService.NAME);

        String name = "TestTask";
        class TestTask extends BaseTask implements Task {

            @Override
            public void task() {
                // nothing to do
            }

            @Override
            public String name() {
                return name;
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
                // nothing to do
            }
        }

        service.scheduleAtFixedRate(new TestTask(), 0, 1, TimeUnit.MILLISECONDS);
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            List<ObservedTask> tasks = service.tasks();
            for (ObservedTask task : tasks) {
                if (task.name().equals(name)) {
                    return task.lastRun() != 0;
                }
            }
            return false;
        });
    }
}