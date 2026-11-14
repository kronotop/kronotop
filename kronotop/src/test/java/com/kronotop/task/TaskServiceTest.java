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
    void shouldRemoveTaskFromRegistryWhenTaskCompletes() {
        // Behavior: A task that never reports isFinished() is executed and blocks, so it stays in the
        // registry. When it is released, it calls onTaskCompleted(this) and getTask() for its name starts
        // to throw TaskNotFoundException.
        KronotopInstance instance = getInstances().getFirst();
        TaskService service = instance.getContext().getService(TaskService.NAME);

        SelfCompletingTask task = new SelfCompletingTask(service, "SelfCompletingTest");
        service.execute(task);
        assertSame(task, service.getTask("SelfCompletingTest"));

        task.finish();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThrows(TaskNotFoundException.class, () -> service.getTask("SelfCompletingTest"))
        );
    }

    @Test
    void shouldKeepRegisteredTaskWhenAnotherInstanceWithSameNameCompletes() {
        // Behavior: A task is executed and blocks, so it stays in the registry. onTaskCompleted() is called
        // with a second task instance that has the same name but was never registered. getTask() still
        // returns the first instance.
        KronotopInstance instance = getInstances().getFirst();
        TaskService service = instance.getContext().getService(TaskService.NAME);

        SelfCompletingTask registered = new SelfCompletingTask(service, "SameNameTest");
        service.execute(registered);

        service.onTaskCompleted(new SelfCompletingTask(service, "SameNameTest"));

        assertSame(registered, service.getTask("SameNameTest"));

        // Cleanup
        service.shutdownAndRemoveTask("SameNameTest");
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

    private static class SelfCompletingTask extends BaseTask {
        private final TaskService service;
        private final String name;
        private final CountDownLatch release = new CountDownLatch(1);

        SelfCompletingTask(TaskService service, String name) {
            this.service = service;
            this.name = name;
        }

        void finish() {
            release.countDown();
        }

        @Override
        public void task() {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            service.onTaskCompleted(this);
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
            finish();
        }
    }
}