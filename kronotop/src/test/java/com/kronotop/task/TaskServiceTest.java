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
import static org.junit.jupiter.api.Assertions.assertEquals;

class TaskServiceTest extends BaseClusterTest {

    @Test
    void when_many_tasks_exists() throws InterruptedException {
        KronotopInstance instance = getInstances().getFirst();
        TaskService service = instance.getContext().getService(TaskService.NAME);

        int NUMBER_OF_TASKS = 10;

        CountDownLatch latch = new CountDownLatch(NUMBER_OF_TASKS);
        AtomicInteger counter = new AtomicInteger();

        class TestTask extends BaseTask implements Task {
            @Override
            public void task() {
                counter.incrementAndGet();
                latch.countDown();
            }

            @Override
            public String name() {
                return "TestTask";
            }

            @Override
            public boolean isCompleted() {
                return false;
            }

            @Override
            public void complete() {
            }

            @Override
            public void shutdown() {
            }
        }

        for (int i = 0; i < NUMBER_OF_TASKS; i++) {
            service.execute(new TestTask());
        }

        latch.await();
        assertEquals(NUMBER_OF_TASKS, counter.get());
    }

    @Test
    void check_task_stats() {
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
            public boolean isCompleted() {
                return false;
            }

            @Override
            public void complete() {
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