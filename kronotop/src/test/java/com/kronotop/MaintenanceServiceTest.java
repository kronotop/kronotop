/*
 * Copyright (c) 2023-2024 Kronotop
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

import com.kronotop.instance.KronotopInstance;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MaintenanceServiceTest extends BaseClusterTest {

    @Test
    public void when_many_tasks_exists() throws InterruptedException {
        KronotopInstance instance = getInstances().getFirst();
        MaintenanceService service = instance.getContext().getService(MaintenanceService.NAME);

        int NUMBER_OF_TASKS = 10;

        CountDownLatch latch = new CountDownLatch(NUMBER_OF_TASKS);
        AtomicInteger counter = new AtomicInteger();

        class TestTask implements Runnable {
            @Override
            public void run() {
                counter.incrementAndGet();
                latch.countDown();
            }
        }

        for (int i = 0; i < NUMBER_OF_TASKS; i++) {
            service.execute(new TestTask());
        }

        latch.await();
        assertEquals(NUMBER_OF_TASKS, counter.get());
    }
}