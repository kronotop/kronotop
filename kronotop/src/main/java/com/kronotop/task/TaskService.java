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

package com.kronotop.task;

import com.kronotop.BaseKronotopService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.*;

public class TaskService extends BaseKronotopService implements KronotopService {
    public static final String NAME = "Task";
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskService.class);
    private final ScheduledExecutorService scheduler;

    public TaskService(Context context) {
        super(context, NAME);

        ThreadFactory factory = Thread.ofVirtual().name("kr.task-", 0L).factory();
        this.scheduler = new ScheduledThreadPoolExecutor(1, factory);
    }

    public static TimeUnit timeUnitOf(String unit) {
        unit = unit.toUpperCase();
        return switch (unit) {
            case "MILLISECONDS" -> TimeUnit.MILLISECONDS;
            case "SECONDS" -> TimeUnit.SECONDS;
            case "MINUTES" -> TimeUnit.MINUTES;
            case "HOURS" -> TimeUnit.HOURS;
            case "DAYS" -> TimeUnit.DAYS;
            default -> throw new IllegalArgumentException("Unsupported time unit");
        };
    }

    public void execute(Task task) {
        TaskRunner runner = new TaskRunner(task);
        scheduler.execute(runner);
    }

    public ScheduledFuture<?> scheduleAtFixedRate(Task task, long initialDelay, long delay, TimeUnit unit) {
        TaskRunner runner = new TaskRunner(task);
        return scheduler.scheduleAtFixedRate(runner, initialDelay, delay, unit);
    }

    @Override
    public void shutdown() {
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(6, TimeUnit.SECONDS)) {
                LOGGER.warn("{} service cannot be stopped gracefully", NAME);
            }
        } catch (InterruptedException e) {
            LOGGER.warn("Error while shutting down {} service", NAME, e);
        }
    }

    private record TaskRunner(Task task) implements Runnable {

        @Override
        public void run() {
            Thread.
                    ofVirtual().
                    name(String.format("TaskRunner-%d", Instant.now().toEpochMilli())).
                    start(task);
        }
    }
}