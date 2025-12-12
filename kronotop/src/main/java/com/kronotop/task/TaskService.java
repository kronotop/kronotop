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

import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.KronotopService;
import com.kronotop.internal.ExecutorServiceUtil;
import com.kronotop.server.ServerKind;
import com.kronotop.task.handlers.TaskAdminHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class TaskService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "Task";
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskService.class);
    private final ScheduledExecutorService scheduler;
    private final ConcurrentHashMap<String, TaskRunner> tasks = new ConcurrentHashMap<>();

    public TaskService(Context context) {
        super(context, NAME);

        ThreadFactory factory = Thread.ofVirtual().name("kr.task-", 0L).factory();
        this.scheduler = new ScheduledThreadPoolExecutor(1, factory);
        this.scheduler.scheduleAtFixedRate(new Cleanup(), 60, 60, TimeUnit.SECONDS);

        handlerMethod(ServerKind.INTERNAL, new TaskAdminHandler(this));
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

    /**
     * Executes the given task by wrapping it in a TaskRunner and submitting it to the scheduler.
     *
     * @param task the task to be executed; must not be null
     */
    public void execute(@Nonnull Task task) {
        TaskRunner runner = new TaskRunner(task);
        scheduler.execute(runner);
        tasks.put(task.name(), runner);
    }

    /**
     * Schedules a given {@link Task} to run at a fixed rate with a specified initial delay and interval.
     * The task is executed periodically using the provided time unit.
     *
     * @param task         the task to be executed; must not be null
     * @param initialDelay the delay before the task is first executed, in the given time unit
     * @param period       the interval between successive executions of the task, in the given time unit
     * @param unit         the time unit for the initial delay and period; must not be null
     * @return a {@link ScheduledFuture} representing the scheduled task
     */
    public ScheduledFuture<?> scheduleAtFixedRate(@Nonnull Task task, long initialDelay, long period, TimeUnit unit) {
        if (tasks.get(task.name()) != null) {
            throw new IllegalArgumentException("Task with name " + task.name() + " already exists");
        }
        TaskRunner runner = new TaskRunner(task);
        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(runner, initialDelay, period, unit);
        tasks.put(task.name(), runner);
        return future;
    }

    public List<ObservedTask> tasks() {
        List<ObservedTask> result = new ArrayList<>();
        tasks.forEach((name, runner) -> {
            ObservedTask observedTask = new ObservedTask(
                    name,
                    runner.task.stats().isRunning(),
                    runner.task.isCompleted(),
                    runner.task.stats().getStartedAt(),
                    runner.task.stats().getLastRun()
            );
            result.add(observedTask);
        });
        return result;
    }

    /**
     * Retrieves the {@link Task} associated with the specified name.
     * If no task with the given name exists, a {@link TaskNotFoundException} is thrown.
     *
     * @param name the name of the task to retrieve; must not be null
     * @return the {@link Task} associated with the specified name
     * @throws TaskNotFoundException if a task with the given name does not exist
     */
    public Task getTask(@Nonnull String name) {
        TaskRunner runner = tasks.get(name);
        if (runner == null) {
            throw new TaskNotFoundException(name);
        }
        return runner.task;
    }

    /**
     * Shuts down and removes the task identified by the specified name. This method retrieves the task
     * corresponding to the given name, shuts it down, waits for its termination, and removes it from
     * the internal task collection. If the task's termination is interrupted, a {@link RuntimeException}
     * is thrown.
     *
     * @param name the name of the task to be shut down and removed; must not be null
     * @throws TaskNotFoundException if no task with the specified name exists
     */
    public void shutdownAndRemoveTask(@Nonnull String name) {
        Task task = getTask(name);
        task.shutdown();
        tasks.remove(name);
    }

    /**
     * Checks whether a task with the specified name exists in the task collection.
     *
     * @param name the name of the task to check; must not be null
     * @return true if a task with the given name exists, false otherwise
     */
    public boolean hasTask(@Nonnull String name) {
        return tasks.containsKey(name);
    }

    @Override
    public void shutdown() {
        tasks.forEach((name, runner) -> {
            LOGGER.debug("Shutting down task {}", name);
            runner.task.stats().setRunning(false);
            runner.task.shutdown();
        });
        
        if (!ExecutorServiceUtil.shutdownNowThenAwaitTermination(scheduler)) {
            LOGGER.warn("{} service cannot be stopped gracefully", NAME);
        }
    }

    static class TaskRunner implements Runnable {
        private final Task task;

        TaskRunner(Task task) {
            this.task = task;
        }

        @Override
        public void run() {
            String name = String.format("TaskRunner-%d", System.currentTimeMillis() / 1000L);
            Thread.ofVirtual().name(name).start(task);
        }
    }

    class Cleanup implements Runnable {
        @Override
        public void run() {
            tasks.entrySet().iterator().forEachRemaining(entry -> {
                if (entry.getValue().task.isCompleted()) {
                    LOGGER.debug("Cleaning up completed task {}", entry.getKey());
                    entry.getValue().task.shutdown();
                    tasks.remove(entry.getKey());
                }
            });
        }
    }
}
