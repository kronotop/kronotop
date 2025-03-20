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

/**
 * The Task interface represents a unit of work that can be executed and monitored.
 * It extends the Runnable interface, allowing it to be used in thread execution.
 * Implementations of this interface encapsulate specific business logic in the `task` method.
 */
public interface Task extends Runnable {

    // Returns TaskStats for observing the internal state of Task implementation.
    TaskStats stats();

    // Returns the task name
    String name();

    // Task method is called by run method of Runnable interface to run the business logic.
    void task();

    // Returns true if the task is completed. Completing means the task did its job successfully
    // and deleted its own metadata from FDB.
    boolean isCompleted();

    // Completes the task and removes its metadata if there is any.
    void complete();

    // Shuts down the running task but the metadata still remains in FDB
    void shutdown();

    // Await until the complete method does its job
    void awaitCompletion() throws InterruptedException;
}
