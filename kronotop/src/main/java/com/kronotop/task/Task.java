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
 * A background task with lifecycle management and optional FoundationDB metadata persistence.
 *
 * <p>Tasks are long-running operations managed by {@link TaskService}. They extend {@link Runnable}
 * for thread execution, with {@link #task()} containing the actual business logic.</p>
 *
 * <p><b>Lifecycle:</b></p>
 * <ul>
 *   <li>{@link #task()} - Execute the work (may be blocking)</li>
 *   <li>{@link #shutdown()} - Signal graceful stop (metadata preserved in FDB for restart)</li>
 *   <li>{@link #complete()} - Mark finished and remove metadata from FDB</li>
 * </ul>
 *
 * <p><b>Metadata Persistence:</b> Tasks may store metadata in FoundationDB for crash recovery.
 * {@link #shutdown()} preserves this metadata (allowing restart), while {@link #complete()}
 * removes it (task finished successfully).</p>
 *
 * @see BaseTask
 * @see TaskService
 */
public interface Task extends Runnable {

    /**
     * Returns statistics for monitoring task execution state.
     */
    TaskStats stats();

    /**
     * Returns the unique task name used for registration and lookup.
     */
    String name();

    /**
     * Executes the task's business logic. Called by {@link Runnable#run()}.
     * May be a blocking operation.
     */
    void task();

    /**
     * Returns true if the task completed successfully and removed its metadata from FDB.
     */
    boolean isCompleted();

    /**
     * Marks the task as completed and removes its metadata from FoundationDB.
     * Call this when the task finishes its work successfully.
     */
    void complete();

    /**
     * Signals the task to stop gracefully. Metadata remains in FDB, allowing the task
     * to be restarted later. Use {@link #complete()} to fully terminate and clean up.
     */
    void shutdown();
}
