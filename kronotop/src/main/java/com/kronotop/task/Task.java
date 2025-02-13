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
 * Represents a unit of work that can be executed, shut down, and queried for its name.
 * This interface extends {@link Runnable}, allowing instances to be executed by threads
 * or task schedulers.
 */
public interface Task extends Runnable {
    String name();
    boolean isCompleted();
    void shutdown();
    void awaitTermination() throws InterruptedException;
}
