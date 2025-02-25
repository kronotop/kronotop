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

public class TaskStats {
    private final long startedAt;
    private volatile long lastRun;
    private volatile boolean running;

    public TaskStats() {
        this.startedAt = System.currentTimeMillis() / 1000L;
    }

    public Long getStartedAt() {
        return startedAt;
    }

    public Long getLastRun() {
        return lastRun;
    }

    public void setLastRun(Long lastRun) {
        this.lastRun = lastRun;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }
}
