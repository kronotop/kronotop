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

import java.time.Instant;

public class TaskStats {
    private volatile long lastRun;
    private volatile boolean running;
    private final long startTime;

    public TaskStats() {
        this.startTime = Instant.now().toEpochMilli();
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setLastRun(Long lastRun) {
        this.lastRun = lastRun;
    }

    public Long getLastRun() {
        return lastRun;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public boolean isRunning() {
        return running;
    }
}
