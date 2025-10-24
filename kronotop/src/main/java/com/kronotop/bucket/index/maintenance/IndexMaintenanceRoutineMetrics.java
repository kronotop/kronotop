/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.index.maintenance;

import java.util.concurrent.atomic.AtomicLong;

public class IndexMaintenanceRoutineMetrics {
    private final AtomicLong processedEntries = new AtomicLong();
    private final long initiatedAt;
    private volatile long latestExecution;

    public IndexMaintenanceRoutineMetrics() {
        this.initiatedAt = System.currentTimeMillis();
    }

    public long getInitiatedAt() {
        return initiatedAt;
    }

    public long getLatestExecution() {
        return latestExecution;
    }

    public void setLatestExecution(long latestExecution) {
        this.latestExecution = latestExecution;
    }

    public void incrementProcessedEntries(long delta) {
        processedEntries.addAndGet(delta);
    }

    public long getProcessedEntries() {
        return processedEntries.get();
    }
}