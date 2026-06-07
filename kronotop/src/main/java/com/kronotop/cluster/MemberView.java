/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.cluster;

import java.util.concurrent.atomic.AtomicLong;

public class MemberView {
    private final AtomicLong expectedHeartbeat;
    private volatile long latestHeartbeat;
    private volatile boolean alive;

    public MemberView(long latestHeartbeat) {
        this.latestHeartbeat = latestHeartbeat;
        this.expectedHeartbeat = new AtomicLong(latestHeartbeat + 1);
        this.alive = true;
    }

    public long getExpectedHeartbeat() {
        return expectedHeartbeat.get();
    }

    public long getLatestHeartbeat() {
        return latestHeartbeat;
    }

    public void setLatestHeartbeat(long heartbeat) {
        this.latestHeartbeat = heartbeat;
        expectedHeartbeat.set(heartbeat + 1);
    }

    public void increaseExpectedHeartbeat() {
        expectedHeartbeat.incrementAndGet();
    }

    public boolean isAlive() {
        return alive;
    }

    public void setAlive(boolean alive) {
        this.alive = alive;
    }
}