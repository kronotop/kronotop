/*
 * Copyright (c) 2023-2025 Kronotop
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

public class MemberView {
    private long latestHeartbeat;
    private long expectedHeartbeat;
    private boolean alive;

    public MemberView(long latestHeartbeat) {
        this.latestHeartbeat = latestHeartbeat;
        this.expectedHeartbeat = latestHeartbeat + 1;
        this.alive = true;
    }

    public long getExpectedHeartbeat() {
        return expectedHeartbeat;
    }

    public long getLatestHeartbeat() {
        return latestHeartbeat;
    }

    public void setLatestHeartbeat(long lastHeartbeat) {
        this.latestHeartbeat = lastHeartbeat;
        this.expectedHeartbeat = lastHeartbeat + 1;
    }

    public void increaseExpectedHeartbeat() {
        expectedHeartbeat++;
    }

    public boolean isAlive() {
        return alive;
    }

    public void setAlive(boolean alive) {
        this.alive = alive;
    }
}