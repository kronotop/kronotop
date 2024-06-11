/*
 * Copyright (c) 2023 Kronotop
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

/**
 * Represents a broadcast event that can be published to the cluster's journal.
 */
public class BroadcastEvent {
    private EventTypes type;
    private String payload;

    private BroadcastEvent() {
    }

    public BroadcastEvent(EventTypes type, String payload) {
        this.type = type;
        this.payload = payload;
    }

    public EventTypes getType() {
        return type;
    }

    public String getPayload() {
        return payload;
    }
}
