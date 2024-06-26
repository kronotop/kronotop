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

package com.kronotop.cluster.coordinator;

import com.kronotop.cluster.BroadcastEvent;
import com.kronotop.cluster.EventTypes;

/**
 * Represents an event that updates the routing table.
 */
public class UpdateRoutingTableEvent extends BroadcastEvent {
    public UpdateRoutingTableEvent(String payload) {
        super(EventTypes.UPDATE_ROUTING_TABLE, payload);
    }
}
