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

package com.kronotop.sql.metadata.events;

import java.time.Instant;

/**
 * The BaseMetadataEvent class represents a base event for metadata.
 * <p>
 * This class provides the basic functionality to create a metadata event,
 * such as setting the event type and the creation timestamp.
 * <p>
 * It is important to note that the BaseMetadataEvent class is meant to be extended
 * by specific event classes according to the type of metadata event being represented.
 */
public class BaseMetadataEvent {
    private EventTypes type;
    private long createdAt;

    BaseMetadataEvent() {
    }

    public BaseMetadataEvent(EventTypes type) {
        this.type = type;
        this.createdAt = Instant.now().toEpochMilli();
    }

    public EventTypes getType() {
        return type;
    }

    public long getCreatedAt() {
        return createdAt;
    }
}
