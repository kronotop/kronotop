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

package com.kronotop.sql.backend.metadata.events;

/**
 * The SchemaDroppedEvent class represents an event that occurs when a schema is dropped.
 * <p>
 * This class extends the BaseMetadataEvent class and inherits its properties and methods.
 * It provides additional functionality specific to a schema dropped event, such as
 * storing the list of dropped schemas.
 *
 * @see BaseMetadataEvent
 */
public class SchemaDroppedEvent extends BaseMetadataEvent {
    private String schema;

    SchemaDroppedEvent() {
    }

    public SchemaDroppedEvent(String schema) {
        super(EventTypes.SCHEMA_DROPPED);
        this.schema = schema;
    }

    public String getSchema() {
        return schema;
    }
}
