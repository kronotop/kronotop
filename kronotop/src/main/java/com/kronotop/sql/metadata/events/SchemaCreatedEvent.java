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


/**
 * The SchemaCreatedEvent class represents an event that is triggered when a schema is created.
 * <p>
 * This class extends the {@link BaseMetadataEvent} class and provides additional functionality
 * and information specific to schema creation events.
 * <p>
 * The SchemaCreatedEvent class contains a list of names that represent the schemas that were created.
 * The event type is set to EventTypes.SCHEMA_CREATED.
 */
public class SchemaCreatedEvent extends BaseMetadataEvent {
    private String schema;

    SchemaCreatedEvent() {
    }

    public SchemaCreatedEvent(String schema) {
        super(EventTypes.SCHEMA_CREATED);
        this.schema = schema;
    }

    public String getSchema() {
        return schema;
    }
}
