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
 * The TableDroppedEvent class represents an event that occurs when a table is dropped from a schema.
 * It extends the BaseMetadataEvent class to inherit basic event functionality.
 * <p>
 * This event contains information about the schema and table that was dropped.
 * <p>
 * It provides methods to retrieve the schema and table name associated with the event.
 */
public class TableDroppedEvent extends BaseMetadataEvent {
    private String schema;
    private String table;

    TableDroppedEvent() {
    }

    public TableDroppedEvent(String schema, String name) {
        super(EventTypes.TABLE_DROPPED);
        this.schema = schema;
        this.table = name;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }
}
