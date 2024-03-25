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
 * The TableCreatedEvent class represents an event that is triggered when a table is created in the system.
 * <p>
 * This class extends the BaseMetadataEvent class and inherits its functionality to create a metadata event.
 * It contains information about the schema, table name, and versionstamp of the created table.
 */
public class TableCreatedEvent extends BaseMetadataEvent {
    private String schema;
    private String table;
    private byte[] versionstamp;

    TableCreatedEvent() {
    }

    public TableCreatedEvent(String schema, String name, byte[] versionstamp) {
        super(EventTypes.TABLE_CREATED);
        this.schema = schema;
        this.table = name;
        this.versionstamp = versionstamp;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public byte[] getVersionstamp() {
        return versionstamp;
    }
}
