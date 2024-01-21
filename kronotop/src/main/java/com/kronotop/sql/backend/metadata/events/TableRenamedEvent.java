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

import java.util.List;

/**
 * The TableRenamedEvent class represents an event that occurs when a table is renamed.
 */
public class TableRenamedEvent extends BaseMetadataEvent {
    private List<String> schema;
    private String oldName;
    private String newName;
    private byte[] versionstamp;

    TableRenamedEvent() {
    }

    public TableRenamedEvent(List<String> schema, String oldName, String newName, byte[] versionstamp) {
        super(EventTypes.TABLE_RENAMED);
        this.schema = schema;
        this.oldName = oldName;
        this.newName = newName;
        this.versionstamp = versionstamp;
    }

    public List<String> getSchema() {
        return schema;
    }

    public String getOldName() {
        return oldName;
    }

    public String getNewName() {
        return newName;
    }

    public byte[] getVersionstamp() {
        return versionstamp;
    }
}
