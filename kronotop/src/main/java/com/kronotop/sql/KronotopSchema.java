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

package com.kronotop.sql;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Map;

public class KronotopSchema extends AbstractSchema {
    private final String schemaName;
    private final Map<String, Table> tableMap;

    private KronotopSchema(String schemaName, Map<String, Table> tableMap) {
        this.schemaName = schemaName;
        this.tableMap = tableMap;
    }

    public static Builder newBuilder(String schemaName) {
        return new Builder(schemaName);
    }

    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public Map<String, Table> getTableMap() {
        return tableMap;
    }

    public static final class Builder {

        private final String schemaName;
        private final Map<String, Table> tableMap = new HashMap<>();

        private Builder(String schemaName) {
            if (schemaName == null || schemaName.isEmpty()) {
                throw new IllegalArgumentException("Schema name cannot be null or empty");
            }

            this.schemaName = schemaName;
        }

        public Builder addTable(KronotopTable table) {
            if (tableMap.containsKey(table.getTableName())) {
                throw new IllegalArgumentException("Table already defined: " + table.getTableName());
            }

            tableMap.put(table.getTableName(), table);

            return this;
        }

        public KronotopSchema build() {
            return new KronotopSchema(schemaName, tableMap);
        }
    }
}
