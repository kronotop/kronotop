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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents a Kronotop schema.
 */
public class KronotopSchema extends AbstractSchema {
    private final String name;
    private final ConcurrentHashMap<String, Table> tableMap;
    private final ConcurrentHashMap<String, Schema> subSchemas = new ConcurrentHashMap<>();

    public KronotopSchema(String name, ConcurrentHashMap<String, Table> tableMap) {
        this.name = name;
        this.tableMap = tableMap;
    }

    public static Builder newBuilder(String name) {
        return new Builder(name);
    }

    public String getName() {
        return name;
    }

    public void setSubSchema(String name, KronotopSchema schema) {
        subSchemas.compute(name, (n, s) -> {
            if (s != null) {
                throw new IllegalArgumentException("SubSchema has already been registered: " + name);
            }
            return schema;
        });
    }

    @Override
    protected Map<String, Schema> getSubSchemaMap() {
        return subSchemas;
    }

    @Override
    public Map<String, Table> getTableMap() {
        return tableMap;
    }

    public static final class Builder {
        private final String schemaName;
        private final ConcurrentHashMap<String, Table> tableMap = new ConcurrentHashMap<>();

        private Builder(String schemaName) {
            if (schemaName == null || schemaName.isEmpty()) {
                throw new IllegalArgumentException("Schema name cannot be null or empty");
            }
            this.schemaName = schemaName;
        }

        public Builder addTable(KronotopTable table) {
            if (tableMap.containsKey(table.getName())) {
                throw new IllegalArgumentException("Table has already already defined: " + table.getName());
            }
            tableMap.put(table.getName(), table);
            return this;
        }

        public KronotopSchema build() {
            return new KronotopSchema(schemaName, tableMap);
        }
    }
}
