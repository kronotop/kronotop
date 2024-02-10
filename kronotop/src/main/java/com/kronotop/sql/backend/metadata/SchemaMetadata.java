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

package com.kronotop.sql.backend.metadata;

import com.kronotop.sql.KronotopSchema;

/**
 * The SchemaMetadata class represents the metadata for schemas in a database.
 * It provides methods to add, retrieve, remove, and check for the existence of schemas.
 */
public class SchemaMetadata {
    private final String name;
    private final TableMetadata tables = new TableMetadata();
    private final KronotopSchema kronotopSchema;

    public SchemaMetadata(String name) {
        this.name = name;
        this.kronotopSchema = new KronotopSchema(name);
    }

    public String getName() {
        return name;
    }

    /**
     * Retrieves the metadata of all tables in the schema.
     *
     * @return the TableMetadata object containing information about the tables
     */
    public TableMetadata getTables() {
        return tables;
    }

    /**
     * Retrieves the KronotopSchema object associated with the SchemaMetadata instance.
     *
     * @return the KronotopSchema object representing a Kronotop schema.
     */
    public KronotopSchema getKronotopSchema() {
        return kronotopSchema;
    }
}
