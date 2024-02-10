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

import java.util.HashMap;
import java.util.Map;

/**
 * The Metadata class represents a metadata store for schemas in a database.
 * It provides methods to insert, retrieve, remove, and check for the existence of schemas.
 */
public class Metadata {
    private final Map<String, SchemaMetadata> schemas = new HashMap<>();

    /**
     * Inserts a schema and its corresponding metadata into the schema metadata store.
     *
     * @param schema   the name of the schema
     * @param metadata the metadata associated with the schema
     * @throws SchemaAlreadyExistsException if the schema already exists in the metadata store
     */
    public void put(String schema, SchemaMetadata metadata) throws SchemaAlreadyExistsException {
        if (schemas.containsKey(schema)) {
            throw new SchemaAlreadyExistsException(schema);
        }
        schemas.put(schema, metadata);
    }

    /**
     * Retrieves the metadata associated with the specified schema name.
     *
     * @param schema the name of the schema
     * @return the SchemaMetadata object associated with the schema name, or null if the schema does not exist
     */
    public SchemaMetadata get(String schema) throws SchemaNotExistsException {
        SchemaMetadata metadata = schemas.get(schema);
        if (metadata == null) {
            throw new SchemaNotExistsException(schema);
        }
        return metadata;
    }

    /**
     * Removes the specified schema from the schema metadata store.
     *
     * @param schema the name of the schema to be removed
     * @throws SchemaNotExistsException if the specified schema does not exist in the metadata store
     */
    public void remove(String schema) throws SchemaNotExistsException {
        if (!schemas.containsKey(schema)) {
            throw new SchemaNotExistsException(schema);
        }
        schemas.remove(schema);

    }

    /**
     * Checks if the specified schema exists in the schema metadata store.
     *
     * @param schema the name of the schema to check
     * @return true if the schema exists, false otherwise
     */
    public boolean has(String schema) {
        return schemas.containsKey(schema);
    }
}
