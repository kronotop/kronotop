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
 * The TableMetadata class represents the metadata for tables in a database.
 * It provides methods to add, retrieve, remove, and check for the existence of tables.
 */
public class TableMetadata {
    private final Map<String, VersionedTableMetadata> tables = new HashMap<>();

    /**
     * Adds a table to the TableMetadata with the corresponding metadata.
     *
     * @param table    the name of the table to be added
     * @param metadata the VersionedTableMetadata object that represents the metadata of the table
     * @throws TableAlreadyExistsException if a table with the same name already exists in the TableMetadata
     */
    public void put(String table, VersionedTableMetadata metadata) throws TableAlreadyExistsException {
        if (tables.containsKey(table)) {
            throw new TableAlreadyExistsException(table);
        }
        tables.put(table, metadata);
    }

    /**
     * Retrieves the VersionedTableMetadata object for the specified table from the TableMetadata.
     *
     * @param table the name of the table to retrieve
     * @return the VersionedTableMetadata object for the specified table
     * @throws TableNotExistsException if the table does not exist in the TableMetadata
     */
    public VersionedTableMetadata get(String table) throws TableNotExistsException {
        VersionedTableMetadata versionedTableMetadata = tables.get(table);
        if (versionedTableMetadata == null) {
            throw new TableNotExistsException(table);
        }
        return versionedTableMetadata;
    }

    /**
     * Retrieves the VersionedTableMetadata object for the specified table from the TableMetadata or creates a new one if it doesn't exist in the TableMetadata.
     *
     * @param table The name of the table for which to retrieve or create the VersionedTableMetadata object.
     * @return The VersionedTableMetadata object for the specified table.
     */
    public VersionedTableMetadata getOrCreate(String table) {
        VersionedTableMetadata versionedTableMetadata = tables.get(table);
        if (versionedTableMetadata != null) {
            return versionedTableMetadata;
        }
        VersionedTableMetadata next = new VersionedTableMetadata();
        tables.put(table, next);
        return next;
    }

    /**
     * Removes a table from the TableMetadata with the specified table name.
     *
     * @param table the name of the table to be removed
     * @throws TableNotExistsException if the table does not exist in the TableMetadata
     */
    public void remove(String table) throws TableNotExistsException {
        if (!tables.containsKey(table)) {
            throw new TableNotExistsException(table);
        }
        tables.remove(table);
    }

    /**
     * Checks if a table exists in the TableMetadata.
     *
     * @param table the name of the table to check
     * @return true if the table exists in the TableMetadata, false otherwise
     */
    public boolean has(String table) {
        return tables.containsKey(table);
    }
}
